//! HTTP surface for the trustless light node — the read / submit / mempool API a
//! Rosetta adapter (MinaMesh) or a client-side integrity monitor consumes.
//!
//! A background gossip task verifies every block before it can become the tip
//! (`mina-verify` trust gate) and taps the tx-pool into a best-effort mempool view.
//! The HTTP handlers serve from that state; `/account` Merkle-proves balance/nonce
//! against the verified tip's epoch-ledger root, and `/submit` broadcasts a signed tx
//! to gossip. The light node holds no keys and trusts no peer.
//!
//! Endpoints:
//!   GET  /health, /healthz            — liveness + sync freshness + verify counters
//!   GET  /tip                         — verified best tip {height, state_hash, epoch_ledger_hash}
//!   GET  /account?pubkey=&index=      — trustless balance/nonce (by public key via the
//!                                       swept index map, or an explicit index hint)
//!   GET  /mempool                     — best-effort pending tx ids (untrusted)
//!   POST /submit  {"tx_hex":"…"}      — broadcast a signed user command to gossip
//!
//! Env: MINA_NETWORK (devnet|mainnet), LIGHT_NODE_HTTP_ADDR (default 127.0.0.1:8645),
//!      MINA_VK_JSON (optional, for networks without an embedded VK).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use mina_light_node::{index_map, sweep_index_map};
use mina_p2p_messages::binprot::BinProtRead;
use mina_p2p_messages::v2::MinaBaseUserCommandStableV2;
use mina_relay::broadcast::broadcast_tx;
use mina_relay::mempool::MempoolView;
use mina_relay::{network_seeds, rpc_net, subscribe_gossip};
use mina_verify::{
    block_from_gossip_payload, staking_epoch_ledger_hash, sync_ledger_queries,
    verify_account_at_root, Block, ChainMonitor, Ingest, Verifier, LEDGER_DEPTH,
};
use serde::{Deserialize, Serialize};

/// The latest proof-verified tip and the data derived from it.
#[derive(Clone)]
struct TipInfo {
    block: Block,
    height: u32,
    state_hash: String,
}

/// A `addr-hash → leaf-index` map (keyed by [`index_map::addr_key`]). Mina indices are
/// permanent + append-only, so this is monotonic across ledgers: loaded from a baked
/// `.bin` and/or built by sweeping, then only the appended tail is re-swept. `covered`
/// is the number of leaves already mapped. An untrusted hint — `/account` re-proves
/// every read against the verified root.
struct IndexCache {
    covered: u64,
    map: HashMap<[u8; 16], u64>,
}

struct AppState {
    network: String,
    chain_id: &'static str,
    peers: &'static [&'static str],
    started: Instant,
    tip: RwLock<Option<TipInfo>>,
    mempool: Mutex<MempoolView>,
    index: RwLock<Option<IndexCache>>,
    verified: AtomicU64,
    rejected: AtomicU64,
    /// Reorgs (the new tip won fork-choice over a competing branch) and non-winning
    /// competing forks seen — finality/safety signals for the integrity monitor.
    reorgs: AtomicU64,
    forks: AtomicU64,
    /// Unix seconds of the last successful verification (0 = none yet) — sync freshness.
    last_verified_unix: AtomicU64,
}

/// Build the network verifier — from `MINA_VK_JSON` (a caller-supplied verifier-index,
/// for networks without an embedded VK) if set, else the embedded VK for `network`.
fn build_verifier(network: &str) -> Result<Verifier, String> {
    match std::env::var("MINA_VK_JSON") {
        Ok(path) => {
            let json = std::fs::read_to_string(&path)
                .map_err(|e| format!("read MINA_VK_JSON {path}: {e}"))?;
            Verifier::with_index_json(&json).map_err(|e| e.to_string())
        }
        Err(_) => Verifier::for_network(network).map_err(|e| e.to_string()),
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) =
        network_seeds(&network).unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?}"));
    let addr: SocketAddr = std::env::var("LIGHT_NODE_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8645".into())
        .parse()
        .expect("LIGHT_NODE_HTTP_ADDR");

    let state = Arc::new(AppState {
        network: network.clone(),
        chain_id,
        peers,
        started: Instant::now(),
        tip: RwLock::new(None),
        mempool: Mutex::new(MempoolView::new(4096, Duration::from_secs(600))),
        index: RwLock::new(None),
        verified: AtomicU64::new(0),
        rejected: AtomicU64::new(0),
        reorgs: AtomicU64::new(0),
        forks: AtomicU64::new(0),
        last_verified_unix: AtomicU64::new(0),
    });

    // Baked map: if LIGHT_NODE_INDEX_MAP points at a .bin (built by `mapgen`), load it so
    // `/account?pubkey=` works immediately — no cold-start sweep. The background sweep
    // then only fills the appended tail.
    if let Ok(path) = std::env::var("LIGHT_NODE_INDEX_MAP") {
        match std::fs::read(&path) {
            // An empty/placeholder file = no baked map (the image ships one so the COPY
            // always succeeds); fall through to sweeping.
            Ok(blob) if blob.len() < 8 => {
                log::info!("LIGHT_NODE_INDEX_MAP {path} is empty; will sweep instead")
            }
            Ok(blob) => {
                let covered = index_map::covered(&blob);
                let map: HashMap<[u8; 16], u64> = index_map::load(&blob).into_iter().collect();
                eprintln!(
                    "loaded baked index map {path}: {} keys, covered {covered}",
                    map.len()
                );
                *state.index.write().unwrap() = Some(IndexCache { covered, map });
            }
            Err(e) => log::warn!("LIGHT_NODE_INDEX_MAP {path}: {e}; will sweep instead"),
        }
    }

    // Verify-before-tip worker thread (multi-second crypto, off the async runtime).
    let (block_tx, block_rx) = mpsc::channel::<Vec<u8>>();
    {
        let net = network.clone();
        let state = state.clone();
        std::thread::spawn(move || {
            // Fail loud if we can't build a verifier — a light *node* that can't verify
            // is just an untrusted relay. (Without this the thread would die and the
            // process would stay "healthy" while verifying nothing — audit finding S2.)
            let verifier = match build_verifier(&net) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("fatal: cannot build verifier for {net:?}: {e}");
                    std::process::exit(1);
                }
            };
            // Fork-choice-aware tip tracking: classify each verified tip (extend / reorg /
            // fork / behind) instead of a naive height compare, so reorgs are detected.
            let mut monitor = ChainMonitor::new(512);
            while let Ok(payload) = block_rx.recv() {
                let block = match block_from_gossip_payload(&payload) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                // Trust gate: only a proof-verified block can become the tip.
                match verifier.verify_tip(block) {
                    Ok(Some(t)) => {
                        state.verified.fetch_add(1, Ordering::Relaxed);
                        state
                            .last_verified_unix
                            .store(now_unix(), Ordering::Relaxed);
                        let height = t.height();
                        let outcome = monitor.ingest(&t);
                        match &outcome {
                            Ingest::Reorg {
                                depth,
                                common_ancestor,
                                ..
                            } => {
                                state.reorgs.fetch_add(1, Ordering::Relaxed);
                                log::warn!(
                                    "REORG to h{height} (rolled back {}, diverged at {})",
                                    depth.map_or("?".into(), |d| d.to_string()),
                                    common_ancestor.as_deref().unwrap_or("<unknown>"),
                                );
                            }
                            Ingest::Fork { common_ancestor } => {
                                state.forks.fetch_add(1, Ordering::Relaxed);
                                log::warn!(
                                    "competing FORK at h{height} (diverged at {})",
                                    common_ancestor.as_deref().unwrap_or("<unknown>"),
                                );
                            }
                            _ => {}
                        }
                        // Adopt the new best only when fork-choice says so.
                        if matches!(
                            outcome,
                            Ingest::Genesis | Ingest::Extend { .. } | Ingest::Reorg { .. }
                        ) {
                            log::info!("verified tip h{height}");
                            *state.tip.write().unwrap() = Some(TipInfo {
                                state_hash: t.state_hash().to_string(),
                                block: t.block().clone(),
                                height,
                            });
                        }
                    }
                    Ok(None) => {
                        state.rejected.fetch_add(1, Ordering::Relaxed);
                        log::warn!("rejected invalid block proof — not adopting as tip");
                    }
                    Err(e) => log::debug!("malformed block (skipped): {e:?}"),
                }
            }
        });
    }

    // Gossip task: feed blocks to the verifier, tap tx-pool into the mempool view.
    {
        let state = state.clone();
        tokio::spawn(async move {
            subscribe_gossip(
                chain_id,
                peers,
                None,
                |payload| {
                    match payload.get(8) {
                        Some(0) => {
                            let _ = block_tx.send(payload.to_vec());
                        }
                        Some(2) => {
                            state.mempool.lock().unwrap().ingest_gossip(payload);
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                },
                |_| ControlFlow::Continue(()),
            )
            .await;
        });
    }

    // Index sweep task: build the pubkey→leaf-index map at cold start, then only sweep
    // the newly-appended tail (indices are append-only), so `/account?pubkey=` resolves
    // the index itself — no indexer needed.
    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                let tip = state.tip.read().unwrap().clone();
                if let Some(tip) = tip {
                    let root = staking_epoch_ledger_hash(&tip.block);
                    let covered = state
                        .index
                        .read()
                        .unwrap()
                        .as_ref()
                        .map(|c| c.covered)
                        .unwrap_or(0);
                    match sweep_index_map(state.chain_id, state.peers, root, covered).await {
                        Ok((num, pairs)) if num > covered => {
                            let mut guard = state.index.write().unwrap();
                            let cache = guard.get_or_insert_with(|| IndexCache {
                                covered: 0,
                                map: HashMap::new(),
                            });
                            for (pk, idx) in pairs {
                                cache.map.insert(index_map::addr_key(&pk), idx);
                            }
                            cache.covered = num;
                            log::info!(
                                "pubkey→index map: +{} account(s) (now {num} covered)",
                                num - covered
                            );
                        }
                        Ok(_) => {} // already up to date
                        Err(e) => log::warn!("epoch-ledger sweep failed: {e}"),
                    }
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/healthz", get(health))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/tip", get(tip))
        .route("/account", get(account))
        .route("/mempool", get(mempool))
        .route("/submit", post(submit))
        .with_state(state);

    eprintln!("mina-light-node-server on http://{addr} ({network}) — trustless reads + submit");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> ApiError {
    (code, Json(serde_json::json!({ "error": msg.into() })))
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let last = state.last_verified_unix.load(Ordering::Relaxed);
    let since = if last == 0 {
        serde_json::Value::Null
    } else {
        now_unix().saturating_sub(last).into()
    };
    Json(serde_json::json!({
        "status": "ok",
        "network": state.network,
        "uptime_secs": state.started.elapsed().as_secs(),
        "verified": state.verified.load(Ordering::Relaxed),
        "rejected": state.rejected.load(Ordering::Relaxed),
        "seconds_since_last_verified": since,
    }))
}

#[derive(Serialize)]
struct TipResponse {
    network: String,
    height: u32,
    state_hash: String,
    staking_epoch_ledger_hash: String,
}

async fn tip(State(state): State<Arc<AppState>>) -> Result<Json<TipResponse>, ApiError> {
    let tip = state.tip.read().unwrap().clone();
    let tip = tip.ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "no verified tip yet"))?;
    Ok(Json(TipResponse {
        network: state.network.clone(),
        height: tip.height,
        state_hash: tip.state_hash,
        staking_epoch_ledger_hash: staking_epoch_ledger_hash(&tip.block).to_string(),
    }))
}

#[derive(Serialize)]
struct StatusResponse {
    network: String,
    // chain (proof-verified)
    height: u32,
    state_hash: String,
    epoch: u32,
    global_slot: u32,
    /// Ouroboros chain-quality / censorship-resistance signal (lower = unhealthier).
    min_window_density: u32,
    staking_epoch_ledger_hash: String,
    // node / monitor
    verified: u64,
    rejected: u64,
    reorgs: u64,
    forks: u64,
    uptime_secs: u64,
    /// Sync freshness; `null` until the first verified tip.
    seconds_since_last_verified: Option<u64>,
}

/// Rich health + chain-quality view — the integrity monitor's read model.
async fn status(State(state): State<Arc<AppState>>) -> Result<Json<StatusResponse>, ApiError> {
    let tip = state
        .tip
        .read()
        .unwrap()
        .clone()
        .ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "no verified tip yet"))?;
    let cs = &tip.block.header.protocol_state.body.consensus_state;
    let last = state.last_verified_unix.load(Ordering::Relaxed);
    Ok(Json(StatusResponse {
        network: state.network.clone(),
        height: tip.height,
        state_hash: tip.state_hash.clone(),
        epoch: cs.epoch_count.as_u32(),
        global_slot: cs.global_slot_since_genesis.as_u32(),
        min_window_density: cs.min_window_density.as_u32(),
        staking_epoch_ledger_hash: staking_epoch_ledger_hash(&tip.block).to_string(),
        verified: state.verified.load(Ordering::Relaxed),
        rejected: state.rejected.load(Ordering::Relaxed),
        reorgs: state.reorgs.load(Ordering::Relaxed),
        forks: state.forks.load(Ordering::Relaxed),
        uptime_secs: state.started.elapsed().as_secs(),
        seconds_since_last_verified: (last != 0).then(|| now_unix().saturating_sub(last)),
    }))
}

/// Prometheus exposition — the integrity monitor scrapes this (alert on tip staleness,
/// reorg depth, low density, rejected blocks, etc.).
async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::fmt::Write;
    fn m(out: &mut String, name: &str, typ: &str, help: &str, val: u64) {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} {typ}\n{name} {val}"
        );
    }

    let mut s = String::new();
    m(
        &mut s,
        "mina_light_node_up",
        "gauge",
        "1 if the node is serving",
        1,
    );
    m(
        &mut s,
        "mina_light_node_uptime_seconds",
        "gauge",
        "process uptime",
        state.started.elapsed().as_secs(),
    );
    m(
        &mut s,
        "mina_light_node_verified_total",
        "counter",
        "blocks whose proof verified",
        state.verified.load(Ordering::Relaxed),
    );
    m(
        &mut s,
        "mina_light_node_rejected_total",
        "counter",
        "blocks rejected (invalid proof)",
        state.rejected.load(Ordering::Relaxed),
    );
    m(
        &mut s,
        "mina_light_node_reorgs_total",
        "counter",
        "reorgs adopted",
        state.reorgs.load(Ordering::Relaxed),
    );
    m(
        &mut s,
        "mina_light_node_forks_total",
        "counter",
        "competing forks seen",
        state.forks.load(Ordering::Relaxed),
    );

    let last = state.last_verified_unix.load(Ordering::Relaxed);
    if last != 0 {
        m(
            &mut s,
            "mina_light_node_seconds_since_last_verified",
            "gauge",
            "sync freshness",
            now_unix().saturating_sub(last),
        );
    }
    if let Some(tip) = state.tip.read().unwrap().as_ref() {
        let cs = &tip.block.header.protocol_state.body.consensus_state;
        m(
            &mut s,
            "mina_light_node_tip_height",
            "gauge",
            "verified best tip height",
            tip.height as u64,
        );
        m(
            &mut s,
            "mina_light_node_epoch",
            "gauge",
            "current epoch",
            cs.epoch_count.as_u32() as u64,
        );
        m(
            &mut s,
            "mina_light_node_global_slot",
            "gauge",
            "global slot since genesis",
            cs.global_slot_since_genesis.as_u32() as u64,
        );
        m(
            &mut s,
            "mina_light_node_min_window_density",
            "gauge",
            "Ouroboros min-window density",
            cs.min_window_density.as_u32() as u64,
        );
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        s,
    )
}

#[derive(Deserialize)]
struct AccountQuery {
    /// Leaf index in the epoch ledger — an untrusted hint. A wrong hint cannot forge a
    /// balance: the path won't fold, or the pubkey won't match. Optional when `pubkey`
    /// is given (resolved from the swept pubkey→index map).
    index: Option<u64>,
    /// Public key to read. Resolves the index from the swept map (if `index` absent) and
    /// is cross-checked against the proved account either way.
    pubkey: Option<String>,
}

#[derive(Serialize)]
struct AccountResponse {
    public_key: String,
    balance: u64,
    nonce: u32,
    /// The verified tip the balance is Merkle-proved against.
    anchored_height: u32,
    anchored_state_hash: String,
    /// Reads anchor to the (finalized) staking epoch ledger, not the staged tip.
    ledger: &'static str,
}

async fn account(
    State(state): State<Arc<AppState>>,
    Query(q): Query<AccountQuery>,
) -> Result<Json<AccountResponse>, ApiError> {
    let tip = state
        .tip
        .read()
        .unwrap()
        .clone()
        .ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "no verified tip yet"))?;
    let root = staking_epoch_ledger_hash(&tip.block);

    // Resolve the leaf index: an explicit hint, else from the swept pubkey→index map
    // (monotonic across epochs, so no epoch-root check is needed).
    let index = match (q.index, &q.pubkey) {
        (Some(i), _) => i,
        (None, Some(pk)) => {
            let cache = state.index.read().unwrap();
            match cache.as_ref() {
                Some(c) => *c.map.get(&index_map::addr_key(pk)).ok_or_else(|| {
                    err(
                        StatusCode::NOT_FOUND,
                        format!("{pk} not in the epoch ledger"),
                    )
                })?,
                None => {
                    return Err(err(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "pubkey→index map not ready (sweeping epoch ledger)",
                    ))
                }
            }
        }
        (None, None) => return Err(err(StatusCode::BAD_REQUEST, "provide ?pubkey= or ?index=")),
    };

    // UNTRUSTED fetch: walk the sync-ledger for the account + Merkle path.
    let queries = sync_ledger_queries(index, LEDGER_DEPTH);
    let answers = rpc_net::fetch_sync_ledger_answers(
        state.chain_id,
        state.peers,
        root.clone(),
        &queries,
        Duration::from_secs(60),
    )
    .await
    .map_err(|e| {
        err(
            StatusCode::BAD_GATEWAY,
            format!("sync-ledger fetch failed: {e}"),
        )
    })?;

    // TRUST GATE: fold account + path onto the proven epoch-ledger root.
    let acct = verify_account_at_root(&root, index, LEDGER_DEPTH, &answers).map_err(|e| {
        err(
            StatusCode::BAD_GATEWAY,
            format!("account did not verify: {e}"),
        )
    })?;

    let public_key = acct.public_key.into_address();
    if let Some(want) = &q.pubkey {
        if &public_key != want {
            return Err(err(
                StatusCode::NOT_FOUND,
                format!("index {index} holds {public_key}, not requested {want}"),
            ));
        }
    }

    Ok(Json(AccountResponse {
        public_key,
        balance: acct.balance.as_u64(),
        nonce: acct.nonce.as_u32(),
        anchored_height: tip.height,
        anchored_state_hash: tip.state_hash,
        ledger: "staking_epoch",
    }))
}

#[derive(Serialize)]
struct MempoolResponse {
    count: usize,
    transaction_ids: Vec<String>,
}

async fn mempool(State(state): State<Arc<AppState>>) -> Json<MempoolResponse> {
    let mut view = state.mempool.lock().unwrap();
    view.expire();
    let ids = view.ids();
    Json(MempoolResponse {
        count: ids.len(),
        transaction_ids: ids,
    })
}

#[derive(Deserialize)]
struct SubmitRequest {
    /// A signed `MinaBaseUserCommandStableV2`, hex-encoded binprot.
    tx_hex: String,
}

#[derive(Serialize)]
struct SubmitResponse {
    tx_id: String,
    published: bool,
    echoes: usize,
}

async fn submit(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SubmitRequest>,
) -> Result<Json<SubmitResponse>, ApiError> {
    let bytes = hex::decode(req.tx_hex.trim())
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("tx_hex not hex: {e}")))?;
    let mut cursor = &bytes[..];
    let cmd = MinaBaseUserCommandStableV2::binprot_read(&mut cursor)
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("not a user command: {e}")))?;

    let outcome = broadcast_tx(
        state.chain_id,
        state.peers,
        vec![cmd],
        Duration::from_secs(20),
        Duration::from_secs(90),
    )
    .await
    .map_err(|e| err(StatusCode::BAD_GATEWAY, format!("broadcast failed: {e}")))?;

    Ok(Json(SubmitResponse {
        tx_id: outcome.tx_ids.into_iter().next().unwrap_or_default(),
        published: true,
        echoes: outcome.echoes,
    }))
}
