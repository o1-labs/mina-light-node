//! HTTP surface for the trustless light node — the read / submit / mempool API a
//! Rosetta adapter (MinaMesh) consumes.
//!
//! A background gossip task verifies every block before it can become the tip
//! (`mina-verify` trust gate) and taps the tx-pool into a best-effort mempool view.
//! The HTTP handlers serve from that state; `/account` Merkle-proves balance/nonce
//! against the verified tip's epoch-ledger root, and `/submit` broadcasts a signed tx
//! to gossip. The light node holds no keys and trusts no peer.
//!
//! Endpoints:
//!   GET  /health                      — liveness
//!   GET  /tip                         — verified best tip {height, state_hash, epoch_ledger_hash}
//!   GET  /account?pubkey=&index=      — trustless balance/nonce (index hint from the indexer;
//!                                       the pubkey is cross-checked against the proved account)
//!   GET  /mempool                     — best-effort pending tx ids (untrusted)
//!   POST /submit  {"tx_hex":"…"}      — broadcast a signed user command to gossip
//!
//! Env: MINA_NETWORK (devnet|mainnet), LIGHT_NODE_HTTP_ADDR (default 127.0.0.1:8645),
//!      MINA_VK_JSON (optional, for networks without an embedded VK).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use mina_p2p_messages::binprot::BinProtRead;
use mina_p2p_messages::v2::{
    LedgerHash, MinaBaseUserCommandStableV2, MinaLedgerSyncLedgerAnswerStableV2,
    MinaLedgerSyncLedgerQueryStableV1,
};
use mina_relay::broadcast::broadcast_tx;
use mina_relay::mempool::MempoolView;
use mina_relay::{network_seeds, rpc_net, subscribe_gossip};
use mina_verify::{
    block_from_gossip_payload, header_from_precomputed, ledger_sweep_queries, pubkey_index_pairs,
    staking_epoch_ledger_hash, sync_ledger_queries, verify_account_at_root, Verifier, LEDGER_DEPTH,
};
use serde::{Deserialize, Serialize};

/// The latest proof-verified tip and the data reads anchor to. Source-agnostic (p2p
/// gossip or the indexer's precomputed blocks) — both produce a verified header.
#[derive(Clone)]
struct TipInfo {
    height: u32,
    state_hash: String,
    /// The staking-epoch ledger root from the verified protocol state — the proven root
    /// `/account` Merkle-proves against.
    epoch_root: LedgerHash,
}

/// A public-key → leaf-index map. Mina indices are permanent + append-only, so this is
/// monotonic across ledgers: built once, then only the appended tail is re-swept.
/// `covered` is the number of leaves already mapped. The map is an untrusted hint —
/// `/account` re-proves every read against the verified root.
struct IndexCache {
    covered: u64,
    map: HashMap<String, u64>,
}

struct AppState {
    network: String,
    chain_id: &'static str,
    peers: &'static [&'static str],
    tip: RwLock<Option<TipInfo>>,
    mempool: Mutex<MempoolView>,
    index: RwLock<Option<IndexCache>>,
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
        tip: RwLock::new(None),
        mempool: Mutex::new(MempoolView::new(4096, Duration::from_secs(600))),
        index: RwLock::new(None),
    });

    // Block source for the tip. Two modes, both gated by the same trust check:
    //   - LIGHT_NODE_BLOCKS_DIR set: poll the indexer's precomputed-block dir (reuse its
    //     data — no p2p bootstrap). Robust where p2p gossip is unreliable (e.g. mesa-mut).
    //   - else: the p2p gossip block stream.
    let blocks_dir = std::env::var("LIGHT_NODE_BLOCKS_DIR").ok();
    let use_gossip_blocks = blocks_dir.is_none();
    let (block_tx, block_rx) = mpsc::channel::<Vec<u8>>();
    {
        let net = network.clone();
        let state = state.clone();
        let blocks_dir = blocks_dir.clone();
        std::thread::spawn(move || {
            let verifier = match std::env::var("MINA_VK_JSON") {
                Ok(path) => Verifier::with_index_json(
                    &std::fs::read_to_string(&path).expect("read MINA_VK_JSON"),
                )
                .expect("verifier from VK json"),
                Err(_) => Verifier::for_network(&net).expect("verifier"),
            };
            match blocks_dir {
                Some(dir) => precomputed_block_loop(&verifier, &state, &dir),
                None => {
                    while let Ok(payload) = block_rx.recv() {
                        let block = match block_from_gossip_payload(&payload) {
                            Ok(b) => b,
                            Err(_) => continue,
                        };
                        // Trust gate: only a proof-verified block can become the tip.
                        match verifier.verify_tip(block) {
                            Ok(Some(t)) => {
                                let info = TipInfo {
                                    height: t.height(),
                                    state_hash: t.state_hash().to_string(),
                                    epoch_root: staking_epoch_ledger_hash(t.block()),
                                };
                                adopt_tip(&state, info);
                            }
                            Ok(None) => log::warn!("rejected invalid block proof — not adopting"),
                            Err(e) => log::debug!("malformed block (skipped): {e:?}"),
                        }
                    }
                }
            }
        });
    }

    // Gossip task: tap tx-pool into the mempool view (and feed blocks unless a blocks dir
    // is the tip source). On networks where gossip is unreliable this is best-effort.
    {
        let state = state.clone();
        tokio::spawn(async move {
            subscribe_gossip(
                chain_id,
                peers,
                None,
                |payload| {
                    match payload.get(8) {
                        Some(0) if use_gossip_blocks => {
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
                    let root = tip.epoch_root.clone();
                    let covered = state.index.read().unwrap().as_ref().map(|c| c.covered).unwrap_or(0);
                    match sweep_tail(state.chain_id, state.peers, root, covered).await {
                        Ok((num, pairs)) if num > covered => {
                            let mut guard = state.index.write().unwrap();
                            let cache = guard
                                .get_or_insert_with(|| IndexCache { covered: 0, map: HashMap::new() });
                            for (pk, idx) in pairs {
                                cache.map.insert(pk, idx);
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
        .route("/tip", get(tip))
        .route("/account", get(account))
        .route("/mempool", get(mempool))
        .route("/submit", post(submit))
        .with_state(state);

    eprintln!("mina-light-node-server on http://{addr} ({network}) — trustless reads + submit");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

/// Adopt `info` as the tip if it's strictly newer than the current one.
fn adopt_tip(state: &Arc<AppState>, info: TipInfo) {
    let mut tip = state.tip.write().unwrap();
    if tip.as_ref().map(|t| info.height > t.height).unwrap_or(true) {
        log::info!("verified tip h{}", info.height);
        *tip = Some(info);
    }
}

/// The highest-numbered precomputed block in `dir` (files named `<net>-<height>-<hash>.json`).
fn latest_precomputed(dir: &str) -> Option<(u32, std::path::PathBuf)> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            let stem = path.file_name()?.to_str()?.strip_suffix(".json")?;
            let height: u32 = stem.splitn(3, '-').nth(1)?.parse().ok()?;
            Some((height, path))
        })
        .max_by_key(|(h, _)| *h)
}

/// Poll the indexer's precomputed-block dir; verify each new tip's proof (the trust gate)
/// and adopt it. Reuses the indexer's already-downloaded data — no p2p bootstrap.
fn precomputed_block_loop(verifier: &Verifier, state: &Arc<AppState>, dir: &str) {
    log::info!("tip source: precomputed blocks in {dir} (verify-before-adopt)");
    let mut last = 0u32;
    loop {
        if let Some((height, path)) = latest_precomputed(dir) {
            if height > last {
                last = height;
                match std::fs::read_to_string(&path).map(|j| header_from_precomputed(&j)) {
                    Ok(Ok(header)) => {
                        if verifier.verify_header(&header) {
                            let cs = &header.protocol_state.body.consensus_state;
                            let state_hash = path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .and_then(|n| n.strip_suffix(".json"))
                                .and_then(|n| n.splitn(3, '-').nth(2))
                                .unwrap_or_default()
                                .to_string();
                            adopt_tip(
                                state,
                                TipInfo {
                                    height: cs.blockchain_length.as_u32(),
                                    state_hash,
                                    epoch_root: cs.staking_epoch_data.ledger.hash.clone(),
                                },
                            );
                        } else {
                            log::warn!("precomputed block h{height} failed proof verification — skipped");
                        }
                    }
                    Ok(Err(e)) => log::warn!("decode precomputed block h{height}: {e}"),
                    Err(e) => log::warn!("read precomputed block h{height}: {e}"),
                }
            }
        }
        std::thread::sleep(Duration::from_secs(15));
    }
}

/// Sweep the leaves at indices `[covered, num)` of the epoch ledger and return
/// `(num, (pubkey, index) pairs)`. `NumAccounts` sizes it; then one `What_contents` batch
/// per 32-account subtree of the tail (the whole ledger when `covered == 0`). The map is
/// an untrusted hint — every `/account` read re-proves inclusion against the verified root.
async fn sweep_tail(
    chain_id: &'static str,
    peers: &'static [&'static str],
    root: LedgerHash,
    covered: u64,
) -> Result<(u64, Vec<(String, u64)>), String> {
    let na = rpc_net::fetch_sync_ledger_answers(
        chain_id,
        peers,
        root.clone(),
        &[MinaLedgerSyncLedgerQueryStableV1::NumAccounts],
        Duration::from_secs(60),
    )
    .await?;
    let num = match na.into_iter().next() {
        Some(MinaLedgerSyncLedgerAnswerStableV2::NumAccounts(n, _)) => n.0,
        other => return Err(format!("expected NumAccounts, got {other:?}")),
    };
    if num <= covered {
        return Ok((num, Vec::new()));
    }

    let queries = ledger_sweep_queries(covered, num, LEDGER_DEPTH);
    let answers = rpc_net::fetch_sync_ledger_answers(
        chain_id,
        peers,
        root,
        &queries,
        Duration::from_secs(300),
    )
    .await?;
    let pairs = pubkey_index_pairs(&answers, mina_verify::sweep_base_index(covered), LEDGER_DEPTH)
        .map_err(|e| e.to_string())?;
    Ok((num, pairs))
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn err(code: StatusCode, msg: impl Into<String>) -> ApiError {
    (code, Json(serde_json::json!({ "error": msg.into() })))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
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
        staking_epoch_ledger_hash: tip.epoch_root.to_string(),
    }))
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
    let root = tip.epoch_root.clone();

    // Resolve the leaf index: an explicit hint, else from the swept pubkey→index map
    // (monotonic across epochs, so no epoch-root check is needed).
    let index = match (q.index, &q.pubkey) {
        (Some(i), _) => i,
        (None, Some(pk)) => {
            let cache = state.index.read().unwrap();
            match cache.as_ref() {
                Some(c) => *c.map.get(pk).ok_or_else(|| {
                    err(StatusCode::NOT_FOUND, format!("{pk} not in the epoch ledger"))
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
    .map_err(|e| err(StatusCode::BAD_GATEWAY, format!("sync-ledger fetch failed: {e}")))?;

    // TRUST GATE: fold account + path onto the proven epoch-ledger root.
    let acct = verify_account_at_root(&root, index, LEDGER_DEPTH, &answers)
        .map_err(|e| err(StatusCode::BAD_GATEWAY, format!("account did not verify: {e}")))?;

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
    Json(MempoolResponse { count: ids.len(), transaction_ids: ids })
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
