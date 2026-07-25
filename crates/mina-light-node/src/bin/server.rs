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
//!   GET  /health, /healthz            — liveness (always 200 while up) + sync freshness
//!   GET  /ready                       — readiness: 503 until a fresh verified tip exists
//!   GET  /tip                         — verified best tip {height, state_hash, epoch_ledger_hash, fresh}
//!   GET  /account?pubkey=&index=      — trustless balance/nonce (by public key via the
//!                                       swept index map, or an explicit index hint)
//!   GET  /mempool                     — best-effort pending tx ids (untrusted)
//!   POST /submit  {"tx_hex":"…"}      — broadcast a signed user command to gossip
//!
//! Env: MINA_NETWORK (devnet|mainnet), LIGHT_NODE_HTTP_ADDR (default 127.0.0.1:8645),
//!      MINA_VK_JSON (optional, for networks without an embedded VK).

// jemalloc returns freed memory to the OS far better than glibc malloc, whose per-thread
// arenas retain the verifier's large transient allocations and ratchet RSS to a high
// plateau (see workspace Cargo.toml).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::{HashMap, HashSet};
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
use mina_p2p_messages::v2::{LedgerHash, MinaBaseUserCommandStableV2};
use mina_relay::broadcast::broadcast_tx;
use mina_relay::mempool::MempoolView;
use mina_relay::{network_seeds, rpc_net, subscribe_gossip, PeerId};
use mina_verify::{
    block_from_gossip_payload, header_from_precomputed, sync_ledger_queries,
    verify_account_at_root, BlockHeader, ChainMonitor, Ingest, Verifier, LEDGER_DEPTH,
};
use serde::{Deserialize, Serialize};

/// The latest proof-verified tip and the data derived from it.
#[derive(Clone)]
struct TipInfo {
    /// The verified block **header** — enough for every read we serve (`/status`
    /// consensus fields, `/account` epoch-ledger root). Header-only so the gossip and
    /// precomputed-block tip sources produce the same shape (`header_from_precomputed`
    /// yields a header, not a full block).
    header: BlockHeader,
    height: u32,
    state_hash: String,
}

/// The **staking epoch** ledger root from a verified header's consensus state — the
/// header-based twin of [`mina_verify::staking_epoch_ledger_hash`] (which needs a full
/// block). Proven field; the ledger live peers actually serve over the sync-ledger RPC.
fn staking_epoch_ledger_hash_h(header: &BlockHeader) -> LedgerHash {
    header
        .protocol_state
        .body
        .consensus_state
        .staking_epoch_data
        .ledger
        .hash
        .clone()
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
    /// Connected peer count (live) and peers banned for relaying invalid-proof blocks.
    peer_count: AtomicU64,
    banned: AtomicU64,
    /// Unix seconds of the last successful verification (0 = none yet) — sync freshness.
    last_verified_unix: AtomicU64,
    /// A tip older than this many seconds is considered stale — `/ready` fails and
    /// `/tip` flags it. Set from `LIGHT_NODE_STALE_SECS` (default 900).
    stale_secs: u64,
}

/// Freshness of the verified tip: `(seconds_since_verified, is_fresh)`. `seconds` is
/// `None` before the first verification. `is_fresh` is false with no tip yet or once the
/// last verification is older than `stale_secs` — the signal `/ready` gates traffic on.
fn freshness(state: &AppState) -> (Option<u64>, bool) {
    let last = state.last_verified_unix.load(Ordering::Relaxed);
    if last == 0 {
        return (None, false);
    }
    let since = now_unix().saturating_sub(last);
    (Some(since), since <= state.stale_secs)
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

/// Adopt `info` as the tip if it's strictly newer than the current one. Used by the
/// precomputed-block source (the gossip worker adopts via fork-choice instead).
fn adopt_tip(state: &Arc<AppState>, info: TipInfo) {
    let mut tip = state.tip.write().unwrap();
    if tip.as_ref().map(|t| info.height > t.height).unwrap_or(true) {
        log::info!("verified tip h{}", info.height);
        *tip = Some(info);
    }
}

/// The highest-numbered precomputed block in `dir`, as `(height, state_hash, path)`.
/// Files are named `<net>-<height>-<state_hash>.json` (the indexer's layout). The net
/// prefix may itself contain '-' (e.g. `mesa-mut`), so parse from the right: the state
/// hash is the last '-' segment and the height the one before it.
fn latest_precomputed(dir: &str, skip: &HashSet<u32>) -> Option<(u32, String, std::path::PathBuf)> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            let stem = path.file_name()?.to_str()?.strip_suffix(".json")?;
            let (rest, state_hash) = stem.rsplit_once('-')?;
            let (_net, height) = rest.rsplit_once('-')?;
            let height: u32 = height.parse().ok()?;
            (!skip.contains(&height)).then_some((height, state_hash.to_string(), path))
        })
        .max_by_key(|(h, _, _)| *h)
}

/// Poll the indexer's precomputed-block dir; verify each new tip's header proof (the
/// trust gate) and adopt it. Reuses the indexer's already-downloaded blocks — no p2p
/// block bootstrap. The staged-ledger diff is ignored; only the proof-bearing header is
/// read (`header_from_precomputed`), so a header is all we adopt (see [`TipInfo`]).
fn precomputed_block_loop(verifier: &Verifier, state: &Arc<AppState>, dir: &str) {
    log::info!("tip source: precomputed blocks in {dir} (verify-before-adopt)");
    let mut last = 0u32;
    loop {
        // Adopt the highest *usable* block each cycle, skipping any that fail to read,
        // decode, or verify — so one bad top file (partial write, corrupt encoding) can't
        // wedge tip adoption; we fall back to the highest good block below it. `skip`
        // resets per cycle so a transiently-bad file (mid-write) is retried next time.
        let mut skip: HashSet<u32> = HashSet::new();
        while let Some((height, state_hash, path)) = latest_precomputed(dir, &skip) {
            if height <= last {
                break;
            }
            match std::fs::read_to_string(&path).map(|j| header_from_precomputed(&j)) {
                Ok(Ok(header)) if verifier.verify_header(&header) => {
                    state.verified.fetch_add(1, Ordering::Relaxed);
                    state
                        .last_verified_unix
                        .store(now_unix(), Ordering::Relaxed);
                    let cs = &header.protocol_state.body.consensus_state;
                    adopt_tip(
                        state,
                        TipInfo {
                            height: cs.blockchain_length.as_u32(),
                            state_hash,
                            header,
                        },
                    );
                    last = height;
                }
                Ok(Ok(_)) => {
                    state.rejected.fetch_add(1, Ordering::Relaxed);
                    log::warn!("precomputed block h{height} failed proof verification — skipped");
                    skip.insert(height);
                }
                Ok(Err(e)) => {
                    log::warn!("decode precomputed block h{height}: {e}");
                    skip.insert(height);
                }
                Err(e) => {
                    log::warn!("read precomputed block h{height}: {e}");
                    skip.insert(height);
                }
            }
        }
        std::thread::sleep(Duration::from_secs(15));
    }
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
        peer_count: AtomicU64::new(0),
        banned: AtomicU64::new(0),
        last_verified_unix: AtomicU64::new(0),
        stale_secs: std::env::var("LIGHT_NODE_STALE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(900),
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

    // Tip source. Two modes, both gated by the same proof trust check:
    //   - LIGHT_NODE_BLOCKS_DIR set: poll the indexer's precomputed-block dir (reuse its
    //     already-downloaded data — no p2p block bootstrap). Robust where gossip is
    //     unreliable (e.g. mesa-mut). The gossip task then taps the mempool only.
    //   - else: the p2p gossip block stream (verified by the worker thread below).
    let blocks_dir = std::env::var("LIGHT_NODE_BLOCKS_DIR").ok();
    let use_gossip_blocks = blocks_dir.is_none();
    if let Some(dir) = blocks_dir {
        let net = network.clone();
        let state = state.clone();
        std::thread::spawn(move || {
            let verifier = match build_verifier(&net) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("fatal: cannot build verifier for {net:?}: {e}");
                    std::process::exit(1);
                }
            };
            precomputed_block_loop(&verifier, &state, &dir);
        });
    }

    // Verify-before-tip worker thread (multi-second crypto, off the async runtime). Each
    // block carries the peer that relayed it, so the worker can ban repeat offenders.
    //
    // BOUNDED queue: block verification (seconds) is slower than gossip ingest, and block
    // payloads are large. An unbounded channel would let a gossip burst grow without limit
    // until OOM. We cap the backlog and drop newest-on-full — gossip re-delivers blocks, so
    // a dropped one re-arrives once the worker drains, and the canonical tip is still
    // verified within a slot. `BLOCK_QUEUE` covers a healthy fork set per slot with margin.
    const BLOCK_QUEUE: usize = 256;
    let (block_tx, block_rx) = mpsc::sync_channel::<(PeerId, Vec<u8>)>(BLOCK_QUEUE);
    // Reactive verification: the worker asks the gossip loop to disconnect+blocklist a
    // peer once it relays too many invalid-proof blocks.
    let (ban_tx, ban_rx) = tokio::sync::mpsc::unbounded_channel::<PeerId>();
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
            // Invalid-proof strikes per relaying peer; ban after BAN_THRESHOLD (a few, not
            // one — an honest peer may relay a block it hadn't fully validated).
            const BAN_THRESHOLD: u32 = 3;
            let mut strikes: HashMap<PeerId, u32> = HashMap::new();
            while let Ok((src, payload)) = block_rx.recv() {
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
                                header: t.block().header.clone(),
                                height,
                            });
                        }
                    }
                    Ok(None) => {
                        state.rejected.fetch_add(1, Ordering::Relaxed);
                        let n = strikes.entry(src).and_modify(|n| *n += 1).or_insert(1);
                        log::warn!("rejected invalid block proof from {src} (strike {n})");
                        if *n == BAN_THRESHOLD {
                            state.banned.fetch_add(1, Ordering::Relaxed);
                            let _ = ban_tx.send(src); // evict: disconnect + blocklist
                        }
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
            let tick_state = state.clone();
            subscribe_gossip(
                chain_id,
                peers,
                None,
                |src, payload| {
                    match payload.get(8) {
                        Some(0) if use_gossip_blocks => {
                            // Non-blocking: never stall the gossip/async runtime on a full
                            // queue. Drop-newest on backlog — gossip re-delivers the block.
                            if let Err(mpsc::TrySendError::Full(_)) =
                                block_tx.try_send((src, payload.to_vec()))
                            {
                                log::warn!("verify queue full ({BLOCK_QUEUE}); dropped a block (gossip will re-deliver)");
                            }
                        }
                        Some(2) => {
                            state.mempool.lock().unwrap().ingest_gossip(payload);
                        }
                        _ => {}
                    }
                    ControlFlow::Continue(())
                },
                move |n| {
                    tick_state.peer_count.store(n as u64, Ordering::Relaxed);
                    ControlFlow::Continue(())
                },
                ban_rx,
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
                    let root = staking_epoch_ledger_hash_h(&tip.header);
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
        .route("/ready", get(ready))
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
    /// Seconds since this tip was proof-verified — so a single `/tip` call is
    /// self-describing about freshness (a frozen tip is otherwise indistinguishable
    /// from a current one). `None` only in the brief window before the first verify.
    seconds_since_verified: Option<u64>,
    /// False once the tip is older than the staleness threshold — the block source may
    /// be stalled; treat the data as possibly out of date.
    fresh: bool,
}

async fn tip(State(state): State<Arc<AppState>>) -> Result<Json<TipResponse>, ApiError> {
    let tip = state.tip.read().unwrap().clone();
    let tip = tip.ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "no verified tip yet"))?;
    let (seconds_since_verified, fresh) = freshness(&state);
    Ok(Json(TipResponse {
        network: state.network.clone(),
        height: tip.height,
        state_hash: tip.state_hash,
        staking_epoch_ledger_hash: staking_epoch_ledger_hash_h(&tip.header).to_string(),
        seconds_since_verified,
        fresh,
    }))
}

/// Readiness probe — distinct from `/health` (liveness). `/health` always answers 200
/// while the process is up; `/ready` returns 503 until there's a *fresh* verified tip,
/// so an orchestrator/LB routes traffic only to a node actually serving current state.
/// Gating on this endpoint (not `/health`) avoids the classic trap where a liveness
/// probe kills a container that is merely still syncing.
async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (since, fresh) = freshness(&state);
    let has_tip = state.tip.read().unwrap().is_some();
    let (code, status) = match (has_tip, fresh) {
        (true, true) => (StatusCode::OK, "ready"),
        (false, _) => (StatusCode::SERVICE_UNAVAILABLE, "no verified tip yet"),
        (true, false) => (StatusCode::SERVICE_UNAVAILABLE, "stale"),
    };
    (
        code,
        Json(serde_json::json!({
            "status": status,
            "seconds_since_verified": since,
            "stale_after_secs": state.stale_secs,
        })),
    )
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
    peers: u64,
    verified: u64,
    rejected: u64,
    reorgs: u64,
    forks: u64,
    banned: u64,
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
    let cs = &tip.header.protocol_state.body.consensus_state;
    let last = state.last_verified_unix.load(Ordering::Relaxed);
    Ok(Json(StatusResponse {
        network: state.network.clone(),
        height: tip.height,
        state_hash: tip.state_hash.clone(),
        epoch: cs.epoch_count.as_u32(),
        global_slot: cs.global_slot_since_genesis.as_u32(),
        min_window_density: cs.min_window_density.as_u32(),
        staking_epoch_ledger_hash: staking_epoch_ledger_hash_h(&tip.header).to_string(),
        peers: state.peer_count.load(Ordering::Relaxed),
        verified: state.verified.load(Ordering::Relaxed),
        rejected: state.rejected.load(Ordering::Relaxed),
        reorgs: state.reorgs.load(Ordering::Relaxed),
        forks: state.forks.load(Ordering::Relaxed),
        banned: state.banned.load(Ordering::Relaxed),
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
    m(
        &mut s,
        "mina_light_node_peers",
        "gauge",
        "connected peers",
        state.peer_count.load(Ordering::Relaxed),
    );
    m(
        &mut s,
        "mina_light_node_banned_total",
        "counter",
        "peers banned for relaying invalid-proof blocks",
        state.banned.load(Ordering::Relaxed),
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
        let cs = &tip.header.protocol_state.body.consensus_state;
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
    let root = staking_epoch_ledger_hash_h(&tip.header);

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
