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

use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use mina_p2p_messages::binprot::BinProtRead;
use mina_p2p_messages::v2::MinaBaseUserCommandStableV2;
use mina_relay::broadcast::broadcast_tx;
use mina_relay::mempool::MempoolView;
use mina_relay::{network_seeds, rpc_net, subscribe_gossip};
use mina_verify::{
    block_from_gossip_payload, staking_epoch_ledger_hash, sync_ledger_queries,
    verify_account_at_root, Block, Verifier, LEDGER_DEPTH,
};
use serde::{Deserialize, Serialize};

/// The latest proof-verified tip and the data derived from it.
#[derive(Clone)]
struct TipInfo {
    block: Block,
    height: u32,
    state_hash: String,
}

struct AppState {
    network: String,
    chain_id: &'static str,
    peers: &'static [&'static str],
    tip: RwLock<Option<TipInfo>>,
    mempool: Mutex<MempoolView>,
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
    });

    // Verify-before-tip worker thread (multi-second crypto, off the async runtime).
    let (block_tx, block_rx) = mpsc::channel::<Vec<u8>>();
    {
        let net = network.clone();
        let state = state.clone();
        std::thread::spawn(move || {
            let verifier = match std::env::var("MINA_VK_JSON") {
                Ok(path) => Verifier::with_index_json(
                    &std::fs::read_to_string(&path).expect("read MINA_VK_JSON"),
                )
                .expect("verifier from VK json"),
                Err(_) => Verifier::for_network(&net).expect("verifier"),
            };
            while let Ok(payload) = block_rx.recv() {
                let block = match block_from_gossip_payload(&payload) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                // Trust gate: only a proof-verified block can become the tip.
                match verifier.verify_tip(block) {
                    Ok(Some(t)) => {
                        let height = t.height();
                        let info = TipInfo {
                            state_hash: t.state_hash().to_string(),
                            block: t.block().clone(),
                            height,
                        };
                        let mut tip = state.tip.write().unwrap();
                        // Only advance forward (ignore older/equal gossiped blocks).
                        if tip.as_ref().map(|t| height > t.height).unwrap_or(true) {
                            log::info!("verified tip h{height}");
                            *tip = Some(info);
                        }
                    }
                    Ok(None) => log::warn!("rejected invalid block proof — not adopting as tip"),
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
        staking_epoch_ledger_hash: staking_epoch_ledger_hash(&tip.block).to_string(),
    }))
}

#[derive(Deserialize)]
struct AccountQuery {
    /// Leaf index in the epoch ledger — an untrusted hint (from the indexer). A wrong
    /// hint cannot forge a balance: the path won't fold, or the pubkey won't match.
    index: u64,
    /// Optional: cross-check the proved account's public key against this address.
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

    // UNTRUSTED fetch: walk the sync-ledger for the account + Merkle path.
    let queries = sync_ledger_queries(q.index, LEDGER_DEPTH);
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
    let acct = verify_account_at_root(&root, q.index, LEDGER_DEPTH, &answers)
        .map_err(|e| err(StatusCode::BAD_GATEWAY, format!("account did not verify: {e}")))?;

    let public_key = acct.public_key.into_address();
    if let Some(want) = &q.pubkey {
        if &public_key != want {
            return Err(err(
                StatusCode::NOT_FOUND,
                format!("index {} holds {public_key}, not requested {want}", q.index),
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
