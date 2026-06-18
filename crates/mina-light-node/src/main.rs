//! The Mina light node — **verify-before-ingest**.
//!
//! A trustless light client that *participates* in the network: it joins the p2p
//! gossip network via [`mina_relay`] and verifies every block's recursive SNARK
//! proof via [`mina_verify`] BEFORE the block is allowed into the chain view.
//! Invalid blocks are rejected and never ingested — so the view can't be poisoned
//! by a lying or compromised peer. Unlike an RPC light *client*, this is a real
//! light *node*: it participates in the network (that's what `mina-relay` adds).
//!
//!   network (untrusted) ──mina-relay──▶ block ──mina-verify (verify_tip)──▶ ChainMonitor
//!
//! Verification (multi-second crypto) runs on a worker thread so it never blocks the
//! gossip event loop — otherwise we'd miss heartbeats and get pruned from the mesh.
//!
//! ## Query surface (HTTP)
//! A small read-only HTTP server (on its own thread) exposes the verified state:
//! - `GET /tip`     → the latest proof-verified best tip `{height, state_hash, network}`
//! - `GET /healthz` → process health + sync freshness
//!
//! Env: `MINA_NETWORK` (devnet|mainnet). `LIGHT_NODE_SECS` optionally bounds the run
//! (seconds) for tests/CI; unset = run forever. `BIND` HTTP listen addr (default
//! `0.0.0.0:8080`). `LIGHT_NODE_TIP_FILE` optional path for file-based tip IPC.
//! Set `RUST_LOG=info` for libp2p logs.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mina_relay::{network_seeds, subscribe_blocks};
use mina_verify::{block_from_gossip_payload, ChainMonitor, Ingest, Verifier};
use tiny_http::{Header, Method, Response, Server};

/// Default HTTP listen address for the query surface.
const DEFAULT_BIND: &str = "0.0.0.0:8080";

/// The latest proof-verified best tip.
#[derive(Clone)]
struct TipInfo {
    height: u32,
    state_hash: String,
}

/// Shared, read-only-from-HTTP node state. The verify worker is the sole writer.
struct NodeState {
    network: String,
    started: Instant,
    tip: RwLock<Option<TipInfo>>,
    verified: AtomicU64,
    rejected: AtomicU64,
    /// Unix seconds of the last successful verification (0 = none yet) — sync freshness.
    last_verified_unix: AtomicU64,
}

impl NodeState {
    fn new(network: String) -> Self {
        Self {
            network,
            started: Instant::now(),
            tip: RwLock::new(None),
            verified: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            last_verified_unix: AtomicU64::new(0),
        }
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

    // Unset = run forever (a real node). A timeout is only for bounded test/CI runs.
    let deadline = std::env::var("LIGHT_NODE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(Duration::from_secs);
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) = network_seeds(&network)
        .unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?} (devnet|mainnet)"));
    let bind = std::env::var("BIND").unwrap_or_else(|_| DEFAULT_BIND.into());

    eprintln!(
        "mina-light-node on {network} ({}) — verifying every block before ingest\n",
        deadline.map_or("forever".into(), |d| format!("{}s", d.as_secs())),
    );

    let state = Arc::new(NodeState::new(network.clone()));

    // HTTP query surface on its own thread (synchronous, read-only).
    {
        let state = state.clone();
        std::thread::spawn(move || serve_http(state, &bind));
    }

    // Worker thread: verify-before-ingest, off the gossip event loop.
    let net = network.clone();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let worker_state = state.clone();
    let worker = std::thread::spawn(move || verify_loop(&net, rx, worker_state));

    subscribe_blocks(
        chain_id,
        peers,
        deadline,
        |payload| {
            // Hand off instantly; verification happens on the worker thread.
            let _ = tx.send(payload.to_vec());
            ControlFlow::Continue(())
        },
        |_| ControlFlow::Continue(()),
    )
    .await;

    drop(tx); // close the channel so the worker drains and prints its summary
    let _ = worker.join();
}

/// The trust gate: verify each gossiped block before ingesting it. Sole writer of
/// [`NodeState`]. Returns when the channel closes.
fn verify_loop(network: &str, rx: mpsc::Receiver<Vec<u8>>, state: Arc<NodeState>) {
    let verifier = Verifier::for_network(network).expect("verifier for network");
    let mut monitor = ChainMonitor::new(512);

    while let Ok(payload) = rx.recv() {
        let block = match block_from_gossip_payload(&payload) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  decode error (skipped): {e}");
                continue;
            }
        };
        // --- verify-before-ingest: untrusted sender, no trust in the bytes ---
        match verifier.verify_tip(block) {
            Ok(Some(tip)) => {
                let height = tip.height();
                let outcome = monitor.ingest(&tip);
                state.verified.fetch_add(1, Ordering::Relaxed);
                state
                    .last_verified_unix
                    .store(now_unix(), Ordering::Relaxed);
                report(height, &outcome);
                // Adopting a new best tip: publish it (HTTP /tip, stdout JSON, file).
                if matches!(
                    outcome,
                    Ingest::Genesis | Ingest::Extend { .. } | Ingest::Reorg { .. }
                ) {
                    let state_hash = tip.state_hash().to_string();
                    *state.tip.write().unwrap() = Some(TipInfo {
                        height,
                        state_hash: state_hash.clone(),
                    });
                    expose_tip(height, &state_hash);
                }
            }
            Ok(None) => {
                state.rejected.fetch_add(1, Ordering::Relaxed);
                eprintln!("  ✗ REJECTED: invalid proof — NOT ingested");
            }
            Err(e) => eprintln!("  ✗ malformed block (skipped): {e:?}"),
        }
    }

    eprintln!(
        "\ndone: {} verified block(s) ingested, {} rejected. best height: {:?}",
        state.verified.load(Ordering::Relaxed),
        state.rejected.load(Ordering::Relaxed),
        monitor.best_height()
    );
}

/// Serve the read-only HTTP query surface until the process exits.
fn serve_http(state: Arc<NodeState>, bind: &str) {
    let server = match Server::http(bind) {
        Ok(s) => s,
        Err(e) => {
            // The node is still useful without HTTP (stdout/file tip), so don't crash.
            log::error!("HTTP bind {bind} failed: {e}; query surface disabled");
            return;
        }
    };
    log::info!("HTTP query surface on http://{bind} (GET /tip, /healthz)");
    let json =
        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).expect("valid header");
    for req in server.incoming_requests() {
        let (code, body) = route(&state, req.method(), req.url());
        let resp = Response::from_string(body)
            .with_status_code(code)
            .with_header(json.clone());
        let _ = req.respond(resp);
    }
}

/// Pure request router — returns `(status, json_body)`. Kept side-effect-free so it's
/// unit-testable without a socket.
fn route(state: &NodeState, method: &Method, url: &str) -> (u16, String) {
    let path = url.split('?').next().unwrap_or(url);
    match (method, path) {
        (&Method::Get, "/healthz") => {
            let last = state.last_verified_unix.load(Ordering::Relaxed);
            let since = if last == 0 {
                serde_json::Value::Null
            } else {
                now_unix().saturating_sub(last).into()
            };
            let body = serde_json::json!({
                "status": "ok",
                "network": state.network,
                "uptime_secs": state.started.elapsed().as_secs(),
                "verified": state.verified.load(Ordering::Relaxed),
                "rejected": state.rejected.load(Ordering::Relaxed),
                "seconds_since_last_verified": since,
            });
            (200, body.to_string())
        }
        (&Method::Get, "/tip") => match &*state.tip.read().unwrap() {
            Some(t) => {
                let body = serde_json::json!({
                    "height": t.height,
                    "state_hash": t.state_hash,
                    "network": state.network,
                });
                (200, body.to_string())
            }
            None => (
                503,
                serde_json::json!({ "error": "no verified tip yet" }).to_string(),
            ),
        },
        _ => (
            404,
            serde_json::json!({ "error": "not found; try GET /tip or GET /healthz" }).to_string(),
        ),
    }
}

fn report(height: u32, outcome: &Ingest) {
    let line = match outcome {
        Ingest::Genesis => "first verified tip — best".into(),
        Ingest::Extend { .. } => "✓ extends best".into(),
        Ingest::Duplicate => "· duplicate".into(),
        Ingest::Behind { .. } => "· behind best (orphan/older)".into(),
        Ingest::Reorg {
            depth,
            common_ancestor,
            ..
        } => format!(
            "⟳ REORG to new best (rolled back {}, diverged at {})",
            depth.map(|d| d.to_string()).unwrap_or_else(|| "?".into()),
            common_ancestor.as_deref().unwrap_or("<unknown>")
        ),
        Ingest::Fork { common_ancestor } => format!(
            "⑂ FORK competing branch (diverged at {})",
            common_ancestor.as_deref().unwrap_or("<unknown>")
        ),
        Ingest::Unlinked => "? unlinked (ancestor outside window)".into(),
    };
    eprintln!("  ✓ verified h{height} — {line}");
}

/// Mirror the best **proof-verified** tip to stdout (structured JSON line) and, if
/// `LIGHT_NODE_TIP_FILE` is set, atomically to that file — for consumers doing a
/// liveness cross-check (compare this p2p-verified tip against a less-trusted source).
fn expose_tip(height: u32, state_hash: &str) {
    println!(r#"{{"verified_tip":{{"height":{height},"state_hash":"{state_hash}"}}}}"#);
    if let Ok(path) = std::env::var("LIGHT_NODE_TIP_FILE") {
        let json = format!("{{\"height\":{height},\"state_hash\":\"{state_hash}\"}}\n");
        let tmp = format!("{path}.tmp");
        if std::fs::write(&tmp, json).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> NodeState {
        NodeState::new("devnet".into())
    }

    #[test]
    fn tip_is_503_until_first_verified() {
        let (code, body) = route(&state(), &Method::Get, "/tip");
        assert_eq!(code, 503);
        assert!(body.contains("no verified tip"));
    }

    #[test]
    fn tip_returns_height_and_hash_once_set() {
        let s = state();
        *s.tip.write().unwrap() = Some(TipInfo {
            height: 528992,
            state_hash: "3NKabc".into(),
        });
        let (code, body) = route(&s, &Method::Get, "/tip");
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["height"], 528992);
        assert_eq!(v["state_hash"], "3NKabc");
        assert_eq!(v["network"], "devnet");
    }

    #[test]
    fn healthz_ok_and_reports_counters() {
        let s = state();
        s.verified.store(7, Ordering::Relaxed);
        s.rejected.store(2, Ordering::Relaxed);
        let (code, body) = route(&s, &Method::Get, "/healthz");
        assert_eq!(code, 200);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["verified"], 7);
        assert_eq!(v["rejected"], 2);
        // never verified yet -> null, not 0
        assert!(v["seconds_since_last_verified"].is_null());
    }

    #[test]
    fn unknown_path_is_404() {
        let (code, _) = route(&state(), &Method::Get, "/nope");
        assert_eq!(code, 404);
    }

    #[test]
    fn query_string_is_ignored() {
        let (code, _) = route(&state(), &Method::Get, "/healthz?foo=bar");
        assert_eq!(code, 200);
    }
}
