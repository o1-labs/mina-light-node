//! Validate the tx-broadcast path against live devnet, end to end.
//!
//!   MINA_NETWORK=devnet cargo run -p mina-relay --example broadcast_tx
//!
//! 1. Capture a real, already-signed pending transaction off the tx-pool gossip.
//! 2. Hermetic check: our encoder round-trips through the decoder on that real tx
//!    (proves framing + binprot are wire-correct, no network needed for this step).
//! 3. Re-broadcast the signed tx to the gossip topic and watch for propagation.
//!
//! Re-broadcasting a tx the network already holds proves the *publish path* works
//! (no rejection, mesh forms, bytes accepted). Peers usually won't re-echo a tx
//! already in their pool, so `echoes == 0` is expected here; a brand-new signed tx
//! (via the Construction API + offline signer) is what exercises fresh propagation.

use std::ops::ControlFlow;
use std::time::Duration;

use mina_relay::broadcast::{broadcast_tx, encode_tx_pool_diff};
use mina_relay::mempool::{command_id, tx_pool_diff_from_gossip};
use mina_relay::{network_seeds, subscribe_gossip};

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) = network_seeds(&network).expect("known network (devnet|mainnet|mesa-mut)");
    let capture_secs: u64 = std::env::var("CAPTURE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(240);

    // 1. Capture a real, already-signed pending tx off gossip.
    eprintln!("[1] waiting up to {capture_secs}s for a live pending tx on {network}…");
    let mut captured = None;
    subscribe_gossip(
        chain_id,
        peers,
        Some(Duration::from_secs(capture_secs)),
        |payload| match tx_pool_diff_from_gossip(payload).into_iter().next() {
            Some(cmd) => {
                captured = Some(cmd);
                ControlFlow::Break(())
            }
            None => ControlFlow::Continue(()),
        },
        |_| ControlFlow::Continue(()),
    )
    .await;

    let cmd = match captured {
        Some(c) => c,
        None => {
            eprintln!("no pending tx seen in the window; devnet tx flow is sporadic — retry later");
            std::process::exit(1);
        }
    };
    let id = command_id(&cmd);
    eprintln!("    captured tx {id}");

    // 2. Hermetic check: our encoder round-trips through the decoder on a REAL tx.
    let payload = encode_tx_pool_diff(vec![cmd.clone()], 1);
    let decoded = tx_pool_diff_from_gossip(&payload);
    assert_eq!(decoded.len(), 1, "re-encoded payload must decode to exactly one command");
    assert_eq!(command_id(&decoded[0]), id, "round-trip must preserve the canonical tx hash");
    eprintln!("[2] ✓ encode→decode round-trip preserves the tx (framing + binprot wire-correct)");

    // 3. Re-broadcast the signed tx and watch for propagation.
    eprintln!("[3] re-broadcasting to the tx-pool gossip topic…");
    match broadcast_tx(
        chain_id,
        peers,
        vec![cmd],
        Duration::from_secs(30),
        Duration::from_secs(120),
    )
    .await
    {
        Ok(out) => {
            eprintln!(
                "    ✓ published {} tx; {} echo(es) seen (0 expected for an already-pooled tx)",
                out.tx_ids.len(),
                out.echoes
            );
        }
        Err(e) => {
            eprintln!("    ✗ broadcast failed: {e}");
            std::process::exit(2);
        }
    }
}
