//! End-to-end trust-gate test against the **live devnet p2p network**.
//!
//! Joins devnet gossip via `mina-relay`, captures the first block, and asserts its
//! blockchain SNARK proof verifies via `mina-verify` — the exact path the node runs.
//!
//! Ignored by default (needs outbound p2p + several seconds of crypto). Run it with:
//!   cargo test -p mina-light-node --test devnet_e2e -- --ignored --nocapture

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mina_relay::{network_seeds, subscribe_blocks};
use mina_verify::{block_from_gossip_payload, Verifier};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "hits the live devnet p2p network; run with --ignored"]
async fn devnet_tip_verifies_end_to_end() {
    let (chain_id, peers) = network_seeds("devnet").expect("devnet seeds");

    // Capture the first gossiped block, then leave the mesh.
    let captured: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let sink = captured.clone();
    subscribe_blocks(
        chain_id,
        peers,
        Some(Duration::from_secs(180)),
        move |payload| {
            *sink.lock().unwrap() = Some(payload.to_vec());
            ControlFlow::Break(())
        },
        |_| ControlFlow::Continue(()),
    )
    .await;

    let payload = captured
        .lock()
        .unwrap()
        .take()
        .expect("received at least one devnet block within 180s");

    let block = block_from_gossip_payload(&payload).expect("decode gossip block");
    let verifier = Verifier::devnet();
    assert!(
        verifier.verify_block(&block),
        "a live devnet tip's proof must verify",
    );
}
