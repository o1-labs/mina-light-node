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

use mina_relay::rpc_net::{fetch_best_tip, fetch_sync_ledger_answers};
use mina_relay::{network_seeds, subscribe_blocks};
use mina_verify::{
    block_from_gossip_payload, staking_epoch_ledger_hash, sync_ledger_queries,
    verify_account_at_root, Verifier, LEDGER_DEPTH,
};

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

/// Trustless account read end-to-end (the path `GET /account?index=` runs): fetch +
/// verify the tip, walk the sync-ledger for account 0, fold to the verified staking-
/// epoch-ledger root. Account 0 is the well-known devnet account.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "hits the live devnet p2p network; run with --ignored"]
async fn devnet_account0_balance_is_proof_backed() {
    let (chain_id, peers) = network_seeds("devnet").expect("devnet seeds");

    let block = fetch_best_tip(chain_id, peers, Duration::from_secs(120))
        .await
        .expect("fetch best tip");
    assert!(
        Verifier::devnet().verify_block(&block),
        "tip proof must verify before reading against it",
    );
    let root = staking_epoch_ledger_hash(&block);

    let queries = sync_ledger_queries(0, LEDGER_DEPTH);
    let answers = fetch_sync_ledger_answers(
        chain_id,
        peers,
        root.clone(),
        &queries,
        Duration::from_secs(120),
    )
    .await
    .expect("sync-ledger answers");

    let account = verify_account_at_root(&root, 0, LEDGER_DEPTH, &answers)
        .expect("account proven at index 0");
    assert_eq!(
        account.public_key.into_address(),
        "B62qiy32p8kAKnny8ZFwoMhYpBppM1DWVCqAPBYNcXnsAHhnfAAuXgg",
        "devnet account 0 public key",
    );
}
