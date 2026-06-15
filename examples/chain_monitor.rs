//! Live trustless chain monitor — the "verify-before-ingest" pattern.
//!
//! Connects to devnet gossip and, for every block it receives from an *untrusted*
//! peer, verifies the blockchain SNARK proof BEFORE the block is allowed into the
//! chain monitor. Invalid blocks are rejected and never ingested. This is exactly
//! what a trustless indexer would do: every persisted row is proof-backed, so the
//! index can't be poisoned by a lying or compromised node.
//!
//! Verification (multi-second crypto) runs on a worker thread so it never blocks the
//! gossip event loop — otherwise we'd miss heartbeats and get pruned from the mesh.
//!
//! Env: CAPTURE_SECS (run duration, default 600). Set RUST_LOG=info for libp2p logs.

use std::ops::ControlFlow;
use std::sync::mpsc;
use std::time::Duration;

use mina_verify::{block_from_gossip_payload, ChainMonitor, Ingest, Verifier};
use mina_verify_capture::{network_seeds, subscribe_blocks};

#[tokio::main]
async fn main() {
    env_logger::init();

    let secs: u64 = std::env::var("CAPTURE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) = network_seeds(&network)
        .unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?} (devnet|mainnet)"));

    eprintln!("monitoring {network} gossip for {secs}s — verifying every block before ingest\n");

    // Worker thread: verify-before-ingest, off the gossip event loop.
    let net = network.clone();
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let worker = std::thread::spawn(move || {
        let verifier = Verifier::for_network(&net).expect("verifier for network");
        let mut monitor = ChainMonitor::new(512);
        let (mut ingested, mut rejected) = (0u64, 0u64);

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
                    ingested += 1; // a real indexer would persist `tip` here
                    report(height, &outcome);
                }
                Ok(None) => {
                    rejected += 1;
                    eprintln!("  ✗ REJECTED: invalid proof — NOT ingested");
                }
                Err(e) => eprintln!("  ✗ malformed block (skipped): {e:?}"),
            }
        }

        eprintln!(
            "\ndone: {ingested} verified block(s) ingested, {rejected} rejected. best height: {:?}",
            monitor.best_height()
        );
    });

    subscribe_blocks(
        chain_id,
        peers,
        Some(Duration::from_secs(secs)),
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
