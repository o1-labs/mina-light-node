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
//! Env: `MINA_NETWORK` (devnet|mainnet). `LIGHT_NODE_SECS` optionally bounds the run
//! (in seconds) for tests/CI; unset = run forever, the out-of-the-box default.
//! Set `RUST_LOG=info` for libp2p logs.

// jemalloc returns freed memory to the OS far better than glibc malloc, whose per-thread
// arenas retain the verifier's large transient allocations and ratchet RSS to a high
// plateau (see workspace Cargo.toml).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::ops::ControlFlow;
use std::sync::mpsc;
use std::time::Duration;

use mina_relay::{network_seeds, subscribe_blocks};
use mina_verify::{block_from_gossip_payload, ChainMonitor, Ingest, Verifier};

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

    eprintln!(
        "mina-light-node on {network} ({}) — verifying every block before ingest\n",
        deadline.map_or("forever".into(), |d| format!("{}s", d.as_secs())),
    );

    // Worker thread: verify-before-ingest, off the gossip event loop.
    //
    // BOUNDED queue: verification (seconds) is slower than gossip ingest and block payloads
    // are large, so an unbounded channel would let a gossip burst grow until OOM. Cap the
    // backlog and drop-newest on full — gossip re-delivers, so a dropped block re-arrives
    // once the worker drains.
    const BLOCK_QUEUE: usize = 256;
    let net = network.clone();
    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(BLOCK_QUEUE);
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
                    // Expose the new best verified tip (for a consumer's liveness
                    // cross-check: compare this p2p-verified tip against a GCS tip).
                    if matches!(
                        outcome,
                        Ingest::Genesis | Ingest::Extend { .. } | Ingest::Reorg { .. }
                    ) {
                        expose_tip(height, &tip.state_hash().to_string());
                    }
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
        deadline,
        |payload| {
            // Hand off instantly; verification happens on the worker thread. Non-blocking
            // so a full queue never stalls the gossip loop — drop-newest, gossip re-delivers.
            if let Err(mpsc::TrySendError::Full(_)) = tx.try_send(payload.to_vec()) {
                log::warn!(
                    "verify queue full ({BLOCK_QUEUE}); dropped a block (gossip will re-deliver)"
                );
            }
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

/// Expose the current best **proof-verified** tip so a consumer (e.g. a trustless
/// indexer) can cross-check it against a less-trusted source (e.g. a GCS tip) —
/// divergence reveals staleness/withholding. Emits a structured JSON line on stdout
/// (stderr carries the human logs) and, if `LIGHT_NODE_TIP_FILE` is set, atomically
/// writes the tip there for file-based IPC.
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
