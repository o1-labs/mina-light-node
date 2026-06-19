//! Tap the transaction-pool gossip and print the live (best-effort) mempool view.
//!
//!   MINA_NETWORK=devnet MEMPOOL_SECS=180 cargo run -p mina-relay --example mempool
use std::ops::ControlFlow;
use std::time::Duration;

use mina_relay::mempool::MempoolView;
use mina_relay::{network_seeds, subscribe_gossip};

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) =
        network_seeds(&network).unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?}"));
    let secs: u64 = std::env::var("MEMPOOL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    let mut view = MempoolView::new(4096, Duration::from_secs(600));
    eprintln!("tapping {network} tx-pool gossip for {secs}s (best-effort, untrusted)...");

    let (_ban_tx, ban_rx) = tokio::sync::mpsc::unbounded_channel();
    subscribe_gossip(
        chain_id,
        peers,
        Some(Duration::from_secs(secs)),
        |_src, payload| {
            let added = view.ingest_gossip(payload);
            if added > 0 {
                eprintln!("  +{added} pending tx  (mempool view now: {})", view.len());
            }
            ControlFlow::Continue(())
        },
        |_peers| ControlFlow::Continue(()),
        ban_rx,
    )
    .await;

    eprintln!("\nfinal mempool view: {} pending tx", view.len());
    for tx in view.iter().take(10) {
        eprintln!("  {}", tx.id);
    }
}
