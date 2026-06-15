//! The Mina light node.
//!
//! A trustless light client that *participates* in the network: it joins the p2p
//! gossip network via [`mina_relay`] and (TODO) verifies the chain from its
//! recursive SNARK proof via `mina-verify`. Unlike an RPC light *client*, it is a
//! real p2p *node* — that's what `mina-relay` adds.
//!
//! Architecture (see ARCHITECTURE.md / the trustless-rosetta-arch doc):
//!   network (untrusted)  ──mina-relay──▶  candidate tip  ──mina-verify──▶  verified tip
//!
//! Env: `MINA_NETWORK` (devnet|mainnet), `LIGHT_NODE_SECS` (run timeout, default 600).

use std::{ops::ControlFlow, time::Duration};

use mina_relay::{network_seeds, subscribe_blocks};

#[tokio::main]
async fn main() {
    env_logger::init();

    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let (chain_id, peers) = network_seeds(&network)
        .unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?} (devnet|mainnet)"));
    let secs: u64 = std::env::var("LIGHT_NODE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);

    log::info!("mina-light-node starting on {network} ({} seed peers)", peers.len());

    // Skeleton: follow the gossip network and surface candidate tips.
    // TODO(trust gate): hand each candidate to `mina-verify` (verify the tip proof,
    //   trust the linked prefix); mark a tip "verified" only after the proof checks.
    // TODO(reads): expose verified-tip + account Merkle-proof reads (via mina-verify).
    // TODO(mempool): tap the tx-pool gossip topic -> bounded, TTL'd mempool view.
    // TODO(broadcast): publish signed txs to the tx-pool gossip topic.
    // TODO(liveness): expose the live p2p tip so consumers can cross-check a GCS tip.
    subscribe_blocks(
        chain_id,
        peers,
        Some(Duration::from_secs(secs)),
        |payload| {
            // A raw gossip NewState payload (untrusted). In the full node this is
            // decoded + forwarded to the verifier; here we just log it.
            log::info!("candidate block: {} bytes (unverified)", payload.len());
            ControlFlow::Continue(())
        },
        |peers| {
            log::debug!("connected peers: {peers}");
            ControlFlow::Continue(())
        },
    )
    .await;

    log::info!("mina-light-node stopped");
}
