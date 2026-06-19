//! Generate the baked `pubkey → leaf-index` map for a network — the release-time step
//! that lets `mina-light-node-server` answer `/account?pubkey=` without a cold-start
//! sweep. Fetch + verify the tip, derive its staking-epoch-ledger root, sweep the ledger
//! over the sync-ledger RPC, and write the compact `.bin` (see `index_map`).
//!
//! The map is an untrusted hint (every read re-proves Merkle inclusion), so it is safe to
//! ship/bake. Indices are append-only, so it only goes stale for accounts created after
//! the build — the server tail-sweeps those.
//!
//!   MINA_NETWORK=devnet mina-light-node-mapgen [out.bin]   # default: <network>_index_map.bin

use std::time::Duration;

use mina_light_node::{index_map, sweep_index_map};
use mina_relay::network_seeds;
use mina_relay::rpc_net::fetch_best_tip;
use mina_verify::{staking_epoch_ledger_hash, Verifier};

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::var("MINA_NETWORK").unwrap_or_else(|_| "devnet".into());
    let out = std::env::args()
        .nth(1)
        .unwrap_or_else(|| format!("{network}_index_map.bin"));
    let (chain_id, peers) =
        network_seeds(&network).unwrap_or_else(|| panic!("unknown MINA_NETWORK {network:?}"));

    eprintln!("[{network}] fetching + verifying tip…");
    let block = fetch_best_tip(chain_id, peers, Duration::from_secs(120))
        .await
        .expect("fetch best tip");
    let verifier = Verifier::for_network(&network).expect("verifier for network");
    assert!(verifier.verify_block(&block), "tip proof did not verify");
    let root = staking_epoch_ledger_hash(&block);

    eprintln!("[{network}] sweeping staking-epoch ledger {root} …");
    let (num, pairs) = sweep_index_map(chain_id, peers, root, 0)
        .await
        .expect("epoch-ledger sweep");
    let bin = index_map::build(&pairs, num);
    std::fs::write(&out, &bin).expect("write index map");
    eprintln!(
        "[{network}] wrote {out}: {} pairs → {} bytes (covered {num})",
        pairs.len(),
        bin.len(),
    );
}
