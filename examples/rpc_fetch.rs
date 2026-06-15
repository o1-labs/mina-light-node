//! Live end-to-end: fetch the best tip over libp2p RPC (no gossip wait) and verify it.
//!   cargo run --example rpc_fetch -p mina-verify-monitor -- [devnet|mainnet|mesa-mut]

use std::time::Duration;

use mina_verify::Verifier;
use mina_verify_capture::{network_seeds, rpc_net};

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::args().nth(1).unwrap_or_else(|| "devnet".into());
    let (chain_id, peers) = network_seeds(&network).expect("known network");

    println!("RPC get_best_tip on {network} ({} seeds)...", peers.len());
    match rpc_net::fetch_best_tip(chain_id, peers, Duration::from_secs(90)).await {
        Ok(block) => {
            let height = block
                .header
                .protocol_state
                .body
                .consensus_state
                .blockchain_length
                .as_u32();
            let verifier = match std::env::var("MINA_VK_JSON") {
                Ok(path) => {
                    let json = std::fs::read_to_string(&path).expect("read MINA_VK_JSON");
                    Verifier::with_index_json(&json).expect("verifier from VK json")
                }
                Err(_) => Verifier::for_network(&network).expect("verifier"),
            };
            let ok = verifier.verify_block(&block);
            println!("fetched best tip height {height} via RPC — verify_block = {ok}");
            std::process::exit(if ok { 0 } else { 2 });
        }
        Err(e) => {
            eprintln!("rpc failed: {e}");
            std::process::exit(1);
        }
    }
}
