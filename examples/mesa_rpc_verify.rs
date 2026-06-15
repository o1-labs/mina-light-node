// Fetch mesa-mut best tip via RPC (deterministic) and verify + print computed state hash.
use std::time::Duration;
use mina_verify::Verifier;
use mina_verify_capture::rpc_net;

const CHAIN_ID: &str = "8b8ccbf273ef48aa0193ed634e69540657f0fc4292c9919a54b76a21b104abb2";
const PEERS: &[&str] = &[
    "/ip4/37.27.25.96/tcp/55883/p2p/12D3KooWFocPgQjsFU7uSeP2JJ4ZhVSX1rMcPPQDb6k8nw8Vn4nU",
    "/ip4/37.27.136.38/tcp/5874/p2p/12D3KooWFR5TPkzF7fUGpZsFjS9dRdTUdJ2S9oJ9DPaAj27mugjg",
    "/ip4/37.27.136.38/tcp/13524/p2p/12D3KooWEFiRm9sU8zzswAqGspUESjjJGp46f5unmdjvy1CpEwan",
    "/ip4/178.105.168.66/tcp/8302/p2p/12D3KooWBoHDNC34A4oM5odpeBzq3Jrq2Zror5c5krYdSKPUgA36",
    "/ip4/167.233.27.225/tcp/18415/p2p/12D3KooWCpxLfDvbKxWaHxQP1rDXxxA7YsVXU47KYmX4YcKzHTLD",
    "/ip4/157.180.1.110/tcp/1940/p2p/12D3KooWLfer8ojmrC1a2km5rdZSZsz3aDwLUby6Puz3iR2qQgqA",
    "/ip4/65.109.4.219/tcp/8302/p2p/12D3KooWJS5zBTbBZKnrQJ46e9irS84du426CZZgEVjs32Bp5LoX",
    "/ip4/65.109.48.175/tcp/8302/p2p/12D3KooWKE8NSGK4VDtNas1MiGZ1MkerDpLNkwbW19mbieVSHh5x",
    "/ip4/15.235.55.176/tcp/8302/p2p/12D3KooWFQ7GmguCqSQUQZZbwDZiuBpdaNhyMM7SYz5yS9obz11o",
    "/ip4/65.109.37.38/tcp/8302/p2p/12D3KooWKsDeitYKn7EkBCqSU7hrfrW4DyQopQ5WWiV3E6ie3H7b",
    "/ip4/65.108.0.140/tcp/1316/p2p/12D3KooWKeRQ9ePgDA8DcNrRzemLsbbpA2TgaEkrNcTqKafCrPP3",
    "/ip4/15.235.230.161/tcp/8302/p2p/12D3KooWAm9evB8m5Q9djPzV3xRPq1RNo9JodNwaGz5DMbfuKEr1",
    "/ip4/167.233.30.88/tcp/8302/p2p/12D3KooWRvFe1ruPwz7zgjs7yrVE3gqD394ka3zFep1UFgnwWALj",
    "/ip4/54.36.165.140/tcp/8302/p2p/12D3KooWNTtJmyVaFMyBzAXyNsPu1sCrBAaeZxxacxKou5r2X92b",
    "/ip4/139.84.156.198/tcp/8302/p2p/12D3KooWLbDdXXcsBBJXVFQaBcQWgDyxF4r1EuqqGfVaYf71QNpZ",
    "/ip4/157.180.1.108/tcp/13085/p2p/12D3KooWEaF1ED3t2FyLhPirybL83puLBSUfFTzeeTq16pCZyAEi",
    "/ip4/128.116.219.252/tcp/8303/p2p/12D3KooWPEoKvoDk4iA7yD9VsbRH1LkuPqUgHSnhjqFe7gpkKcAc",
    "/ip4/65.108.67.35/tcp/8302/p2p/12D3KooWPqKhGtDDqz4DCbyoJ9P2JWAWKGMRUCnqcKcn9G44KZVv",
];

#[tokio::main]
async fn main() {
    env_logger::init();
    let vk_path = std::env::args().nth(1).expect("usage: <mesa_vk.json>");
    let verifier = Verifier::with_index_json(&std::fs::read_to_string(&vk_path).unwrap()).unwrap();
    println!("RPC get_best_tip on mesa-mut...");
    match rpc_net::fetch_best_tip(CHAIN_ID, PEERS, Duration::from_secs(120)).await {
        Ok(block) => {
            let h = block.header.protocol_state.body.consensus_state.blockchain_length.as_u32();
            let sh = block.header.try_hash().map(|x| x.to_string()).unwrap_or_else(|e| format!("{e:?}"));
            let ok = verifier.verify_block(&block);
            println!("height {h} computed_state_hash={sh} verify_block={ok}");
        }
        Err(e) => { eprintln!("rpc failed: {e}"); std::process::exit(1); }
    }
}
