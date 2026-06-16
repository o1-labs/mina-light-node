//! Live **trustless account read** on devnet — the read trust-chain end to end.
//!
//!   cargo run --example account_read -p mina-light-node -- [index] [network]
//!
//! 1. Fetch the current best tip over libp2p RPC (UNTRUSTED bytes).
//! 2. Verify its recursive SNARK proof (`mina-verify`) — the TRUST GATE. The block now
//!    commits, via its proof, to the account-ledger Merkle root.
//! 3. Ask a peer for the account at `index` plus its Merkle path via the sync-ledger
//!    RPC (UNTRUSTED — the relay is a dumb pipe).
//! 4. Fold account + path to the verified root (`mina-verify`) — the TRUST GATE again.
//!    A peer that lies about the account, the path, or the ledger is rejected.
//!
//! Nothing the network said is trusted until step 4 folds it onto the proof-verified
//! root. `index` is itself an untrusted hint: a wrong index yields an account whose
//! path can't fold (or a pubkey you didn't ask for) — it can't forge a false balance.

use std::time::Duration;

use mina_relay::{network_seeds, rpc_net};
use mina_verify::{
    staking_epoch_ledger_hash, sync_ledger_queries, verify_account_at_root, Verifier, LEDGER_DEPTH,
};

#[tokio::main]
async fn main() {
    env_logger::init();
    let index: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let network = std::env::args().nth(2).unwrap_or_else(|| "devnet".into());
    let (chain_id, peers) = network_seeds(&network).expect("known network (devnet|mainnet|mesa-mut)");
    let deadline = Duration::from_secs(120);

    // 1. UNTRUSTED: fetch the best tip over RPC (no gossip-mesh wait).
    println!("[1] RPC get_best_tip on {network} ({} seeds)…", peers.len());
    let block = rpc_net::fetch_best_tip(chain_id, peers, deadline)
        .await
        .expect("fetch best tip");
    let height = block
        .header
        .protocol_state
        .body
        .consensus_state
        .blockchain_length
        .as_u32();

    // 2. TRUST GATE: verify the tip's proof. Its ledger root is now trustworthy.
    let verifier = match std::env::var("MINA_VK_JSON") {
        Ok(path) => Verifier::with_index_json(&std::fs::read_to_string(&path).expect("read MINA_VK_JSON"))
            .expect("verifier from VK json"),
        Err(_) => Verifier::for_network(&network).expect("verifier"),
    };
    assert!(verifier.verify_block(&block), "tip proof failed to verify — aborting");
    // The staking epoch ledger root is a field of the proven consensus state AND the
    // ledger live peers actually serve over sync-ledger (the staged tip root is not).
    let root = staking_epoch_ledger_hash(&block);
    println!("[2] verified tip h{height}; staking-epoch ledger root committed by its proof ✓");

    // 3. UNTRUSTED: walk the sync-ledger for the account at `index` + its Merkle path.
    let queries = sync_ledger_queries(index, LEDGER_DEPTH);
    println!("[3] sync-ledger walk for account index {index} ({} queries)…", queries.len());
    let answers =
        rpc_net::fetch_sync_ledger_answers(chain_id, peers, root.clone(), &queries, deadline)
            .await
            .expect("sync-ledger answers");

    // 4. TRUST GATE: fold account + path onto the proven root.
    match verify_account_at_root(&root, index, LEDGER_DEPTH, &answers) {
        Ok(account) => {
            println!("[4] ✓ TRUSTLESS account read verified against h{height}'s proof:");
            println!("      public_key: {:?}", account.public_key);
            println!("      token_id:   {:?}", account.token_id);
            println!("      balance:    {:?}", account.balance);
            println!("      nonce:      {:?}", account.nonce);
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("[4] ✗ account read REJECTED: {e}");
            std::process::exit(2);
        }
    }
}
