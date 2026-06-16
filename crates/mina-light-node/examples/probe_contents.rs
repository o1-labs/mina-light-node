//! Probe how the live daemon batches `WhatContents` on the staking epoch ledger:
//! query the leftmost subtree at a range of depths and report how many accounts come
//! back. This sizes the epoch-ledger sweep (1 account/call = infeasible; a shallow
//! batch = a cheap one-time sweep to build pubkey->index).
//!   cargo run --example probe_contents -p mina-light-node -- [network]

use std::time::Duration;

use mina_p2p_messages::number::Number;
use mina_p2p_messages::v2::{
    MerkleAddressBinableArgStableV1 as Addr, MinaLedgerSyncLedgerAnswerStableV2 as Answer,
    MinaLedgerSyncLedgerQueryStableV1 as Query,
};
use mina_relay::{network_seeds, rpc_net};
use mina_verify::{staking_epoch_ledger_hash, Verifier};

fn what_contents(depth: usize) -> Query {
    let bytes = vec![0u8; depth.div_ceil(8)]; // leftmost subtree (index 0)
    Query::WhatContents(Addr(Number(depth as u64), bytes.into()))
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::args().nth(1).unwrap_or_else(|| "devnet".into());
    let (chain_id, peers) = network_seeds(&network).expect("known network");
    let deadline = Duration::from_secs(90);

    let block = rpc_net::fetch_best_tip(chain_id, peers, deadline)
        .await
        .expect("fetch best tip");
    let verifier = Verifier::for_network(&network).expect("verifier");
    assert!(verifier.verify_block(&block), "tip proof failed");
    let root = staking_epoch_ledger_hash(&block);
    println!("staking epoch ledger root {root}\n");

    for depth in [35usize, 34, 33, 32, 30, 28, 25, 22, 20] {
        match rpc_net::fetch_sync_ledger_answers(
            chain_id,
            peers,
            root.clone(),
            &[what_contents(depth)],
            deadline,
        )
        .await
        {
            Ok(answers) => match answers.into_iter().next() {
                Some(Answer::ContentsAre(accounts)) => {
                    println!("  depth {depth:2} -> {} accounts/batch", accounts.len())
                }
                other => println!("  depth {depth:2} -> unexpected: {other:?}"),
            },
            Err(e) => {
                let short = if e.len() > 70 { format!("{}…", &e[..70]) } else { e };
                println!("  depth {depth:2} -> refused ({short})");
            }
        }
    }
}
