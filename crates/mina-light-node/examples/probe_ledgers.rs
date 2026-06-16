//! Probe which proven ledger roots a live devnet peer will actually serve via the
//! sync-ledger RPC. Peers materialize only some ledgers (snarked root, epoch ledgers),
//! not the tip's staged root — this finds the right trustless-read target.
//!   cargo run --example probe_ledgers -p mina-light-node -- [network]

use std::time::Duration;

use mina_relay::{network_seeds, rpc_net};
use mina_p2p_messages::v2::MinaLedgerSyncLedgerQueryStableV1 as Q;
use mina_verify::Verifier;

#[tokio::main]
async fn main() {
    env_logger::init();
    let network = std::env::args().nth(1).unwrap_or_else(|| "devnet".into());
    let (chain_id, peers) = network_seeds(&network).expect("known network");
    let deadline = Duration::from_secs(120);

    let block = rpc_net::fetch_best_tip(chain_id, peers, deadline)
        .await
        .expect("fetch best tip");
    let verifier = Verifier::for_network(&network).expect("verifier");
    assert!(verifier.verify_block(&block), "tip proof failed");
    let bs = &block.header.protocol_state.body.blockchain_state;
    let cs = &block.header.protocol_state.body.consensus_state;
    let height = cs.blockchain_length.as_u32();
    println!("verified tip h{height}\n");

    let candidates = [
        ("staged non_snark", bs.staged_ledger_hash.non_snark.ledger_hash.clone()),
        ("genesis", bs.genesis_ledger_hash.clone()),
        ("snarked target first_pass", bs.ledger_proof_statement.target.first_pass_ledger.clone()),
        ("snarked target second_pass", bs.ledger_proof_statement.target.second_pass_ledger.clone()),
        ("snarked source first_pass", bs.ledger_proof_statement.source.first_pass_ledger.clone()),
        ("staking epoch ledger", cs.staking_epoch_data.ledger.hash.clone()),
        ("next epoch ledger", cs.next_epoch_data.ledger.hash.clone()),
    ];

    for (name, hash) in candidates {
        match rpc_net::fetch_sync_ledger_answers(chain_id, peers, hash, &[Q::NumAccounts], deadline).await {
            Ok(answers) => println!("  ✓ SERVED  {name:30} -> {:?}", answers.first()),
            Err(e) => {
                let short = if e.len() > 80 { format!("{}…", &e[..80]) } else { e };
                println!("  ✗ refused {name:30} ({short})");
            }
        }
    }
}
