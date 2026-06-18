//! Shared internals for the `mina-light-node` binaries (the node, the HTTP server, and
//! the `mapgen` index-map generator).

pub mod index_map;

use std::time::Duration;

use mina_p2p_messages::v2::{
    LedgerHash, MinaLedgerSyncLedgerAnswerStableV2, MinaLedgerSyncLedgerQueryStableV1,
};
use mina_relay::rpc_net::fetch_sync_ledger_answers;
use mina_verify::{ledger_sweep_queries, pubkey_index_pairs, sweep_base_index, LEDGER_DEPTH};

/// Sweep the epoch-ledger leaves `[covered, num)` into `(num, (pubkey, index) pairs)`.
///
/// `NumAccounts` sizes the ledger; the tail is then swept in **chunks over fresh
/// connections** with retry — a single connection drops (`unexpected end of file`)
/// partway through a many-thousand-query walk. Shared by the server's runtime sweep and
/// the `mapgen` release-time generator. The pairs are an untrusted hint; every account
/// read still re-proves Merkle inclusion against the verified root.
pub async fn sweep_index_map(
    chain_id: &str,
    peers: &[&str],
    root: LedgerHash,
    covered: u64,
) -> Result<(u64, Vec<(String, u64)>), String> {
    let na = fetch_sync_ledger_answers(
        chain_id,
        peers,
        root.clone(),
        &[MinaLedgerSyncLedgerQueryStableV1::NumAccounts],
        Duration::from_secs(60),
    )
    .await?;
    let num = match na.into_iter().next() {
        Some(MinaLedgerSyncLedgerAnswerStableV2::NumAccounts(n, _)) => n.0,
        other => return Err(format!("expected NumAccounts, got {other:?}")),
    };
    if num <= covered {
        return Ok((num, Vec::new()));
    }

    const CHUNK: u64 = 256 * 32; // ≈8k accounts (≈256 What_contents queries) per connection
    let mut pairs = Vec::new();
    let mut start = sweep_base_index(covered);
    while start < num {
        let end = (start + CHUNK).min(num);
        let queries = ledger_sweep_queries(start, end, LEDGER_DEPTH);
        let mut attempt = 0;
        let answers = loop {
            match fetch_sync_ledger_answers(
                chain_id,
                peers,
                root.clone(),
                &queries,
                Duration::from_secs(120),
            )
            .await
            {
                Ok(a) => break a,
                Err(e) if attempt < 5 => {
                    attempt += 1;
                    log::debug!("sweep chunk {start}..{end} retry {attempt}: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(format!("sweep chunk {start}..{end} failed: {e}")),
            }
        };
        pairs.extend(pubkey_index_pairs(&answers, start, LEDGER_DEPTH).map_err(|e| e.to_string())?);
        log::info!("epoch-ledger sweep: {end}/{num} accounts mapped");
        start = end;
    }
    Ok((num, pairs))
}
