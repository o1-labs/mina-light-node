//! Best-effort mempool view, tapped from the transaction-pool gossip topic.
//!
//! This is **not trustless**: a pending transaction isn't in any proven state, so
//! there is nothing to verify — it's simply *this node's* view of what's propagating
//! on the network. No single trusted gatekeeper, but no proof either. Used to back a
//! Rosetta `/mempool` (best-effort) and to observe whether a submitted tx is spreading.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use binprot::{BinProtRead, BinProtWrite};
use mina_p2p_messages::gossip::GossipNetMessageV2;
use mina_p2p_messages::v2::MinaBaseUserCommandStableV2;

/// Decode a raw gossip payload (`[8-byte len][GossipNetMessageV2 binprot]`) as a
/// transaction-pool diff, returning its user commands. Empty for any other message
/// type (block / snark-pool diff) or on decode error.
pub fn tx_pool_diff_from_gossip(payload: &[u8]) -> Vec<MinaBaseUserCommandStableV2> {
    // tag at offset 8: 2 = TransactionPoolDiff.
    if payload.len() < 9 || payload.get(8) != Some(&2) {
        return Vec::new();
    }
    let mut cursor = &payload[8..];
    match GossipNetMessageV2::binprot_read(&mut cursor) {
        Ok(GossipNetMessageV2::TransactionPoolDiff { message, .. }) => {
            message.0.into_iter().collect()
        }
        _ => Vec::new(),
    }
}

/// A content-addressed id for a user command (blake2b of its binprot encoding).
///
/// NOTE: this is a stable *dedup* key, NOT the canonical Mina transaction hash that a
/// Rosetta `transaction_identifier` uses. Canonical hashing is a TODO for the adapter.
pub fn command_id(cmd: &MinaBaseUserCommandStableV2) -> String {
    use blake2::{Blake2b512, Digest};
    let mut bytes = Vec::new();
    cmd.binprot_write(&mut bytes)
        .expect("binprot_write to a Vec is infallible");
    let digest = Blake2b512::digest(&bytes);
    hex::encode(&digest[..16]) // 32-hex short id
}

/// A pending transaction observed on gossip.
#[derive(Clone)]
pub struct PendingTx {
    pub id: String,
    pub command: MinaBaseUserCommandStableV2,
    pub first_seen: Instant,
}

/// A bounded, TTL'd view of pending transactions tapped from tx-pool gossip.
/// Best-effort and untrusted — see the module docs.
pub struct MempoolView {
    txs: HashMap<String, PendingTx>,
    capacity: usize,
    ttl: Duration,
}

impl MempoolView {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            txs: HashMap::new(),
            capacity,
            ttl,
        }
    }

    /// Tap a raw gossip payload. If it's a transaction-pool diff, add its (new)
    /// commands to the view. Returns the number of NEW transactions added.
    pub fn ingest_gossip(&mut self, payload: &[u8]) -> usize {
        self.expire();
        let mut added = 0;
        for cmd in tx_pool_diff_from_gossip(payload) {
            let id = command_id(&cmd);
            if !self.txs.contains_key(&id) {
                self.txs.insert(
                    id.clone(),
                    PendingTx {
                        id,
                        command: cmd,
                        first_seen: Instant::now(),
                    },
                );
                added += 1;
            }
        }
        self.enforce_capacity();
        added
    }

    /// Drop transactions older than the TTL.
    pub fn expire(&mut self) {
        let ttl = self.ttl;
        self.txs.retain(|_, tx| tx.first_seen.elapsed() < ttl);
    }

    fn enforce_capacity(&mut self) {
        if self.txs.len() <= self.capacity {
            return;
        }
        let mut by_age: Vec<(String, Instant)> = self
            .txs
            .iter()
            .map(|(k, v)| (k.clone(), v.first_seen))
            .collect();
        by_age.sort_by_key(|(_, t)| *t);
        let drop_n = self.txs.len() - self.capacity;
        for (k, _) in by_age.into_iter().take(drop_n) {
            self.txs.remove(&k);
        }
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }
    pub fn ids(&self) -> Vec<String> {
        self.txs.keys().cloned().collect()
    }
    pub fn iter(&self) -> impl Iterator<Item = &PendingTx> {
        self.txs.values()
    }
    pub fn contains(&self, id: &str) -> bool {
        self.txs.contains_key(id)
    }
}
