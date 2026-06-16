//! Transaction broadcast — the network-WRITE side of the relay.
//!
//! Publish a signed user command to the transaction-pool gossip topic so it propagates
//! to block producers (peer-to-peer submit; backs a Rosetta `/construction/submit`).
//! This is the inverse of the mempool tap ([`crate::mempool::tx_pool_diff_from_gossip`]):
//! identical framing, opposite direction. The relay only MOVES bytes — the command must
//! already be signed (the signer is client-side/offline; the relay holds no keys and
//! adds no trust).

use std::collections::HashSet;
use std::time::Duration;

use binprot::BinProtWrite;
use libp2p::futures::StreamExt;
use libp2p::{gossipsub, swarm::SwarmEvent, Multiaddr};
use mina_p2p_messages::gossip::GossipNetMessageV2;
use mina_p2p_messages::number::Number;
use mina_p2p_messages::v2::{
    MinaBaseUserCommandStableV2, NetworkPoolTransactionPoolDiffVersionedStableV2,
};

use crate::mempool::{command_id, tx_pool_diff_from_gossip};
use crate::transport::ed25519::{Keypair as EdKeypair, SecretKey};
use crate::CONSENSUS_TOPIC;

/// Encode signed user commands as a framed transaction-pool gossip payload
/// (`[8-byte LE len][GossipNetMessageV2::TransactionPoolDiff binprot]`) — the exact
/// bytes a Mina node publishes on the consensus topic. Inverse of
/// [`crate::mempool::tx_pool_diff_from_gossip`]; round-trips through it.
pub fn encode_tx_pool_diff(cmds: Vec<MinaBaseUserCommandStableV2>, nonce: i32) -> Vec<u8> {
    let msg = GossipNetMessageV2::TransactionPoolDiff {
        message: NetworkPoolTransactionPoolDiffVersionedStableV2(cmds.into_iter().collect()),
        nonce: Number(nonce),
    };
    let mut bytes = vec![0u8; 8];
    msg.binprot_write(&mut bytes)
        .expect("binprot_write to a Vec is infallible");
    let len = ((bytes.len() - 8) as u64).to_le_bytes();
    bytes[..8].copy_from_slice(&len);
    bytes
}

/// Outcome of a [`broadcast_tx`] call.
pub struct BroadcastOutcome {
    /// Canonical tx hash(es) of the published command(s) — the Rosetta
    /// `transaction_identifier`s a caller would poll.
    pub tx_ids: Vec<String>,
    /// How many times a published tx was seen echoed back on tx-pool gossip during the
    /// linger window. Peers re-broadcasting it is evidence the network accepted it (a
    /// malformed/invalid tx is dropped, not propagated).
    pub echoes: usize,
}

/// Publish signed `cmds` to the tx-pool gossip topic and watch for propagation.
///
/// Joins the gossip mesh, publishes once a mesh/fanout peer is available (retrying
/// while the mesh forms), then lingers — counting how often the same tx echoes back
/// from other peers. `deadline` bounds the whole call.
pub async fn broadcast_tx(
    chain_id: &str,
    peers: &[&str],
    cmds: Vec<MinaBaseUserCommandStableV2>,
    linger: Duration,
    deadline: Duration,
) -> Result<BroadcastOutcome, String> {
    let tx_ids: Vec<String> = cmds.iter().map(command_id).collect();
    let id_set: HashSet<String> = tx_ids.iter().cloned().collect();
    let payload = encode_tx_pool_diff(cmds, 1);

    let peer_addrs: Vec<Multiaddr> = peers
        .iter()
        .map(|s| s.parse().expect("multiaddr"))
        .collect();
    let local_key: libp2p::identity::Keypair = EdKeypair::from(SecretKey::generate()).into();
    let behaviour: gossipsub::Behaviour = {
        let cfg = gossipsub::ConfigBuilder::default()
            .max_transmit_size(1024 * 1024 * 32)
            .build()
            .expect("valid gossipsub config");
        gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            cfg,
        )
        .expect("gossipsub behaviour")
    };
    let pnet_input = format!("/coda/0.0.1/{chain_id}");
    let mut swarm = crate::transport::swarm(
        local_key,
        pnet_input.as_bytes(),
        Vec::<Multiaddr>::new(),
        peer_addrs.iter().cloned(),
        behaviour,
    );
    let topic = gossipsub::IdentTopic::new(CONSENSUS_TOPIC);
    swarm
        .behaviour_mut()
        .subscribe(&topic)
        .map_err(|e| format!("subscribe: {e:?}"))?;
    for peer in &peer_addrs {
        for proto in peer.iter() {
            if let libp2p::multiaddr::Protocol::P2p(peer_id) = proto {
                swarm.behaviour_mut().add_explicit_peer(&peer_id);
            }
        }
    }

    let run = async {
        let mut published = false;
        let mut echoes = 0usize;
        let mut publish_tick = tokio::time::interval(Duration::from_millis(500));
        let mut linger_deadline: Option<tokio::time::Instant> = None;

        loop {
            if let Some(ld) = linger_deadline {
                if tokio::time::Instant::now() >= ld {
                    break;
                }
            }
            tokio::select! {
                _ = publish_tick.tick() => {
                    if !published {
                        // Gossipsub publish fails with InsufficientPeers until the mesh
                        // (or fanout) forms; retry until it takes.
                        match swarm.behaviour_mut().publish(topic.clone(), payload.clone()) {
                            Ok(_) => {
                                published = true;
                                linger_deadline = Some(tokio::time::Instant::now() + linger);
                                log::info!("published tx-pool diff ({} cmd) to gossip", tx_ids.len());
                            }
                            Err(e) => log::debug!("publish not ready: {e:?}"),
                        }
                    }
                }
                ev = swarm.next() => match ev {
                    Some(SwarmEvent::Behaviour(gossipsub::Event::Message { message, .. })) => {
                        for cmd in tx_pool_diff_from_gossip(&message.data) {
                            if id_set.contains(&command_id(&cmd)) {
                                echoes += 1;
                            }
                        }
                    }
                    Some(SwarmEvent::ConnectionClosed { .. }) => {
                        for addr in &peer_addrs {
                            let _ = swarm.dial(addr.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        if !published {
            return Err("never reached a publishable peer before linger window".to_string());
        }
        Ok(BroadcastOutcome { tx_ids, echoes })
    };

    tokio::select! {
        r = run => r,
        _ = tokio::time::sleep(deadline) => Err("deadline reached before broadcast completed".into()),
    }
}
