//! Live block source: connect to a Mina network over its libp2p gossip and hand
//! each block (NewState) message to a callback. Used by the `mina-relay`
//! binary (save to disk) and by `mina-verify-monitor` (verify-before-ingest).

pub mod broadcast;
pub mod mempool;
pub mod rpc;
pub mod rpc_net;
mod transport;

use std::ops::ControlFlow;
use std::time::Duration;

use libp2p::{futures::StreamExt, gossipsub, swarm::SwarmEvent, Multiaddr};
use transport::ed25519::{Keypair as EdKeypair, SecretKey};

/// Live devnet chain id (matches `daemonStatus.chainId`).
pub const DEVNET_CHAIN_ID: &str =
    "29936104443aaf264a7f0192ac64b1c7173198c1ed404c1bcff5e562e05eb7f6";

/// Devnet seed peers.
pub const DEVNET_PEERS: &[&str] = &[
    "/dns4/seed-1.devnet.gcp.o1test.net/tcp/10003/p2p/12D3KooWAdgYL6hv18M3iDBdaK1dRygPivSfAfBNDzie6YqydVbs",
    "/dns4/seed-2.devnet.gcp.o1test.net/tcp/10003/p2p/12D3KooWLjs54xHzVmMmGYb7W5RVibqbwD1co7M2ZMfPgPm7iAag",
    "/dns4/seed-3.devnet.gcp.o1test.net/tcp/10003/p2p/12D3KooWEiGVAFC7curXWXiGZyMWnZK9h8BKr88U8D5PKV3dXciv",
    "/ip4/65.108.123.234/tcp/8302/p2p/12D3KooWSfbf7roLyxHVQrCX5XcjAk5f8HeHPNBwsUDCojzcox81",
    "/ip4/65.109.147.145/tcp/8302/p2p/12D3KooWKuCvno5wq5mKorTgjjiubuCUTRTanbc3eBVejGiUw3st",
    "/ip4/65.109.29.163/tcp/8302/p2p/12D3KooWSwetAL9qf3vGYHPa8zPV984YurcMA88815DwDdXaBgZt",
    "/ip4/65.108.37.166/tcp/32107/p2p/12D3KooWBzwA4uRgwbmjX7LAsp9nMacW8xPqJejbmw1W96nXX1Ra",
    "/ip4/135.181.19.59/tcp/32009/p2p/12D3KooWPtxgAhUNd5MD4q6625tA1BGZ21eSNhXtFkFHscHKLdXV",
    "/ip4/37.27.118.240/tcp/32010/p2p/12D3KooWSFgucvvf5fwwHD4ziApbtqZGWtbb5B9btmvsjghQt9Jk",
    "/ip4/103.50.33.192/tcp/8302/p2p/12D3KooWK8CL1rzFQVqTPPU28oRb2aWGfp3gUdLxht1um79Sdssn",
    "/ip4/135.181.117.217/tcp/8302/p2p/12D3KooWL1kJuFjZT3HpRvUtF5AACgsC76YA7GxiW636cY5o7UBk",
];

/// Live mainnet chain id.
pub const MAINNET_CHAIN_ID: &str =
    "a7351abc7ddf2ea92d1b38cc8e636c271c1dfd2c081c637f62ebc2af34eb7cc1";

/// Mainnet seed peers.
pub const MAINNET_PEERS: &[&str] = &[
    "/dns4/seed-1.mainnet.gcp.o1test.net/tcp/10003/p2p/12D3KooWCa1d7G3SkRxy846qTvdAFX69NnoYZ32orWVLqJcDVGHW",
    "/dns4/seed-2.mainnet.gcp.o1test.net/tcp/32002/p2p/12D3KooWK4NfthViCTyLgVQa1WvqDC1NccVxGruCXCZUt3GqvFvn",
    "/dns4/seed-3.mainnet.gcp.o1test.net/tcp/32003/p2p/12D3KooWNofeYVAJXA3WGg2qCDhs3GEe71kTmKpFQXRbZmCz1Vr7",
    "/dns4/seed-4.mainnet.gcp.o1test.net/tcp/10003/p2p/12D3KooWEdBiTUQqxp3jeuWaZkwiSNcFxC6d6Tdq7u2Lf2ZD2Q6X",
    "/dns4/seed-5.mainnet.gcp.o1test.net/tcp/32005/p2p/12D3KooWL1DJTigSwuKQRfQE3p7puFUqfbHjXbZJ9YBWtMNpr3GU",
    "/dns4/seed-6.mainnet.gcp.o1test.net/tcp/32006/p2p/12D3KooWHGx4u32n42ub7dJNxAcAhwiA1WDq1Zsjn3k7RsS11pE8",
    "/dns4/mina-mainnet-seed.staketab.com/tcp/10003/p2p/12D3KooWSDTiXcdBVpN12ZqXJ49qCFp8zB1NnovuhZu6A28GLF1J",
    "/dns4/production-mainnet-libp2p.minaprotocol.network/tcp/10000/p2p/12D3KooWPywsM191KGGNVGiNqN35nyyJg4W2BhhYukF6hP9YBR8q",
    "/dns4/seed.minaexplorer.com/tcp/8302/p2p/12D3KooWR7coZtrMHvsgsfiWq2GESYypac3i29LFGp6EpbtjxBiJ",
];

/// Live mesa-mut (hardfork upgrade) chain id. testnet-signed; pre-fork it tracks
/// mainnet. Peers are dynamic — these are a live snapshot and may rotate.
pub const MESA_MUT_CHAIN_ID: &str =
    "8b8ccbf273ef48aa0193ed634e69540657f0fc4292c9919a54b76a21b104abb2";

/// mesa-mut peers (live snapshot; no stable published seeds for this network).
pub const MESA_MUT_PEERS: &[&str] = &[
    "/ip4/57.129.147.16/tcp/8302/p2p/12D3KooWAhy6QE9Re1dJwQiU6QMooVA9hTPN2HxJsnSjBQALANFv",
    "/ip4/194.87.21.152/tcp/8302/p2p/12D3KooWKPYJknMVauxpvWpwt4E1SUXr9fsjAN8YaYHaCv1FTFjg",
    "/ip4/37.27.109.20/tcp/8302/p2p/12D3KooWBZrHAfHDvtWUqW2ngSsQfHtyHqzQzxyQEvsjZygdCV9N",
    "/ip4/65.21.197.119/tcp/8302/p2p/12D3KooWF2sNkn1urFsQvB9GMSQCpkMMzuYvhqcWknWK6fUdYWE7",
    "/ip4/65.109.53.139/tcp/8302/p2p/12D3KooWGUjdTjajTMMzz8tJfCWTLcayDaixzWYEHLaB8gQwZrsy",
];

/// The consensus-messages gossip topic (blocks + pool diffs).
pub const CONSENSUS_TOPIC: &str = "coda/consensus-messages/0.0.1";

/// `(chain_id, seed peers)` for a network name ("devnet" / "mainnet" / "mesa-mut").
pub fn network_seeds(network: &str) -> Option<(&'static str, &'static [&'static str])> {
    match network {
        "devnet" => Some((DEVNET_CHAIN_ID, DEVNET_PEERS)),
        "mainnet" => Some((MAINNET_CHAIN_ID, MAINNET_PEERS)),
        "mesa-mut" => Some((MESA_MUT_CHAIN_ID, MESA_MUT_PEERS)),
        _ => None,
    }
}

/// Whether a raw consensus-gossip payload is a `NewState` (block) message.
///
/// The on-wire form is `[8-byte LE length][GossipNetMessageV2 binprot]`; the binprot
/// enum tag sits at offset 8 and `0` is the `NewState` variant (1 = snark-pool diff,
/// 2 = tx-pool diff). A full decode happens later in the verifier — this is the cheap
/// prefilter [`subscribe_blocks`] uses to skip non-block traffic.
pub fn is_new_state_payload(data: &[u8]) -> bool {
    data.get(8) == Some(&0)
}

/// Connect to a Mina network over gossip and invoke `on_block` with the raw gossip
/// payload of each `NewState` (block). Thin wrapper over [`subscribe_gossip`] that
/// filters to the block tag (`0`); the payload is in the exact form
/// [`mina_verify::block_from_gossip_payload`] expects.
pub async fn subscribe_blocks<F, T>(
    chain_id: &str,
    peers: &[&str],
    deadline: Option<Duration>,
    mut on_block: F,
    on_tick: T,
) where
    F: FnMut(&[u8]) -> ControlFlow<()>,
    T: FnMut(usize) -> ControlFlow<()>,
{
    subscribe_gossip(
        chain_id,
        peers,
        deadline,
        |data| {
            if is_new_state_payload(data) {
                on_block(data)
            } else {
                ControlFlow::Continue(())
            }
        },
        on_tick,
    )
    .await
}

/// Connect to a Mina network over gossip and invoke `on_msg` with the raw payload
/// (`[8-byte len][GossipNetMessageV2 binprot]`) of **every** gossip message — blocks
/// (tag 0), snark-pool diffs (1), and transaction-pool diffs (2). Discriminate on
/// `data[8]`; decode with [`mina_p2p_messages::gossip::GossipNetMessageV2`] (e.g.
/// [`crate::mempool::tx_pool_diff_from_gossip`] for pending transactions).
///
/// Runs until `on_msg` returns [`ControlFlow::Break`] or `deadline` elapses.
pub async fn subscribe_gossip<F, T>(
    chain_id: &str,
    peers: &[&str],
    deadline: Option<Duration>,
    mut on_msg: F,
    mut on_tick: T,
) where
    F: FnMut(&[u8]) -> ControlFlow<()>,
    T: FnMut(usize) -> ControlFlow<()>,
{
    let peers: Vec<Multiaddr> = peers
        .iter()
        .map(|s| s.parse().expect("valid multiaddr"))
        .collect();

    let local_key: libp2p::identity::Keypair = EdKeypair::from(SecretKey::generate()).into();
    log::info!("local peer id: {}", local_key.public().to_peer_id());

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

    // pnet PSK = Blake2b256("/coda/0.0.1/" || chain_id); transport::swarm hashes the
    // bytes we pass with no prefix, so prepend it here.
    let pnet_input = format!("/coda/0.0.1/{chain_id}");
    let mut swarm = transport::swarm(
        local_key,
        pnet_input.as_bytes(),
        Vec::<Multiaddr>::new(),
        peers.iter().cloned(),
        behaviour,
    );

    let topic = gossipsub::IdentTopic::new(CONSENSUS_TOPIC);
    swarm.behaviour_mut().subscribe(&topic).unwrap();
    for peer in &peers {
        for proto in peer.iter() {
            if let libp2p::multiaddr::Protocol::P2p(peer_id) = proto {
                swarm.behaviour_mut().add_explicit_peer(&peer_id);
            }
        }
    }

    let sleep = async {
        match deadline {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(sleep);
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    let mut connected = std::collections::HashSet::new();

    loop {
        tokio::select! {
            _ = &mut sleep => { log::info!("deadline reached"); break; }
            _ = tick.tick() => {
                // periodic wake — emit a heartbeat (with the live peer count) / cancel while idle.
                if let ControlFlow::Break(()) = on_tick(connected.len()) { break; }
            }
            ev = swarm.next() => match ev {
                Some(SwarmEvent::Behaviour(gossipsub::Event::Message { message, .. })) => {
                    // Every gossip message (block / snark-pool / tx-pool diff); the
                    // caller discriminates on data[8] (0=block, 1=snark, 2=tx-pool).
                    if let ControlFlow::Break(()) = on_msg(&message.data) {
                        break;
                    }
                }
                Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                    connected.insert(peer_id);
                }
                Some(SwarmEvent::ConnectionClosed { peer_id, .. }) => {
                    connected.remove(&peer_id);
                    // Stay in the gossip mesh: re-dial the seeds when a link drops.
                    log::debug!("conn closed {peer_id}; re-dialing");
                    for addr in &peers {
                        let _ = swarm.dial(addr.clone());
                    }
                }
                Some(SwarmEvent::OutgoingConnectionError { peer_id, error, .. }) => {
                    log::debug!("dial error to {peer_id:?}: {error}");
                }
                Some(_) => {}
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::Multiaddr;

    #[test]
    fn network_seeds_known_networks() {
        for net in ["devnet", "mainnet", "mesa-mut"] {
            let (chain_id, peers) = network_seeds(net).expect("known network");
            assert!(!peers.is_empty(), "{net} has seed peers");
            // chain id is a 32-byte hash rendered as 64 lowercase hex chars.
            assert_eq!(chain_id.len(), 64, "{net} chain id is 64 hex chars");
            assert!(
                chain_id.bytes().all(|b| b.is_ascii_hexdigit()),
                "{net} chain id is hex",
            );
        }
    }

    #[test]
    fn network_seeds_unknown_is_none() {
        assert!(network_seeds("nope").is_none());
        assert!(network_seeds("").is_none());
    }

    #[test]
    fn all_seed_peers_parse_as_multiaddrs() {
        // subscribe_gossip/fetch_best_tip `.parse().expect(...)` these; a malformed
        // constant would panic only at runtime, so pin it down here.
        for net in ["devnet", "mainnet", "mesa-mut"] {
            let (_, peers) = network_seeds(net).unwrap();
            for p in peers {
                p.parse::<Multiaddr>()
                    .unwrap_or_else(|e| panic!("{net} peer {p:?} is a valid multiaddr: {e}"));
            }
        }
    }

    #[test]
    fn new_state_prefilter() {
        // tag byte 0 at offset 8 = NewState (block).
        assert!(is_new_state_payload(&[0, 0, 0, 0, 0, 0, 0, 0, 0]));
        // a non-zero tag (snark/tx-pool diff) is not a block.
        assert!(!is_new_state_payload(&[0, 0, 0, 0, 0, 0, 0, 0, 1]));
        assert!(!is_new_state_payload(&[0, 0, 0, 0, 0, 0, 0, 0, 2]));
        // too short to carry a tag.
        assert!(!is_new_state_payload(&[0, 0, 0, 0]));
        assert!(!is_new_state_payload(&[]));
    }
}
