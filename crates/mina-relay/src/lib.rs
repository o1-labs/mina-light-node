//! Live block source: connect to a Mina network over its libp2p gossip and hand
//! each block (NewState) message to a callback. Used by the `mina-relay`
//! binary (save to disk) and by `mina-verify-monitor` (verify-before-ingest).

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

/// Connect to a Mina network over gossip and invoke `on_block` with the raw gossip
/// payload (`[8-byte len][GossipNetMessageV2 binprot]`) of each `NewState` (block).
///
/// Runs until `on_block` returns [`ControlFlow::Break`] or `deadline` elapses. The
/// payload is in the exact form [`mina_verify::block_from_gossip_payload`] expects.
pub async fn subscribe_blocks<F, T>(
    chain_id: &str,
    peers: &[&str],
    deadline: Option<Duration>,
    mut on_block: F,
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
                    // tag at offset 8: 0 = NewState (block).
                    if message.data.get(8) == Some(&0) {
                        if let ControlFlow::Break(()) = on_block(&message.data) {
                            break;
                        }
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
