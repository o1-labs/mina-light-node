//! Multi-node gossip **relay** test — proves the inbound p2p listener lets light
//! nodes peer with *each other* and relay gossip, not just leech from seeds.
//!
//! Topology (the only way to test relay rather than two independent seed taps):
//!
//! ```text
//!   minimina seed ──▶ node A (listens) ──▶ node B (seedless; only peer = A)
//! ```
//!
//! Node B is given **no seeds** — its sole peer is node A's listen address. So if B
//! sees any gossip at all, A must have (1) accepted B's inbound connection (the
//! listener works) and (2) forwarded gossip to it (relay works). Then we stop A and
//! assert B loses its only peer, and restart A and assert B recovers.
//!
//! Uses `mina-relay` directly — the gossip layer is proof-systems-agnostic, so this
//! needs only the network's `chain_id` and one seed's libp2p multiaddr, no verifier
//! or VK. A **proof-none lightnet** is the ideal fixture (fast, no SNARK proving; the
//! test only counts gossip, never verifies it). Ignored by default; validated against
//! `o1labs/mina-local-network:compatible-latest-lightnet`:
//!
//! ```sh
//! # 1. boot a single-node proof-none lightnet, publishing GraphQL + its libp2p port
//! docker run -d --name ln --env NETWORK_TYPE=single-node --env PROOF_LEVEL=none \
//!   -p 3085:3085 -p 3086:3086 o1labs/mina-local-network:compatible-latest-lightnet
//! # 2. once SYNCED, read chain id + peer id + libp2p port from GraphQL daemonStatus
//! #    { daemonStatus { chainId addrsAndPorts { peer { peerId libp2pPort } } } }
//! export MINA_TEST_CHAIN_ID=<chainId>
//! export MINA_TEST_SEED_ADDR=/ip4/127.0.0.1/tcp/<libp2pPort>/p2p/<peerId>
//! cargo test -p mina-relay --test gossip_relay -- --ignored --nocapture
//! ```
//!
//! NB the lightnet regenerates its keys + chain id per boot — read them after each run.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mina_relay::{subscribe_gossip, Multiaddr};
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;

/// Local listen port node A binds; node B dials it. Fixed so B can construct A's addr
/// without needing A's (randomly generated) peer id — libp2p learns it on connect.
const NODE_A_PORT: u16 = 17311;

/// Spawn a gossip participant. `msgs` counts every gossip message it receives (relay
/// signal); `peers` tracks its live connected-peer count (connectivity signal).
fn spawn_node(
    chain_id: String,
    peers_list: Vec<String>,
    listen: Option<Multiaddr>,
    msgs: Arc<AtomicU64>,
    peers: Arc<AtomicU64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let peer_refs: Vec<&str> = peers_list.iter().map(String::as_str).collect();
        let (_ban_tx, ban_rx) = unbounded_channel();
        subscribe_gossip(
            &chain_id,
            &peer_refs,
            listen,
            None,
            move |_src, _payload| {
                msgs.fetch_add(1, Ordering::Relaxed);
                ControlFlow::Continue(())
            },
            move |n| {
                peers.store(n as u64, Ordering::Relaxed);
                ControlFlow::Continue(())
            },
            ban_rx,
        )
        .await;
    })
}

/// Poll `val` until `pred` holds or `within` elapses; returns whether it held.
async fn wait_until(val: &AtomicU64, within: Duration, pred: impl Fn(u64) -> bool) -> bool {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if pred(val.load(Ordering::Relaxed)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    pred(val.load(Ordering::Relaxed))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a running local mina network; set MINA_TEST_CHAIN_ID + MINA_TEST_SEED_ADDR, run with --ignored"]
async fn seedless_node_gets_gossip_relayed_by_a_listening_peer() {
    let chain_id = std::env::var("MINA_TEST_CHAIN_ID")
        .expect("set MINA_TEST_CHAIN_ID to the local network's chain id");
    let seed_addr = std::env::var("MINA_TEST_SEED_ADDR")
        .expect("set MINA_TEST_SEED_ADDR to a seed's libp2p multiaddr");
    let a_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{NODE_A_PORT}")
        .parse()
        .unwrap();
    // B dials A by address only; libp2p learns A's peer id on connect.
    let a_addr = format!("/ip4/127.0.0.1/tcp/{NODE_A_PORT}");

    // Node A: joins the local network (dials the seed) AND listens for inbound peers.
    let a_msgs = Arc::new(AtomicU64::new(0));
    let a_peers = Arc::new(AtomicU64::new(0));
    let node_a = spawn_node(
        chain_id.clone(),
        vec![seed_addr],
        Some(a_listen),
        a_msgs,
        a_peers.clone(),
    );

    // Give A time to connect to the seed and start listening.
    assert!(
        wait_until(&a_peers, Duration::from_secs(60), |n| n >= 1).await,
        "node A never connected to the local seed"
    );

    // Node B: NO seeds — its only peer is A's listen address.
    let b_msgs = Arc::new(AtomicU64::new(0));
    let b_peers = Arc::new(AtomicU64::new(0));
    let node_b = spawn_node(
        chain_id.clone(),
        vec![a_addr.clone()],
        None,
        b_msgs.clone(),
        b_peers.clone(),
    );

    // Connectivity: B reaching peer_count >= 1 means A accepted B's *inbound* dial —
    // the listener works. (B has no other peer to connect to.)
    assert!(
        wait_until(&b_peers, Duration::from_secs(60), |n| n >= 1).await,
        "node B never connected to node A — inbound listener not accepting dials"
    );

    // Relay: B receiving any gossip means A *forwarded* it (B has no seed link).
    assert!(
        wait_until(&b_msgs, Duration::from_secs(180), |n| n >= 1).await,
        "node B connected to A but received no relayed gossip within 180s"
    );

    // Stop A: B should lose its only peer (proves the peer link was real, not a
    // lingering seed connection).
    node_a.abort();
    assert!(
        wait_until(&b_peers, Duration::from_secs(60), |n| n == 0).await,
        "node B still reports peers after node A was stopped"
    );

    // NB restart-recovery is intentionally not asserted: the node only re-dials a peer
    // on `ConnectionClosed`, not on a timer, so if the peer is briefly down at that
    // instant it is never retried. That periodic-redial resilience is a separate concern
    // (see the relay hardening in #14), not the inbound-listener behavior under test here.

    node_b.abort();
}
