//! Tier 2 (#21): a **submitted transaction relays through a hub into a pinned node's
//! mempool** — the end-to-end mempool-provider loop at the relay layer.
//!
//! ```text
//!   broadcast_tx ──▶ hub (relays) ──▶ spoke (static-peer, taps tx-pool → MempoolView)
//! ```
//!
//! We publish the tx *through the hub* rather than the lightnet daemon on purpose: the
//! daemon would reject the fixture's (foreign-network) signature and never re-gossip it,
//! whereas a light relay forwards gossip without checking signatures. This exercises
//! `broadcast_tx` → gossip propagation → `MempoolView::ingest_gossip` → decode — exactly
//! the path a wallet/exchange spoke relies on. Signature validity is irrelevant here;
//! only the wire shape and the tap/decode are under test.
//!
//! Ignored by default; bring up a proof-none lightnet (see `gossip_relay.rs`) and:
//! ```sh
//! export MINA_TEST_CHAIN_ID=<chainId>  MINA_TEST_SEED_ADDR=/ip4/.../tcp/.../p2p/...
//! cargo test -p mina-relay --test submit_mempool -- --ignored --nocapture
//! ```

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mina_p2p_messages::binprot::BinProtRead;
use mina_p2p_messages::v2::MinaBaseUserCommandStableV2;
use mina_relay::broadcast::broadcast_tx;
use mina_relay::mempool::{command_id, MempoolView};
use mina_relay::{subscribe_gossip, Multiaddr};
use tokio::sync::mpsc::unbounded_channel;

/// Distinct from `gossip_relay`'s port so the two `#[ignore]` tests can run together.
const HUB_PORT: u16 = 17313;

async fn wait_atomic(val: &AtomicU64, within: Duration, pred: impl Fn(u64) -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if pred(val.load(Ordering::Relaxed)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    pred(val.load(Ordering::Relaxed))
}

async fn wait_for(within: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    pred()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a running local mina network; set MINA_TEST_CHAIN_ID + MINA_TEST_SEED_ADDR, run with --ignored"]
async fn submitted_tx_relays_into_a_pinned_nodes_mempool() {
    let chain_id = std::env::var("MINA_TEST_CHAIN_ID").expect("set MINA_TEST_CHAIN_ID");
    let seed_addr = std::env::var("MINA_TEST_SEED_ADDR").expect("set MINA_TEST_SEED_ADDR");
    let hub_listen: Multiaddr = format!("/ip4/127.0.0.1/tcp/{HUB_PORT}").parse().unwrap();
    let hub_addr = format!("/ip4/127.0.0.1/tcp/{HUB_PORT}");

    // The tx to submit: a real captured payment (shared fixture with the mina-light-node
    // decoder test). Decodes to a well-formed command; that's all this path needs.
    let hex = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../mina-light-node/tests/fixtures/devnet_payment.hex"
    ))
    .trim();
    let bytes = hex::decode(hex).expect("fixture is valid hex");
    let cmd = MinaBaseUserCommandStableV2::binprot_read(&mut &bytes[..])
        .expect("fixture decodes as a user command");
    let want_id = command_id(&cmd);

    // Hub: joins the lightnet (dials the seed) and listens, so it relays gossip between
    // the daemon, the broadcaster, and the spoke.
    let hub_peers = Arc::new(AtomicU64::new(0));
    let hub = {
        let (chain_id, seed, peers) = (chain_id.clone(), seed_addr.clone(), hub_peers.clone());
        let (_bt, br) = unbounded_channel();
        tokio::spawn(async move {
            subscribe_gossip(
                &chain_id,
                &[&seed],
                Some(hub_listen),
                true,
                None,
                |_src, _payload| ControlFlow::Continue(()),
                move |n| {
                    peers.store(n as u64, Ordering::Relaxed);
                    ControlFlow::Continue(())
                },
                br,
            )
            .await;
        })
    };
    assert!(
        wait_atomic(&hub_peers, Duration::from_secs(60), |n| n >= 1).await,
        "hub never connected to the lightnet seed"
    );

    // Spoke: pinned to the hub only (static-peer mode), taps tx-pool into a MempoolView.
    let mempool = Arc::new(Mutex::new(MempoolView::new(1024, Duration::from_secs(600))));
    let spoke_peers = Arc::new(AtomicU64::new(0));
    let spoke = {
        let (chain_id, hub_addr2, peers, mp) = (
            chain_id.clone(),
            hub_addr.clone(),
            spoke_peers.clone(),
            mempool.clone(),
        );
        let (_bt, br) = unbounded_channel();
        tokio::spawn(async move {
            subscribe_gossip(
                &chain_id,
                &[&hub_addr2],
                None,
                false,
                None,
                move |_src, payload| {
                    mp.lock().unwrap().ingest_gossip(payload);
                    ControlFlow::Continue(())
                },
                move |n| {
                    peers.store(n as u64, Ordering::Relaxed);
                    ControlFlow::Continue(())
                },
                br,
            )
            .await;
        })
    };
    assert!(
        wait_atomic(&spoke_peers, Duration::from_secs(60), |n| n >= 1).await,
        "spoke never connected to the hub"
    );

    // Let the gossipsub meshes settle before publishing.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Submit: broadcast the tx through the hub (not the daemon — it would drop the sig).
    let outcome = broadcast_tx(
        &chain_id,
        &[&hub_addr],
        vec![cmd],
        Duration::from_secs(20),
        Duration::from_secs(30),
    )
    .await
    .expect("broadcast_tx");
    assert!(
        outcome.tx_ids.contains(&want_id),
        "broadcast reported a different tx id"
    );

    // The spoke should tap the relayed tx into its mempool, keyed by canonical hash.
    let arrived = wait_for(Duration::from_secs(60), || {
        mempool.lock().unwrap().contains(&want_id)
    })
    .await;
    assert!(
        arrived,
        "submitted tx never reached the pinned spoke's mempool"
    );

    // And it round-trips back to the same signed command.
    {
        let view = mempool.lock().unwrap();
        let pending = view.iter().find(|p| p.id == want_id).expect("tx present");
        assert!(matches!(
            &pending.command,
            MinaBaseUserCommandStableV2::SignedCommand(_)
        ));
    }

    hub.abort();
    spoke.abort();
}
