//! libp2p transport for the RPC client: a minimal `NetworkBehaviour` +
//! `ConnectionHandler` that opens one outbound `coda/rpcs/0.0.1` substream per
//! connection and hands the raw stream up. `fetch_best_tip` dials a peer over the
//! Mina transport, gets the stream, and runs [`crate::rpc::rpc_best_tip`] on it.

use std::collections::VecDeque;
use std::task::{Context, Poll};
use std::time::Duration;

use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::core::Endpoint;
use libp2p::futures::StreamExt;
use libp2p::swarm::handler::{ConnectionEvent, FullyNegotiatedOutbound};
use libp2p::swarm::{
    ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId, FromSwarm,
    KeepAlive, NetworkBehaviour, PollParameters, Stream, StreamProtocol, SubstreamProtocol,
    SwarmEvent, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use void::Void;

use crate::transport::ed25519::{Keypair as EdKeypair, SecretKey};
use mina_p2p_messages::v2::MinaBlockBlockStableV2;

const RPC_PROTOCOL: StreamProtocol = StreamProtocol::new("coda/rpcs/0.0.1");

/// Opens one outbound RPC substream and delivers the negotiated stream up.
#[derive(Default)]
struct RpcHandler {
    requested: bool,
    ready: VecDeque<Stream>,
}

impl ConnectionHandler for RpcHandler {
    type FromBehaviour = Void;
    type ToBehaviour = Stream;
    type Error = Void;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<ReadyUpgrade<StreamProtocol>, ()> {
        SubstreamProtocol::new(ReadyUpgrade::new(RPC_PROTOCOL), ())
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        KeepAlive::Yes
    }

    fn on_behaviour_event(&mut self, _: Void) {}

    fn poll(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<ReadyUpgrade<StreamProtocol>, (), Stream, Void>> {
        if !self.requested {
            self.requested = true;
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(ReadyUpgrade::new(RPC_PROTOCOL), ()),
            });
        }
        if let Some(stream) = self.ready.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(stream));
        }
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        if let ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
            protocol: stream,
            ..
        }) = event
        {
            self.ready.push_back(stream);
        }
    }
}

/// Surfaces each opened RPC stream as a behaviour event.
#[derive(Default)]
struct RpcBehaviour {
    streams: VecDeque<Stream>,
}

impl NetworkBehaviour for RpcBehaviour {
    type ConnectionHandler = RpcHandler;
    type ToSwarm = Stream;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RpcHandler::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RpcHandler::default())
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        stream: THandlerOutEvent<Self>,
    ) {
        self.streams.push_back(stream);
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(s) = self.streams.pop_front() {
            Poll::Ready(ToSwarm::GenerateEvent(s))
        } else {
            Poll::Pending
        }
    }

    fn on_swarm_event(&mut self, _: FromSwarm<Self::ConnectionHandler>) {}
}

/// Connect to a peer over Mina's transport, RPC `get_best_tip`, return the tip block.
pub async fn fetch_best_tip(
    chain_id: &str,
    peers: &[&str],
    deadline: Duration,
) -> Result<MinaBlockBlockStableV2, String> {
    let addrs: Vec<Multiaddr> = peers
        .iter()
        .map(|s| s.parse().expect("multiaddr"))
        .collect();
    let local_key: libp2p::identity::Keypair = EdKeypair::from(SecretKey::generate()).into();
    let pnet_input = format!("/coda/0.0.1/{chain_id}");

    let mut swarm = crate::transport::swarm(
        local_key,
        pnet_input.as_bytes(),
        Vec::<Multiaddr>::new(),
        addrs.iter().cloned(),
        RpcBehaviour::default(),
    );

    let run = async {
        // Drive the swarm until a peer opens the RPC stream.
        let stream = loop {
            if let Some(SwarmEvent::Behaviour(stream)) = swarm.next().await {
                break stream;
            }
        };
        // Run the RPC while keeping the swarm polled (drives the muxer).
        let mut rpc = std::pin::pin!(crate::rpc::rpc_best_tip(stream));
        loop {
            tokio::select! {
                _ = swarm.next() => {}
                r = &mut rpc => return r,
            }
        }
    };

    tokio::select! {
        r = run => r,
        _ = tokio::time::sleep(deadline) => Err("deadline reached before fetching best tip".into()),
    }
}
