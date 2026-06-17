//! Mina libp2p RPC (`coda/rpcs/0.0.1`) — the deterministic request/response path
//! (no gossip-mesh wait). This module implements the **wire protocol**:
//! frame/handshake/query and reading the `get_best_tip` response into a block, over
//! any already-open RPC stream ([`rpc_best_tip`]).
//!
//! Wire protocol (reverse-engineered from openmina's rpc_kernel):
//!   message = [8-byte LE length][binprot MessageHeader][payload]
//!   on open: Handshake = a Response with id = b"RPC\0\0\0\0\0", payload 0x01
//!   query:   MessageHeader::Query(QueryHeader{tag:"get_best_tip", version:2, id}) + NeedsLength(())
//!   reply:   MessageHeader::Response{id} + RpcResult<NeedsLength<GetBestTipV2Response>, Error>
//!
//! Transport: [`crate::rpc_net`] opens the `coda/rpcs/0.0.1` substream (a custom libp2p
//! `ConnectionHandler` over the fork's libp2p) and hands the stream to [`rpc_best_tip`].
//! (crates.io `libp2p-stream` can't be used — the fork is a monorepo and its swarm
//! version can't be `[patch]`-unified with the external crate.)

use binprot::{BinProtRead, BinProtWrite};
use libp2p::futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use mina_p2p_messages::rpc::GetBestTipV2;
use mina_p2p_messages::rpc_kernel::{
    MessageHeader, NeedsLength, QueryHeader, QueryPayload, ResponseHeader, ResponsePayload,
    RpcMethod,
};
use mina_p2p_messages::v2::MinaBlockBlockStableV2;

const HANDSHAKE_ID: u64 = u64::from_le_bytes(*b"RPC\x00\x00\x00\x00\x00");
const QUERY_ID: u64 = 1;

/// Upper bound on a single RPC response frame. The length prefix comes from an
/// untrusted peer; without a cap, `vec![0u8; len]` on a hostile value is an instant
/// OOM/abort. 32 MiB matches the gossip `max_transmit_size` — comfortably above any
/// real Mina message, well below a memory-exhaustion threat.
const MAX_FRAME_LEN: usize = 32 * 1024 * 1024;

/// `coda/rpcs/0.0.1` (no leading `/` — the Mina convention the fork's libp2p allows).
pub const RPC_PROTOCOL: &str = "coda/rpcs/0.0.1";

/// `[8-byte LE length][header][payload]` framing.
fn frame(header: &MessageHeader, payload: &[u8]) -> Vec<u8> {
    let mut v = vec![0u8; 8];
    header.binprot_write(&mut v).expect("write header");
    v.extend_from_slice(payload);
    let len = ((v.len() - 8) as u64).to_le_bytes();
    v[..8].copy_from_slice(&len);
    v
}

/// The RPC handshake sent on stream open.
pub fn handshake_bytes() -> Vec<u8> {
    frame(
        &MessageHeader::Response(ResponseHeader { id: HANDSHAKE_ID }),
        b"\x01",
    )
}

/// A framed query for any RPC method.
fn query_bytes<M: RpcMethod>(query: &M::Query, id: u64) -> Vec<u8>
where
    M::Query: BinProtWrite + Clone,
{
    let mut payload = Vec::new();
    QueryPayload::<M::Query>::binprot_write(&NeedsLength(query.clone()), &mut payload)
        .expect("write query payload");
    let header = MessageHeader::Query(QueryHeader {
        tag: M::NAME.into(),
        version: M::VERSION,
        id,
    });
    frame(&header, &payload)
}

/// A persistent RPC connection over an open `coda/rpcs/0.0.1` stream: handshake once,
/// then issue any number of typed queries (each gets a fresh id). Reuse one connection
/// for multi-step protocols (e.g. walking the ledger) instead of reconnecting per query.
pub struct RpcConn<S> {
    stream: S,
    next_id: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> RpcConn<S> {
    /// Send the handshake and return a ready connection.
    pub async fn open(mut stream: S) -> Result<Self, String> {
        stream
            .write_all(&handshake_bytes())
            .await
            .map_err(|e| e.to_string())?;
        stream.flush().await.map_err(|e| e.to_string())?;
        Ok(Self {
            stream,
            next_id: QUERY_ID,
        })
    }

    /// Issue one query and return the method's raw response (method-specific unwrapping
    /// — Option, RpcResult, … — is left to the caller).
    pub async fn call<M: RpcMethod>(&mut self, query: &M::Query) -> Result<M::Response, String>
    where
        M::Query: BinProtWrite + Clone,
        M::Response: BinProtRead,
    {
        let id = self.next_id;
        self.next_id += 1;
        let bytes = query_bytes::<M>(query, id);
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|e| e.to_string())?;
        self.stream.flush().await.map_err(|e| e.to_string())?;

        loop {
            let mut len_buf = [0u8; 8];
            self.stream
                .read_exact(&mut len_buf)
                .await
                .map_err(|e| e.to_string())?;
            let len = u64::from_le_bytes(len_buf) as usize;
            if len > MAX_FRAME_LEN {
                return Err(format!(
                    "rpc frame length {len} exceeds cap {MAX_FRAME_LEN} (untrusted peer)"
                ));
            }
            let mut buf = vec![0u8; len];
            self.stream
                .read_exact(&mut buf)
                .await
                .map_err(|e| e.to_string())?;

            let mut cursor = &buf[..];
            let header = MessageHeader::binprot_read(&mut cursor).map_err(|e| e.to_string())?;
            match header {
                MessageHeader::Response(ResponseHeader { id: rid }) if rid == id => {
                    let payload: ResponsePayload<M::Response> =
                        BinProtRead::binprot_read(&mut cursor).map_err(|e| e.to_string())?;
                    return payload
                        .0
                        .map_err(|_| "rpc kernel error response".to_string())
                        .map(|nl| nl.0); // NeedsLength -> inner
                }
                // peer handshake, heartbeats, or responses to other ids — keep reading.
                _ => continue,
            }
        }
    }
}

/// Convenience: `get_best_tip` over an open stream → the tip block.
pub async fn rpc_best_tip<S>(stream: S) -> Result<MinaBlockBlockStableV2, String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = RpcConn::open(stream).await?;
    let resp = conn.call::<GetBestTipV2>(&()).await?;
    let tip = resp.ok_or_else(|| "peer has no best tip".to_string())?;
    Ok(tip.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// The 8-byte LE length prefix must equal the framed body length.
    #[test]
    fn frame_length_prefix_matches_body() {
        let bytes = handshake_bytes();
        assert!(bytes.len() >= 8);
        let declared = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        assert_eq!(declared, bytes.len() - 8, "prefix counts only the body");
    }

    /// The handshake is a Response with the magic `RPC\0\0\0\0\0` id and a 0x01 body.
    #[test]
    fn handshake_shape() {
        let bytes = handshake_bytes();
        let body = &bytes[8..];
        let mut cursor = body;
        let header = MessageHeader::binprot_read(&mut cursor).expect("decode header");
        match header {
            MessageHeader::Response(ResponseHeader { id }) => assert_eq!(id, HANDSHAKE_ID),
            other => panic!("handshake is not a Response: {other:?}"),
        }
        assert_eq!(cursor, b"\x01", "handshake payload byte is 0x01");
    }

    /// A query for a concrete method frames with a correct length prefix and tag.
    #[test]
    fn query_frames_with_valid_prefix() {
        let bytes = query_bytes::<GetBestTipV2>(&(), QUERY_ID);
        let declared = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        assert_eq!(declared, bytes.len() - 8);
        let mut cursor = &bytes[8..];
        match MessageHeader::binprot_read(&mut cursor).expect("decode header") {
            MessageHeader::Query(q) => {
                assert_eq!(q.version, GetBestTipV2::VERSION);
                assert_eq!(q.id, QUERY_ID);
            }
            other => panic!("not a Query: {other:?}"),
        }
    }

    /// A stream that swallows all writes and, on read, always yields an 8-byte LE
    /// length prefix declaring a frame far larger than [`MAX_FRAME_LEN`].
    struct HugeLenStream {
        prefix: [u8; 8],
        pos: usize,
    }
    impl HugeLenStream {
        fn new() -> Self {
            Self {
                prefix: (u64::MAX).to_le_bytes(),
                pos: 0,
            }
        }
    }
    impl AsyncWrite for HugeLenStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(b.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    impl AsyncRead for HugeLenStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let n = buf.len().min(self.prefix.len() - self.pos);
            buf[..n].copy_from_slice(&self.prefix[self.pos..self.pos + n]);
            self.pos += n;
            Poll::Ready(Ok(n))
        }
    }

    /// A hostile length prefix must be rejected before allocation, not OOM the node.
    #[tokio::test]
    async fn oversized_frame_is_rejected_not_allocated() {
        let mut conn = RpcConn::open(HugeLenStream::new())
            .await
            .expect("handshake");
        let err = conn.call::<GetBestTipV2>(&()).await.unwrap_err();
        assert!(err.contains("exceeds cap"), "unexpected error: {err}");
    }
}
