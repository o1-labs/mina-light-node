# mina-light-node

A **trustless Mina light node** — it *joins* the p2p network and *verifies* the
chain from its recursive SNARK proof. Unlike a typical light *client* (an RPC
consumer), this is a real light *node*: it participates in the network.

```
   Mina p2p network ──▶ mina-relay ──(candidate tip)──▶ mina-verify ──▶ verified tip
   (untrusted)          (this repo)                     (separate repo)   + Merkle-proof reads
```

- **Safety** ("never accept a false state") comes from `mina-verify` — one recursive
  proof validates the whole chain (stronger than Bitcoin SPV: no header download).
- **Liveness / censorship-resistance** ("you see the *real, current* chain") comes
  from `mina-relay` — decentralized p2p access across many peers, not a trusted RPC.

## Crates

| Crate | What |
|---|---|
| [`mina-relay`](crates/mina-relay) | The p2p network layer: connect to gossip, receive blocks, tap the mempool, broadcast txs, track peers. **Proof-systems-agnostic** (libp2p + message types only; no verification). Reusable on its own. |
| [`mina-light-node`](crates/mina-light-node) | The product binary: wires `mina-relay` + `mina-verify` into one runnable trustless light node. |

The verifier lives in its own repo, [`MinaProtocol/mina-verify`](https://github.com/MinaProtocol/mina-verify),
because it's consumed independently (MCP server via wasm, mobile app, the trustless
indexer's trust gate). This repo composes it; it does not own it.

## Status

Scaffold. `mina-relay` carries the working gossip/RPC/broadcast primitives (moved
from `mina-verify-capture`). The `mina-light-node` binary currently follows the
gossip network and surfaces candidate tips; the verifier trust-gate, account reads,
mempool tap, broadcast API, and liveness cross-check are TODOs (see `src/main.rs`).

## Build / run

```sh
cargo build
MINA_NETWORK=devnet cargo run -p mina-light-node
```

## Roadmap (see the trustless-light-stack arch doc)

- [ ] Wire the **trust gate**: feed candidate tips to `mina-verify`; verify the tip
      proof (trust the linked prefix); expose the verified tip.
- [ ] **Account reads**: Merkle-proof balances/nonce against the verified ledger root.
- [ ] **Mempool tap**: tx-pool gossip → bounded, TTL'd view (decode + sig check + dedup).
- [ ] **Broadcast**: publish signed txs to the tx-pool gossip topic.
- [ ] **Liveness cross-check**: expose the live p2p tip for consumers (e.g. the indexer)
      to compare against a GCS-sourced tip and flag divergence.
- [ ] HTTP/RPC surface + deploy glue (see `deploy/`).
