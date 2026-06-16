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

- [x] Wire the **trust gate**: verify each gossiped block's proof before ingest
      (`verify_tip` + `ChainMonitor`); validated on live devnet (h528196).
- [x] **Account reads**: Merkle-proof balances/nonce against a verified ledger root.
      The relay walks the libp2p sync-ledger RPC (`fetch_sync_ledger_answers`, a dumb
      pipe); `mina-verify` builds the query plan + folds the account/path onto a proven
      root (`sync_ledger_queries` / `verify_account_at_root`). Validated on devnet
      (`cargo run --example account_read`): h528297's proof anchors account index 0's
      balance. NB peers serve only the **epoch** ledgers, not the staged tip root, so
      reads are against the proof-anchored epoch ledger (finalized balances).
- [x] **Mempool tap**: tx-pool gossip → bounded, TTL'd view (`mempool::MempoolView`),
      keyed by the canonical Mina tx hash (`MinaBaseUserCommandStableV2::hash()`, the
      Rosetta `transaction_identifier`); validated on devnet.
- [ ] **Broadcast**: publish signed txs to the tx-pool gossip topic.
- [x] **Liveness cross-check** (expose side): the node emits its best proof-verified tip
      as a structured stdout line + optional `LIGHT_NODE_TIP_FILE`; validated on devnet
      (h528200). Consumers (e.g. the indexer) compare it vs a GCS tip to flag divergence.
- [ ] HTTP/RPC surface + deploy glue (see `deploy/`).
