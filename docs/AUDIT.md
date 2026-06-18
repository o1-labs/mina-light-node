# mina-light-node — security & test/coverage audit

Audit date: 2026-06-18. Scope: the code this repo *owns* — `mina-relay`
(transport, gossip, RPC, mempool) and the `mina-light-node` binary. `mina-verify` is
audited in its own repo and treated here as a trusted dependency.

## Summary

The trust gate (verify-before-ingest) is already wired and the networking is sound.
This audit found one concrete remote-DoS vector in the RPC read path (fixed in this
PR), one latent failure-handling bug in the verify worker (flagged), and a complete
absence of tests (first tests added in this PR).

| Finding | Severity | Status |
|---|---|---|
| S1 RPC frame alloc unbounded from untrusted length prefix | high (remote DoS) | **FIXED here** |
| S2 verify-worker panic is silent — node runs but verifies nothing | medium | flagged (see below) |
| S3 `.parse().expect()` panics on bad peer multiaddr | low | open, mitigated by test |
| S4 `dial()/listen_on().unwrap()` panic on setup error | low | open |
| S5 re-dial-all seeds on every disconnect | low | open |
| S6 full-traffic hex logging at `debug` | informational | acceptable |
| Tests | — | **0 → 9 added here** |

## Findings

### S1 — Unbounded allocation from an untrusted length prefix. FIXED
`rpc.rs` read an 8-byte little-endian length prefix straight from a peer and did
`vec![0u8; len]`. A hostile peer could declare a multi-GiB (or `u64::MAX`) frame and
OOM/abort the process before any data arrived.

*Fix:* a `MAX_FRAME_LEN = 32 MiB` cap (matching the gossip `max_transmit_size`);
oversized prefixes return an error instead of allocating. Covered by
`oversized_frame_is_rejected_not_allocated` (a mock stream feeding `u64::MAX`).

### S2 — Verify-worker panic is silent. FLAGGED (not changed here)
In `main.rs`, the worker thread does `Verifier::for_network(&net).expect(...)`. If
the verifier can't be built (e.g. an unsupported network, or a VK load failure), the
worker **panics and dies**, the channel receiver drops, and the gossip loop's
`let _ = tx.send(...)` then **fails silently** — the process stays alive, connected
to peers, but verifies *nothing*. A "trustless node" that has quietly stopped
verifying is worse than one that crashes.
*Recommended fix (left for a focused follow-up to avoid touching the hot path here):*
propagate the verifier-build error to the main task and exit non-zero, or stop the
gossip loop when `tx.send` errors (return `ControlFlow::Break`).

### S3 — `.parse().expect(...)` on peer multiaddrs. OPEN (low; mitigated)
`subscribe_gossip` / `fetch_best_tip` panic on a malformed peer string. The shipped
seed constants are now pinned valid by `all_seed_peers_parse_as_multiaddrs`, so the
out-of-the-box path can't hit this; it remains a panic hazard for external callers
passing arbitrary peer lists. Consider a `Result`-returning variant if peer lists
become user-supplied.

### S4 — `dial()/listen_on().unwrap()` in transport. OPEN (low)
A dial/listen setup error panics at startup; triggered only by the constant seeds
today (actual connection failures surface as events, not setup errors).

### S5 — Re-dial-all on every connection close. OPEN (low)
On `ConnectionClosed` the loop re-dials *all* seeds; under churn this can produce
redundant dials. Harmless at the current seed count.

### S6 — Full-traffic hex logging at `debug`. OPEN (informational)
`transport.rs` hex-encodes every read/write at `log::debug!`. High volume/CPU at
`RUST_LOG=debug`; default `info` is unaffected; gossip is public (no secret leak).

### Non-findings (reviewed, acceptable)
- **Trust gate present**: `main.rs` runs verify-before-ingest (`verify_tip` +
  `ChainMonitor`); invalid proofs are rejected and never ingested. This is the core
  safety property and it is correctly in place.
- **Ephemeral identity each run** (`SecretKey::generate()`): fine for a light node.
- **Gossip `MessageAuthenticity::Signed`** authenticates the forwarder, not block
  validity — which is exactly why S-side proof verification matters (and is present).
- **mpsc channel growth**: block production (~1 / 180 s) ≫ verify time, and only
  `NewState` payloads pass the prefilter.

## Test & coverage audit

Previously: **zero tests anywhere**. Added in this PR:

**Unit (`cargo test`, no network):**
- `network_seeds_known_networks` / `_unknown_is_none` — network→(chain_id, peers) map.
- `all_seed_peers_parse_as_multiaddrs` — pins every shipped seed as a valid multiaddr
  (guards S3 for the default path).
- `new_state_prefilter` — the offset-8 block tag prefilter (extracted as the testable
  `is_new_state_payload` helper).
- `frame_length_prefix_matches_body`, `handshake_shape`, `query_frames_with_valid_prefix`
  — RPC wire framing / handshake / query encoding.
- `oversized_frame_is_rejected_not_allocated` — the S1 DoS guard.

**Integration (gated, real devnet):**
- `tests/devnet_e2e.rs::devnet_tip_verifies_end_to_end` — joins live devnet gossip,
  captures a block, asserts its proof verifies. `#[ignore]`d; run with
  `cargo test -p mina-light-node --test devnet_e2e -- --ignored`.

### Coverage recommendations (not yet done)
- A `ChainMonitor` reorg/fork unit test belongs in `mina-verify` (it owns the type).
- A `mempool` view test (dedup / TTL eviction) in `mina-relay`.
- **CI added** (`.github/workflows/ci.yml`): `cargo fmt --check`, `clippy -D warnings`,
  `cargo test`, `cargo tarpaulin` coverage (reported, not yet gated), and the
  `--ignored` live-devnet e2e on a daily schedule.

## Deploy note
`LIGHT_NODE_SECS` is now optional (unset = run forever) so the deployed node no longer
self-terminates at 600 s — the out-of-the-box devnet path connects to seeds and
verifies indefinitely.
