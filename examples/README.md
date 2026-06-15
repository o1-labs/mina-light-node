# Reference sources (not yet wired)

Moved from `mina-verify`'s `mina-verify-monitor` during the relay extraction. These
are the **verify-before-ingest** integration to fold into `crates/mina-light-node`
once the `mina-verify` dep is wired (s/mina_verify_capture/mina_relay/):

  (on a worker thread) BEFORE ingest; `ChainMonitor` tracks fork-choice/reorg.
- `mesa_capture_verify.rs`, `dump_mesa_block.rs`, `mesa_rpc_verify.rs` — mesa relay+verify examples.
- `rpc_fetch.rs` — RPC best-tip fetch example.
