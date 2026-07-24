# Spec: `accountWithProof` — a Merkle-path query for trustless light-node reads

**Audience:** the `mina-indexer` (mesa) maintainers.
**Goal:** let the trustless light node serve proof-backed `/account/balance` on mesa-mut
(and any network) by sourcing the account **and its Merkle path** from the indexer, then
verifying it itself against a recursively-proven ledger root.

## Why

The light node already verifies block proofs (ab84160 on mesa-mut). A verified block's
protocol state commits to the **staking-epoch ledger root** (`consensus_state.
staking_epoch_data.ledger.hash`) — a field the SNARK attests, so it's trustless. Given
an account **plus its Merkle inclusion path**, the light node folds them to that root; if
they match, the balance/nonce are trustworthy even though the data came from an untrusted
source. The light node does this today on devnet by walking the libp2p **sync-ledger RPC**
(`answer_sync_ledger_query`). On mesa-mut, no reachable peer serves that RPC — but the
indexer already holds the ledger. This query exposes the same data over GraphQL.

The indexer stays an **untrusted data source**: a wrong account or path simply fails the
fold (or the pubkey cross-check) on the light node. No new trust is placed in the indexer.

## The query

```graphql
type Query {
  # Account + Merkle inclusion path within a specific ledger (by hash).
  accountWithProof(
    ledgerHash: String!     # the ledger to prove against, e.g. a block's
                            # staking_epoch_data.ledger.hash (base58, "j…")
    publicKey: String!      # B62q… account to read
    tokenId: String         # optional; default = MINA (token "1")
  ): AccountWithProof
}

type AccountWithProof {
  # The account, binprot-encoded as MinaBaseAccountBinableArgStableV2, hex string.
  # (Exactly the bytes the sync-ledger RPC returns in `ContentsAre`.)
  accountBinprotHex: String!

  # The Merkle path, leaf → root, one entry per ledger level (length = ledger depth = 35).
  merklePath: [MerklePathNode!]!

  # The account's leaf index in this ledger (0-based). Doubles as the pubkey→index hint.
  index: Int!

  # Echo of the ledger hash the path was produced against (sanity).
  ledgerHash: String!
}

type MerklePathNode {
  # Which side the proven node is on at this level. This is mina-tree's MerklePath enum:
  #   "Left"  => the node is the left child;  `hash` is the right sibling.
  #   "Right" => the node is the right child; `hash` is the left sibling.
  side: MerklePathSide!
  # The sibling hash, as a field element in DECIMAL (matching mina's Fp string form;
  # the same encoding `LedgerHash`/`MinaBaseLedgerHash0StableV1` decodes via `to_field`).
  hash: String!
}

enum MerklePathSide { Left Right }
```

`null` result ⇒ the public key (with that token) is not in that ledger.

## How the light node consumes it (so you can test against it)

The consuming code lives in `mina-verify::account_read` and is already proven on devnet.
Per read, the light node will:

1. `account = mina_tree::Account::try_from(decode_hex(accountBinprotHex) as MinaBaseAccountBinableArgStableV2)`
2. Build `Vec<mina_tree::MerklePath>` from `merklePath`, in the given leaf→root order:
   - `side == Left`  → `MerklePath::Left(hash.to_field::<Fp>())`
   - `side == Right` → `MerklePath::Right(hash.to_field::<Fp>())`
3. `root = ledgerHash.to_field::<Fp>()`
4. **Accept iff** `mina_verify::implied_root(&account, &path) == root` **and**
   `account.public_key.into_address() == publicKey`.

`implied_root` folds exactly as mina hashes the ledger:

```rust
merkle_path.iter().enumerate().fold(account.hash(), |accum, (height, p)| match p {
    MerklePath::Left(right) => V2::hash_node(height, accum, *right),
    MerklePath::Right(left) => V2::hash_node(height, *left, accum),
})
```

So the **only correctness requirement** on your side: the `account` bytes and the sibling
`hash`es must be the ones from a **mina-compatible ledger tree**, i.e. the same tree whose
root is `ledgerHash`.

## Implementation notes

- **If the indexer's ledger is `mina_ledger` (mina-tree `Mask`/`Database`):** this is
  trivial and bit-exact —
  ```rust
  let index = ledger.index_of_account(AccountId::new(pk, token))?;          // -> AccountIndex
  let path: Vec<MerklePath> = ledger.merkle_path_at_index(index);           // leaf -> root
  let account_binprot = MinaBaseAccountBinableArgStableV2::from(&account);  // binprot, then hex
  ```
  Serialize each `MerklePath` element's variant (`Left`/`Right`) + its `Fp` (decimal). Done.
  No need to understand the hashing — mina-tree produces the canonical path.

- **If the indexer uses its own ledger representation:** the sibling hashes and account
  hash MUST match mina's Poseidon hashing (`mina_tree::V2::hash_node(height, l, r)` for
  inner nodes, `Account::hash()` for leaves) or the fold won't reproduce `ledgerHash`.
  Reusing mina-tree to (re)build the proven ledgers is strongly recommended over
  re-deriving the hashing.

- **Which ledgers to support:** at minimum the **staking + next epoch ledgers** (their
  hashes appear in every block's consensus state — that's what the light node proves
  against). Ideally also recent snarked-ledger roots for fresher balances.

## Validation / acceptance test

Self-contained, no light node needed:

1. Pick a recent mesa-mut block; read `staking_epoch_data.ledger.hash` = `H` from its
   protocol state (the light node's `/tip` reports it as `staking_epoch_ledger_hash`).
2. For several known accounts, call `accountWithProof(ledgerHash: H, publicKey: …)`.
3. Fold each result with the snippet above; assert `implied_root == H.to_field()`.

If step 3 holds, the light node will accept the reads. A handy reference oracle: the unit
test `account_read::tests::reconstructs_mina_tree_path_for_several_accounts` in
`mina-verify` builds a real `Database`, takes `merkle_path_at_index`, and folds it to the
root — your serialized output should match that shape exactly.

## Light-node side (already planned)

`/account?pubkey=` will call `accountWithProof` (via `LIGHT_NODE_INDEXER_URL`), verify as
above, and return the proof-anchored balance/nonce — no sync-ledger RPC, no epoch-ledger
sweep. `index` from the response also seeds the pubkey→index cache. Until this query
exists, mesa-mut `/account` stays gated on a sync-ledger-serving peer.
