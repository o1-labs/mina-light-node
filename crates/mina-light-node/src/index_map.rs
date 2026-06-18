//! Compact public-key → leaf-index map (the baked artifact).
//!
//! A sorted array of `(key, index)` — `key` = blake2b-128 of the B62 address. Built once
//! (by `mapgen` sweeping the epoch ledger) and shipped/mounted so the server resolves
//! `/account?pubkey=` without a cold-start sweep. An **untrusted hint**: a wrong index
//! just fails the on-read Merkle proof / pubkey cross-check → refetch/"not found", never
//! a wrong balance. A hash collision is astronomically unlikely and equally harmless.
//!
//! Layout: `[u64 LE covered][entry…]`, `entry = [16-byte key][u32 LE index]`, entries
//! sorted by key, deduped first-wins (lowest index per key — a multi-token pubkey's
//! earliest/MINA account). Format-compatible with the mina-verify-mobile embedded map.

use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;

const KEY: usize = 16;
const ENTRY: usize = KEY + 4;

/// blake2b-128 of the address string — the lookup key. Generation and lookup MUST use
/// this same function.
pub fn addr_key(addr: &str) -> [u8; KEY] {
    let mut h = Blake2bVar::new(KEY).expect("valid blake2b output size");
    h.update(addr.as_bytes());
    let mut out = [0u8; KEY];
    h.finalize_variable(&mut out).expect("blake2b finalize");
    out
}

/// The `covered` count (num_accounts at build time) from the blob header.
pub fn covered(bin: &[u8]) -> u64 {
    if bin.len() >= 8 {
        u64::from_le_bytes(bin[..8].try_into().unwrap())
    } else {
        0
    }
}

/// All `(key, index)` entries in the blob (for loading into an in-memory map).
pub fn load(bin: &[u8]) -> Vec<([u8; KEY], u64)> {
    if bin.len() < 8 {
        return Vec::new();
    }
    let entries = &bin[8..];
    let n = entries.len() / ENTRY;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let e = &entries[i * ENTRY..i * ENTRY + ENTRY];
        let mut k = [0u8; KEY];
        k.copy_from_slice(&e[..KEY]);
        let idx = u32::from_le_bytes(e[KEY..ENTRY].try_into().unwrap()) as u64;
        out.push((k, idx));
    }
    out
}

/// Binary-search the blob for `key`, returning its leaf index.
pub fn lookup(bin: &[u8], key: &[u8; KEY]) -> Option<u64> {
    if bin.len() < 8 {
        return None;
    }
    let entries = &bin[8..];
    let n = entries.len() / ENTRY;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let e = &entries[mid * ENTRY..mid * ENTRY + ENTRY];
        match e[..KEY].cmp(&key[..]) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => {
                return Some(u32::from_le_bytes(e[KEY..ENTRY].try_into().unwrap()) as u64);
            }
        }
    }
    None
}

/// Serialize `(address, index)` pairs into the blob: dedup first-wins (lowest index per
/// key), sort by key, prepend `covered`. Used by the `mapgen` generator.
pub fn build(pairs: &[(String, u64)], covered: u64) -> Vec<u8> {
    use std::collections::HashMap;
    let mut by_key: HashMap<[u8; KEY], u32> = HashMap::new();
    for (addr, idx) in pairs {
        let k = addr_key(addr);
        let slot = by_key.entry(k).or_insert(*idx as u32);
        if (*idx as u32) < *slot {
            *slot = *idx as u32; // keep the lowest index
        }
    }
    let mut entries: Vec<([u8; KEY], u32)> = by_key.into_iter().collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut out = Vec::with_capacity(8 + entries.len() * ENTRY);
    out.extend_from_slice(&covered.to_le_bytes());
    for (k, idx) in entries {
        out.extend_from_slice(&k);
        out.extend_from_slice(&idx.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_then_lookup_roundtrips() {
        let pairs = vec![
            ("B62qaaa".to_string(), 5),
            ("B62qbbb".to_string(), 2),
            ("B62qaaa".to_string(), 9), // dup — first-wins keeps the lowest (5)
            ("B62qccc".to_string(), 7),
        ];
        let bin = build(&pairs, 100);
        assert_eq!(covered(&bin), 100);
        assert_eq!(lookup(&bin, &addr_key("B62qaaa")), Some(5));
        assert_eq!(lookup(&bin, &addr_key("B62qbbb")), Some(2));
        assert_eq!(lookup(&bin, &addr_key("B62qccc")), Some(7));
        assert_eq!(lookup(&bin, &addr_key("B62qzzz")), None);
    }

    #[test]
    fn load_returns_all_entries() {
        let bin = build(&[("B62qaaa".into(), 1), ("B62qbbb".into(), 2)], 2);
        let entries = load(&bin);
        assert_eq!(entries.len(), 2);
        // every loaded key resolves to the same index via lookup
        for (k, idx) in entries {
            assert_eq!(lookup(&bin, &k), Some(idx));
        }
    }

    #[test]
    fn empty_blob_is_safe() {
        assert_eq!(covered(&[]), 0);
        assert!(load(&[]).is_empty());
        assert_eq!(lookup(&[], &addr_key("x")), None);
    }
}
