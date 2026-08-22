# chia-peer — normative specification

**Status: DEPRECATED as of 0.3.0.** This crate specifies no behaviour of its own.

## 1. Scope

`chia-peer` 0.3.0 is a **re-export facade**. It contains one source file, declares one dependency,
and implements nothing. Everything it previously specified — the connection model, the subscription
cache and its reorg semantics, the `ChainSource` fail-closed contract, spend submission, and the
error taxonomy — now lives in `chia-query` and is specified by that crate's `SPEC.md`, §§ *Light
client (native)* and *Behavioral Rules* 5g–5j.

An implementation seeking the normative contract MUST read `chia-query`'s `SPEC.md`. This document
states only what remains true of this crate.

## 2. Requirements on this crate

1. **It MUST implement nothing.** The crate ships exactly one source file (`src/lib.rs`) containing
   re-exports, one `const`, one function and its tests. Adding a module of its own reopens the split
   this crate's deprecation closed (dig_ecosystem#2761). Enforced by
   `tests::the_crate_ships_exactly_one_source_file`.
2. **Every re-export of a formerly-owned item MUST carry `#[deprecated]`** naming its canonical
   `chia_query` path, so a consumer compiling against 0.1/0.2 is told where the item went rather
   than merely finding that it still resolves.
3. **It MUST NOT depend on the Chia wallet-protocol stack.** `chia-wallet-sdk`, `chia-protocol`,
   `dig-chainsource-interface`, `tokio`, `tokio-tungstenite`, `async-trait`, `rand`, `futures-util`
   and the Linux vendored-OpenSSL block are all removed. Its only dependency is `chia-query`.
4. **Its `chia-query` requirement MUST be a caret, never an exact-equals.** The `=0.5.1` pin this
   crate's existence forced on `dig-node-core` is what the fold removed; an exact-equals here would
   recreate that shape one layer up.
5. **It MUST NOT reintroduce a dialler.** Opening a Chia wallet-protocol connection is
   `chia_query::peer::connect`'s alone (chia-query `SPEC.md` rule 5g, the single-dialler invariant).
6. **It MUST NOT re-export a configuration type for the deleted dialler.** `ChiaPeerConfig`
   configured a connection this crate no longer makes; a shim accepting an endpoint and a TLS path
   and ignoring both would be a surface that lies about what it does.

## 3. Preserved guarantees

These are `chia-query`'s to uphold and are restated here only because a consumer migrating across
the move depends on them:

- The light-client provider registers at `DEFAULT_PROVIDER_PRIORITY` = **20**, ahead of the
  coinset.org tier.
- Reads are **fail-closed**: `Ok(None)`/empty means a source reliably reported absence; a transport,
  timeout, parse or subscription-gap failure is an `Err` and is NEVER reported as absence.
- `resolve_singleton_lineage` and `block_timestamp` are `Unsupported` from the light-client source
  and fall through to an aggregating source.
- No code path sets a `trusted` flag on a dialled peer. The pool dials with `PeerOptions::default()`.

## 4. Removal

This facade exists so a consumer of 0.1/0.2 finds a signpost rather than a crate that vanished from
crates.io, which is irreversible. It MUST NOT gain features.
