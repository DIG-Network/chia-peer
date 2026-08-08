# Development log — chia-peer

Durable realizations from building this crate. Context, not a change diary.

## SDK version pairing is load-bearing, and the INTERFACE picks the line

The `ChainSource` trait this crate implements exchanges `chia_protocol` types, so the SDK's
`chia-protocol` and the interface's `chia-protocol` MUST be the same version — otherwise the SDK's
`Peer` returns a `CoinState`/`Coin` that will not unify with the one the interface exposes and the
provider simply cannot be implemented.

The RULE, which survives every future bump: **`dig-chainsource-interface` picks the `chia-protocol`
line; the SDK version is then chosen to match it, never the other way around.** Concretely, the
crate moved `dig-chainsource-interface 0.1 -> 0.3`, whose only source change is one additive
`ChainSourceError::TooManyRecords` variant — but whose manifest moved `chia-protocol 0.26 -> 0.36.1`,
which is what forced `chia-wallet-sdk 0.30 -> 0.34` (0.31/0.32 are unusable here: they pin the
`chia` umbrella, not `chia-protocol 0.36.1`).

### There is no `chia` umbrella on the 0.36 line — use the SDK's re-exports

The umbrella `chia` crate goes 0.32.0 straight to 0.42.0 on crates.io, so nothing it publishes can
be held at `chia-protocol 0.36.1`. Depending on it at all would re-introduce the two-version split
the rule above exists to prevent. Take the pieces from the SDK instead:
`chia_wallet_sdk::chia::{bls, ssl, traits}` and `chia_wallet_sdk::clvm_utils` (note `clvm_utils` is
re-exported at the SDK root, *not* under `chia::`). These are by construction the same types the
SDK's own `Peer`/`connect_peer`/`load_ssl_cert` signatures expect, which the umbrella never
guaranteed.

### What actually breaks across 0.26 -> 0.36 (very little)

`chia-sdk-client` 0.30 -> 0.34 is functionally identical — every source diff is edition-2024 import
reordering; `connect_peer`, `create_native_tls_connector`, `load_ssl_cert`, `Peer`, `PeerOptions`,
`Network` are unchanged. `PeerSimulator`'s `new`/`connect_raw`/`Deref<Target = Mutex<Simulator>>`
and `Simulator::{new_coin, insert_coin}` are unchanged too. The one real signature change on this
crate's surface: `clvm_utils::tree_hash_from_bytes` returns `Result<TreeHash, clvmr::EvalErr>`
instead of `io::Result<TreeHash>` — `EvalErr` is a `thiserror` enum, so `Display`/`Debug` call sites
compile untouched.

## The SDK `Peer` is a concrete struct → test behind a seam

`chia_wallet_sdk::client::Peer` is a concrete `Arc`-wrapped struct, not a trait, so provider tests
cannot mock it directly. The `CoinStateFetcher` async trait is the seam: `PeerFetcher` implements it
over the real peer, and provider unit tests use a scripted mock. For end-to-end coverage of the real
wire path, `chia-wallet-sdk`'s `peer-simulator` feature (`PeerSimulator`) starts an in-process
wallet-protocol full node — `connect_raw()` yields a real `(Peer, Receiver)` with no network. The
simulator uses `TESTNET11_CONSTANTS`, so tests seed `PeerFetcher` with the testnet11 genesis challenge.

## `request_coin_state` header_hash at height 0

With `previous_height = None`, the peer/simulator validates `header_hash == genesis_challenge`
(`Bytes32::default()` is rejected). Always pass the network's genesis challenge as the height-0
header hash.

## The simulator errors (not rejects) for an unspent coin's puzzle/solution

`request_puzzle_and_solution` against the simulator for an unspent coin returns a transport/parse
error rather than a clean `RejectPuzzleSolution`. Tests assert the safety invariant (never a fabricated
`Ok(Some(_))`) rather than a specific `Ok(None)`. In production the provider only calls
`puzzle_and_solution` after confirming a spent height, so this path is reached with a real reveal.

## Fail-closed is the whole point

Every `ChiaPeerError` maps to a `ChainSourceError` `Err`, never `Ok(None)`. Absence (`Ok(None)`/empty)
is reserved for a peer that RELIABLY reported the thing does not exist. `resolve_singleton_lineage` and
`block_timestamp` are reported `Unsupported` (a first-class fail-closed answer) rather than answered
unreliably from subscription state — a composing registry falls through to a source that supports them.

## Known advisory: RUSTSEC-2023-0071 (rsa Marvin timing side-channel)

`rsa 0.9.10` (transitive via `chia-ssl 0.26` → TLS cert handling) carries RUSTSEC-2023-0071 (a
Marvin timing side-channel). There is NO upstream fix available, it is IDENTICAL under chia-wallet-sdk
0.34, and it is ecosystem-wide (every crate on this SDK stack). It is not exploitable in this crate's
usage (ephemeral self-signed client-cert generation, no RSA decryption of attacker-chosen ciphertext).
No crate-level fix here; tracked as an ecosystem follow-up when an upstream `rsa` fix lands.

## Sync facade needs a multi-thread runtime

The `ChainSource` trait is synchronous + object-safe; the provider bridges to the async client with a
`block_in_place`/`block_on` helper that is only sound on a multi-thread tokio runtime and returns a
clear error (never a panic) on a current-thread runtime. Tests drive the sync facade from a plain
`std::thread` (the bridge's "outside a runtime" path) to avoid `block_in_place` misuse.
