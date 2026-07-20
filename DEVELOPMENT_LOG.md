# Development log — chia-peer

Durable realizations from building this crate. Context, not a change diary.

## SDK version pairing is load-bearing (0.30, not 0.34)

`chia-wallet-sdk = 0.34` internally depends on `chia-protocol 0.36`, while `dig-chainsource-interface`
(the `ChainSource` trait this crate implements) speaks `chia-protocol 0.26`. Mixing them puts TWO
incompatible `chia-protocol` versions in the graph — the SDK's `Peer` returns `chia_protocol@0.36`
`CoinState`/`Coin`, which will not unify with the `chia_protocol@0.26` types the interface exposes, so
the provider cannot be implemented. `chia-wallet-sdk = 0.30` shares `chia-protocol 0.26` with the
interface (the same pairing chia-query uses). RULE: keep a single `chia-protocol` version across the
read interface; the SDK version is chosen to match it, not the other way around.

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

## Sync facade needs a multi-thread runtime

The `ChainSource` trait is synchronous + object-safe; the provider bridges to the async client with a
`block_in_place`/`block_on` helper that is only sound on a multi-thread tokio runtime and returns a
clear error (never a panic) on a current-thread runtime. Tests drive the sync facade from a plain
`std::thread` (the bridge's "outside a runtime" path) to avoid `block_in_place` misuse.
