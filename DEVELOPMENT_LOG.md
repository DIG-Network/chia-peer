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
is reserved for a peer that RELIABLY reported the thing does not exist. `block_timestamp` is reported
`Unsupported` (a first-class fail-closed answer) rather than answered unreliably — a composing registry
falls through to a source that indexes timestamps.

`resolve_singleton_lineage` used to be `Unsupported` for the same reason, on the belief that answering
it from a light source would risk a spoofable, partial lineage. That belief was about *recognising* the
next coin. The interface's canonical walk (feature `lineage-walk`) **derives** it instead — from the
current coin's own spend, with the reveal proven against the coin's puzzle hash — so it needs nothing
but `coin_record` + `coin_spend`, which this provider already answers fail-closed. The refusal was
therefore obsolete, and the method is now a one-line delegation. The temptation to "help" it along with
a `coin_records_by_parent` lookup is exactly the hole the walk exists to close: choosing a successor
from a source-supplied child list hands the source the lineage.

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


## A cache that answers without freshness is a third way to take one source's word

The crate is careful about trust: reads are fail-closed, a puzzle reveal must hash to the coin it
claims, and the lineage walk derives each successor rather than recognising it. All of that guards
what a source SAYS. None of it guarded the local cache, which used to answer any coin it happened to
hold — and a cache is one source's word from the PAST, which is worse: it cannot be re-checked, it
cannot be contradicted, and there is nobody to fail closed against.

The bite is that the lie was self-consistent. `coin_spend` derives spentness from the same cached
`spent_height` that `coin_record` reports, so both reads agreed that a spent coin was unspent. Under
the singleton walk that stops the lineage at a superseded coin and authenticates a destroyed (melted)
singleton as live.

The general rule: **cached state must never outlive the subscription that keeps it current.** There
were three distinct doors to an entry no update could ever correct, and only the third is obvious:

1. a coin untracked because it was seen spent, then UN-SPENT by a later reorg rollback — the rollback
   loop happily edited an entry nothing could subsequently re-assert;
2. an explicit `unsubscribe_coins`, after which the state simply froze;
3. a HINTED coin admitted by `seed`. Subscriptions request `include_hinted: true`, and a hinted coin
   is returned because it is hinted TO the subscribed puzzle hash — its own `puzzle_hash` is
   something else (a CAT outer puzzle hash, typically). `apply_update`'s insert path already filtered
   on the subscribed set, so such a coin could enter but never be updated. It needs neither a reorg
   nor an unsubscribe: a plain CAT wallet's first subscribe reaches it.

Refusing to cache a hinted coin is not a lost capability — the read falls through to a live fetch,
which is the correct answer. Caching it was the defect.

One asymmetry worth keeping: the tracked PEAK is deliberately NOT gated on liveness, even though the
coin state is. The provider's height clamp short-circuits when the peak is unknown, so withholding a
stale peak would DISABLE the clamp and re-open a `peak - height` u32 underflow. A frozen peak clamps
harder — the conservative direction — so staleness is safe there and dangerous in coin state. The
tempting symmetric move is the wrong one.
