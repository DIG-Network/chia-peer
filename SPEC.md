# chia-peer — normative specification

`chia-peer` is a Chia **wallet-protocol light client** for DIG nodes (dig-node seam 1). It configures
and drives [`chia-wallet-sdk`] to connect to Chia full nodes as a client, subscribe to coin and
puzzle-hash state, track the peak, handle reorgs, and submit spend bundles, and it exposes its read
side through the canonical [`dig-chainsource-interface`] `ChainSource` trait. This document is the
authoritative contract an independent reimplementation could be built against.

## 1. Scope and reuse boundary

chia-peer is a **thin wrapper**. The wallet-protocol wire, TLS, websocket transport, DNS-introducer
discovery, coin-state subscription, and transaction submission are the SDK's; chia-peer MUST NOT
reimplement them. Each public method wraps a named SDK `Peer` call and adds only DIG glue:

| chia-peer API | wraps SDK call | glue added |
|---|---|---|
| `ChiaLightClient::connect` | `connect_peer` (sends `Handshake{ node_type: Wallet }`) | IPv6-first ordering, drive-loop |
| `subscribe_coins` | `Peer::request_coin_state(subscribe = true)` | subscription tracking + cache seed |
| `subscribe_puzzle_hashes` | `Peer::request_puzzle_state(subscribe = true)` (paged) | tracking + filter config + cache seed |
| `submit_spend` | `Peer::send_transaction` | `TransactionAck` → typed `SubmitOutcome` |
| `unsubscribe_coins` | `Peer::remove_coin_subscriptions` | local untrack |
| `peak` | (drive-loop over `NewPeakWallet`) | local peak state |
| `reconnect` | `connect_peer` | backoff-free re-dial + subscription re-arm |
| `as_chain_source_provider` | — | the sync `ChainSource` facade |

chia-peer has **no dependency on chia-query**. chia-query is the aggregating coinset+peer read router;
chia-peer is one subscribing light-client provider that a registry composes alongside others.

## 2. Version pairing (normative)

chia-peer depends on `chia-wallet-sdk = 0.30`, `chia = 0.26`, `chia-protocol = 0.26`. This pairing is
REQUIRED: `dig-chainsource-interface` speaks `chia-protocol 0.26`, and a newer wallet-sdk pulls a newer
`chia-protocol` whose `Coin`/`CoinSpend`/`Bytes32` types would NOT unify with the interface the
provider implements. A single `chia-protocol` version across the read interface is an invariant.

## 3. Connection model

- **IPv6-first (CLAUDE.md §5.2).** Candidate addresses are ordered so every IPv6 address is dialed
  before any IPv4 address, and the IPv6 loopback (`::1`) before the IPv4 loopback (`127.0.0.1`).
  Ordering is a stable partition: no candidate is dropped, so IPv4 remains a full fallback. IPv4 is
  used only when IPv6 is unreachable.
- **Endpoint selection.** An explicit `endpoint` (the operator's own node) is the sole candidate and
  marks the client `trusted`. Otherwise candidates come from the network's DNS introducers, shuffled
  to spread load, then ordered per the rule above.
- **TLS.** A configured cert/key pair is loaded; absent one, an ephemeral self-signed Chia identity is
  generated (the anonymous-read case). `peer_id` derives from the TLS SPKI per the SDK.
- **Handshake.** `connect_peer` sends `Handshake { node_type: NodeType::Wallet, .. }`; chia-peer never
  hand-rolls the handshake.

## 4. Subscription cache + peak + reorg semantics

The client keeps a local `CoinStateCache`, updated by a background drive-loop reading the peer's
inbound `Message` stream:

- **`NewPeakWallet`** advances the tracked peak `(height, header_hash)`. This path is ADVANCE-ONLY: a
  stale, out-of-order, or hostile lower peak never regresses a higher one.
- **`CoinStateUpdate`** carries `items`, `height`, `fork_height`, `peak_hash`. It is applied as:
  1. **Reorg rollback across `fork_height`:** a cached coin *created* above the fork that the update
     does not re-assert is dropped (it no longer exists); a cached coin *spent* above the fork has its
     spent height cleared (its spend was rolled back), unless the update re-asserts it.
  2. **Authoritative overwrite:** every subscribed coin in `items` is admitted through the single
     cache-add path, which refuses any coin created above the current peak (see the invariant below).
  3. **Peak update:** an **authoritative reorg** — `fork_height` below the current peak, the rollback
     in (1) actually changed subscribed state, and the update is well-formed (`height >= fork_height`)
     — sets the peak DOWN to `(height, peak_hash)`, so confirmation counts do not overstate during a
     genuine deep down-reorg. A normal forward update, an update that rolls back nothing, or one with
     `height < fork_height` is advance-only. Peak-lowering adds no trust beyond the coin-state
     rollback the same update already performs, and a bare `NewPeakWallet` can never lower the peak.

  **Invariant (structural, all paths — enforced BY CONSTRUCTION):** no cached coin ever has
  `created_height > peak_height`, so a consumer's `peak_height - created_height` (u32) confirmation
  count can never underflow into a spurious hyper-confirmed value. This is not a per-call-site check;
  it holds at two boundaries: (a) the **add boundary** — every coin (from `apply_update`'s items AND
  from `seed`) enters through one helper that refuses an above-peak coin; (b) the **peak boundary** —
  every peak change (advance or authoritative-reorg lowering, including the first peak-set) sweeps out
  any coin now above the peak. Together they make the property hold after every public mutation
  regardless of ordering.
- Coins reported as spent are dropped from the local subscription set (their state is retained for
  reads; only the live subscription is released).

Reads consult the cache first; a miss falls through to a **non-subscribing** peer query, so a read
never silently grows the subscription set.

## 5. `ChainSource` provider (fail-closed contract)

`ChiaPeerProvider` implements `dig_chainsource_interface::{ChainSource, ChainSourceProvider}` as a
**synchronous** facade over the async client, via an async→sync bridge that requires a **multi-thread**
tokio runtime and fails closed with a clear error on a current-thread runtime (never a tokio panic).

The fail-closed contract is absolute (interface SPEC §3): `Ok(None)` / an empty `Vec` means the peer
RELIABLY reported absence; any transport, timeout, rejection, malformed payload, or not-connected
condition is `Err(_)` — NEVER a false `Ok(None)`. The `ChiaPeerError → ChainSourceError` mapping
preserves this: every error variant maps to an `Err`, only classifying the reason.

Method behaviour:

| method | behaviour |
|---|---|
| `coin_record` | cache, else non-subscribing `request_coin_state`; empty → `Ok(None)` |
| `coin_records_by_puzzle_hash` | non-subscribing `request_puzzle_state` (paged) |
| `coin_records_by_parent` | `request_children` |
| `coin_spend` | resolve the coin (for its real puzzle hash) → if spent, `request_puzzle_and_solution` → `CoinSpend`; unspent/unknown → `Ok(None)` |
| `parent_spend` | interface default (coin_record + coin_spend) |
| `peak_height` | tracked peak height, or `Ok(None)` |
| `resolve_singleton_lineage` | `Err(Unsupported)` — a money-critical forward walk belongs to an aggregating source; a subscription light client MUST NOT answer it partially |
| `block_timestamp` | `Err(Unsupported)` — a light source keeps no timestamp index |

`Unsupported` is a first-class fail-closed answer; a composing registry falls through to a source that
supports these reads. Reporting `Unsupported` is REQUIRED over returning an unreliable value.

**Confirmation-height clamp (all read paths — money-path correctness):** every read that surfaces a
coin's `created_height` as `confirmed_height` (`coin_record`, `coin_records_by_puzzle_hash`,
`coin_records_by_parent`) MUST report `confirmed_height ≤ peak_height`. The cache path upholds this
structurally (§4 invariant), but the cache-MISS *live-fetch* path returns the peer's `created_height`
directly, which — for an unsubscribed coin created in the current tip block, read in the one-block
window before the drive loop processes the matching `NewPeakWallet` — could exceed the drive-loop-lagged
peak and underflow a consumer's `peak_height - confirmed_height` (u32) count into a spurious
hyper-confirmed value. The provider therefore clamps the reported `confirmed_height` to
`min(created_height, peak_height)`: an above-peak coin reports 0 confirmations (the conservative,
understating direction) and remains PRESENT (never omitted — the coin genuinely exists). The peak is
never inflated from a fetched coin (a lying peer must not raise it). When no peak is known yet
(`peak_height` is `None`), no `peak - confirmed` subtraction is possible and the height is left as
reported.

### Provider descriptor

`provider_info` reports `ProviderKind::LocalNode` when pointed at the operator's own trusted node,
else `ProviderKind::Custom` (introducer-discovered). `trustless = false` (answers are taken on trust);
default `priority = 20`.

## 6. Spend submission

`submit_spend` wraps `Peer::send_transaction` and maps the node's `TransactionAck.status` to
`SubmitOutcome`: `1 → Accepted` (in mempool, pending confirmation), `2 → Pending`, `3 → Failed`,
other → `Unknown(status)`. Submission is a WRITE path and is deliberately NOT part of the reads-only
`ChainSource` surface.

## 7. Error taxonomy

`ChiaPeerError`: `Transport`, `Rejected`, `Malformed`, `Timeout`, `PeerDiscoveryFailed`,
`NotConnected`, `Tls`. Mapping to `ChainSourceError`: `Timeout`/timeout-worded `Transport` → `Timeout`;
`Malformed` → `Malformed`; all others → `Transport`. No variant is ever collapsed to `Ok(None)`.

[`chia-wallet-sdk`]: https://crates.io/crates/chia-wallet-sdk
[`dig-chainsource-interface`]: https://crates.io/crates/dig-chainsource-interface
