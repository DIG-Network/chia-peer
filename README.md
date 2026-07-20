# chia-peer

DIG Chia light-client connectivity — dig-node **seam 1**.

`chia-peer` lets a DIG node act as a Chia **wallet-protocol light client** (Sage-style): it connects
to Chia full nodes *as a client*, subscribes to coin-state and puzzle-hash state, tracks the peak,
handles reorgs, and submits spend bundles. It is a **thin wrapper** that configures and drives
[`chia-wallet-sdk`](https://crates.io/crates/chia-wallet-sdk) 0.34 — the SDK already provides the
light-client `Peer`, the wallet-protocol wire, coin-state subscription, TLS, DNS-introducer
discovery, and transaction submission. This crate adds only DIG glue.

It exposes its read side through the ecosystem's canonical
[`dig-chainsource-interface`](https://crates.io/crates/dig-chainsource-interface) `ChainSource`
trait, so dig-node registers it as a provider via dependency injection.

## What it adds over the SDK

- **IPv6-first peer ordering** (CLAUDE.md §5.2): candidates are tried all-IPv6-first, IPv4 only as a
  fallback (`::1` before `127.0.0.1`).
- **Subscription cache**: coin-state and puzzle-hash subscriptions are tracked and their results
  cached, so reads answer from local state and reorgs roll the cache back correctly.
- **Fail-closed `ChainSource`**: a cache/subscribe miss falls back to a non-subscribing query;
  a transport or subscription-gap failure is an `Err`, **never** a false `Ok(None)`.
- **Typed spend submission** mapped onto a `SubmitOutcome` (not part of the reads-only `ChainSource`).

## Boundary (what it does NOT do)

- It does not reimplement the wallet protocol, TLS, or discovery — those are the SDK's.
- It holds no keys and moves no value on its own; spend bundles are built and signed elsewhere and
  handed to `submit_spend`.
- It is not chia-query: chia-query is the aggregating coinset+peer read router; chia-peer is a single
  subscribing light-client provider. `chia-peer` has **no** dependency on chia-query.

## Usage

```rust,no_run
use chia_peer::{ChiaLightClient, ChiaPeerConfig};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let client = ChiaLightClient::connect(ChiaPeerConfig::mainnet()).await?;
let (height, header_hash) = client.peak().unwrap_or_default();
# Ok(())
# }
```

See [`SPEC.md`](./SPEC.md) for the normative contract (subscription model, peak/reorg semantics,
IPv6 order, the SDK reuse boundary, the `ChainSource` fail-closed mapping) and [`runbooks/`](./runbooks)
for release + local-run procedures.

## License

MIT
