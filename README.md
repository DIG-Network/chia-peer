# chia-peer — DEPRECATED

**This crate no longer implements anything.** As of **0.3.0** it is a thin, deprecated re-export
facade over [`chia-query`](https://crates.io/crates/chia-query), which now owns the Chia
wallet-protocol light client this crate used to hold.

New code should depend on `chia-query` directly.

## Why

`chia-peer` was a **second Chia dialler**: its own TLS connector, its own DNS-introducer discovery,
its own IPv6-first candidate ordering and its own reconnect loop — a parallel implementation of
everything `chia_query::peer::connect` and `chia_query::peer::pool` already did. A process running
both (dig-node ran three such stacks) held several TLS connections to several independently-chosen
full nodes, with several notions of the peak and nothing able to reconcile them.

Merging was chosen over relocating for a mechanical reason. `dig-node-core` pinned
`chia-query = "=0.5.1"` — an *exact-equals* requirement on a foundation crate — because this crate
and chia-query had to agree about a third crate's minor, and 0.5.1 was the last version where they
did. **One crate cannot disagree with itself**, so folding removed the coupling rather than
preserving it.

Tracked as [dig_ecosystem#2761](https://github.com/DIG-Network/dig_ecosystem/issues/2761).

## Migration

The light client no longer takes a config of its own, because it no longer dials. It **borrows** a
session from a `ChiaQuery` client's pool:

```rust
use std::time::Duration;
use chia_query::{ChiaQuery, ChiaQueryConfig};

let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;
let light = client.light_client(Duration::from_secs(30)).await;

light.subscribe_coins(vec![coin_id]).await?;
let peak = light.peak().await;
let provider = light.as_chain_source_provider(handle).await;
```

| 0.2.x | 0.3.0 and after |
|---|---|
| `ChiaPeerConfig::mainnet()` | `ChiaQueryConfig` (`network: NetworkType::Mainnet`) |
| `ChiaPeerConfig::with_trusted_endpoint(addr)` | the pool's priority-address path, recorded as `PeerOrigin::Priority` |
| `ChiaLightClient::connect(config)` | `ChiaQuery::light_client(request_timeout)` |
| `ChiaPeerProvider` | `chia_query::peer::LightClientProvider` |
| `ChiaPeerError` | `chia_query::peer::light_client::error::LightClientError` |
| `client.reconnect()` | `ChiaLightClient::reconnect` — now a re-arm, not a dial |
| *(nothing)* | `ChiaLightClient::needs_rearm` — a followed session that ended is now reported |

`ChiaPeerConfig` has **no re-export and no shim**. It configured a dialler, and there is no longer a
dialler here to configure; a type that pretended otherwise would accept an endpoint and a TLS path
and silently ignore both.

## What changed in behaviour, not just in location

- **Subscriptions are anchored.** The light client pins one pooled session and applies only that
  session's frames. `CoinStateUpdate` is an unsolicited push with no request id, so a frame from an
  unfollowed peer is indistinguishable from a fabrication and is discarded.
- **One-shot reads draw from the whole pool** rather than one held connection, ejecting the peer
  that fails them.
- **`ProviderKind` is observed, not declared.** This crate derived it from a `config.trusted` flag
  — whether the operator had *named* an endpoint — with nothing checking that the peer answering was
  the one named. It is now read from the answering session's `PeerOrigin`.

## What did not change

The provider still registers at `DEFAULT_PROVIDER_PRIORITY` = **20**, ahead of the coinset.org tier.
Reads are still **fail-closed**: `Ok(None)`/empty means a peer reliably reported absence, and any
transport or subscription-gap failure is an `Err`, never a false absence. Nothing sets a `trusted`
flag on a dialled peer — that is a custody grant, and no read hands one out.

## License

MIT
