//! # chia-peer — DEPRECATED. Use [`chia_query::peer::light_client`].
//!
//! **This crate no longer implements anything.** As of 0.3.0 it is a thin, deprecated re-export
//! facade over [`chia_query`], which now owns the light client this crate used to hold.
//!
//! # Why it was folded away
//!
//! `chia-peer` was a **second Chia dialler**. It carried its own TLS connector, its own
//! DNS-introducer discovery, its own IPv6-first candidate ordering and its own reconnect loop —
//! a parallel implementation of everything `chia_query::peer::connect` and `chia_query::peer::pool`
//! already did. A process running both (dig-node ran three such stacks) held several TLS
//! connections to several independently-chosen full nodes, with several notions of the peak and
//! nothing able to reconcile them. That is dig_ecosystem#2761.
//!
//! Merging was chosen over relocating for a reason that is mechanical rather than aesthetic.
//! `dig-node-core` pinned `chia-query = "=0.5.1"` — an *exact-equals* requirement on a foundation
//! crate — because this crate and chia-query had to agree about a third crate's minor, and 0.5.1 was
//! the last version where they did. **One crate cannot disagree with itself**, so folding removed
//! the coupling rather than preserving it.
//!
//! The deleted dialler also still carried a `shuffle()` immediately followed by `dedup()` — which
//! only removes ADJACENT duplicates, so shuffling first made the deduplication very nearly
//! vacuous. It was deleted rather than fixed: chia-query's `ordering::candidate_order` already had
//! the correct version, and two crates disagreeing about one behaviour is precisely what this
//! change exists to remove.
//!
//! # Migration
//!
//! The light client is no longer constructed from a config of its own, because it no longer dials.
//! It BORROWS a session from a [`ChiaQuery`] client's pool:
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use chia_query::{ChiaQuery, ChiaQueryConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = ChiaQuery::new(ChiaQueryConfig::default()).await?;
//! let light = client.light_client(Duration::from_secs(30)).await;
//! light.subscribe_coins(vec![]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! | 0.2.x | 0.3.0 and after |
//! |---|---|
//! | `ChiaPeerConfig::mainnet()` | [`ChiaQueryConfig`] (`network: NetworkType::Mainnet`) |
//! | `ChiaPeerConfig::with_trusted_endpoint(addr)` | the pool's own priority-address path, which records the result as `PeerOrigin::Priority` |
//! | `ChiaLightClient::connect(config)` | [`ChiaQuery::light_client`] |
//! | `ChiaPeerProvider` | [`chia_query::peer::LightClientProvider`] |
//! | `ChiaPeerError` | [`chia_query::peer::light_client::error::LightClientError`] |
//! | `client.reconnect()` | [`ChiaLightClient::reconnect`], now a re-arm rather than a dial |
//! | *(nothing)* | [`ChiaLightClient::needs_rearm`] — a followed session that ended is now REPORTED |
//!
//! **`ChiaPeerConfig` has no re-export and no shim.** It configured a dialler, and there is no
//! longer a dialler here to configure; a type that pretended otherwise would accept an endpoint and
//! a TLS path and silently ignore both. Use [`ChiaQueryConfig`], which configures the pool that
//! actually dials.
//!
//! # What changed in behaviour, not just in location
//!
//! - **Subscriptions are ANCHORED.** The light client pins one pooled session and applies only that
//!   session's frames. A `CoinStateUpdate` is an unsolicited push carrying no request id, so a
//!   frame from an unfollowed peer is indistinguishable from a fabrication and is discarded.
//! - **One-shot reads draw from the whole pool** rather than from a single held connection, and
//!   eject the peer that fails them.
//! - **`ProviderKind` is OBSERVED, not declared.** This crate derived it from a `config.trusted`
//!   flag — whether the operator had *named* an endpoint — with nothing checking that the peer
//!   answering was the one named, so a discovered introducer result could be reported as the
//!   operator's own node while outranking coinset.org at priority 20. It is now read from the
//!   answering session's `PeerOrigin`.
//!
//! # What did NOT change
//!
//! The provider still registers at [`DEFAULT_PROVIDER_PRIORITY`] = 20, ahead of the coinset.org
//! tier. Reads are still fail-closed: `Ok(None)`/empty means a peer reliably reported absence, and
//! any transport or subscription-gap failure is an `Err`, never a false absence. Nothing sets a
//! `trusted` flag on a dialled peer — that is a custody grant, and no read hands one out.
//!
//! # Removal
//!
//! This facade exists so a consumer of 0.1/0.2 finds a signpost rather than a crate that vanished.
//! It will not gain features. New code should depend on `chia-query` directly.

#![deny(missing_docs)]

/// The light client, its provider, and the client that hands one out.
///
/// Every item is re-exported from [`chia_query`] and deprecated at this path. Follow each link to
/// the canonical location and depend on `chia-query` directly.
pub use chia_query::peer::light_client;

#[deprecated(
    since = "0.3.0",
    note = "moved to chia_query::peer::ChiaLightClient; construct it with ChiaQuery::light_client() \
            rather than by dialling — see the crate docs for the migration table"
)]
pub use chia_query::peer::ChiaLightClient;

#[deprecated(
    since = "0.3.0",
    note = "renamed to chia_query::peer::LightClientProvider; its ProviderKind is now read from the \
            answering session's PeerOrigin rather than from configuration"
)]
pub use chia_query::peer::LightClientProvider as ChiaPeerProvider;

#[deprecated(
    since = "0.3.0",
    note = "moved to chia_query::peer::SubmitOutcome (unchanged)"
)]
pub use chia_query::peer::SubmitOutcome;

#[deprecated(
    since = "0.3.0",
    note = "renamed to chia_query::peer::light_client::error::LightClientError; the Tls variant is \
            gone, since this type no longer builds a TLS connector"
)]
pub use chia_query::peer::light_client::error::LightClientError as ChiaPeerError;

#[deprecated(
    since = "0.3.0",
    note = "moved to chia_query::peer::light_client::DEFAULT_PROVIDER_PRIORITY (still 20)"
)]
pub use chia_query::peer::light_client::DEFAULT_PROVIDER_PRIORITY;

#[deprecated(
    since = "0.3.0",
    note = "use chia_query::ChiaQuery directly; it owns the peer pool the light client borrows from"
)]
pub use chia_query::{ChiaQuery, ChiaQueryConfig, NetworkType};

// The canonical interface types a consumer needs to register the provider, re-exported so it need
// not depend on `dig-chainsource-interface` separately just to name them. Not deprecated: these are
// another crate's types and this path was never their home, so pointing at `chia_query` for them
// would be no more canonical than pointing at the interface crate itself.
pub use chia_query::provider_registry::interface::{
    ChainSource, ChainSourceError, ChainSourceProvider, ProviderInfo, ProviderKind,
};

/// The crate version, sourced from `Cargo.toml` at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Whether this crate still implements anything of its own.
///
/// Always `false` from 0.3.0. Exists so the deprecation is a value a consumer's own test can
/// assert on, rather than a doc-comment nobody's build reads — a crate that quietly regrew an
/// implementation would flip this, and saying so is cheaper than discovering it.
pub fn is_facade_only() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_reported() {
        assert!(!VERSION.is_empty());
    }

    /// The facade must be a facade.
    ///
    /// `is_facade_only` is a hand-maintained claim, so this pins the fact it claims: the crate ships
    /// exactly ONE source file. Regrowing an implementation means adding a second, and that is the
    /// moment the split dig_ecosystem#2761 closed would reopen.
    #[test]
    fn the_crate_ships_exactly_one_source_file() {
        assert!(is_facade_only());
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let files: Vec<String> = std::fs::read_dir(&src)
            .expect("read src/")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name.ends_with(".rs"))
            .collect();
        assert_eq!(
            files,
            vec!["lib.rs".to_string()],
            "chia-peer is a re-export facade; every implementation belongs in chia-query"
        );
    }

    /// The one behaviour a consumer most likely depends on across the move: the try-order that puts
    /// a subscribing session ahead of the coinset.org HTTP tier.
    #[test]
    #[allow(deprecated)]
    fn the_provider_priority_survived_the_move() {
        assert_eq!(DEFAULT_PROVIDER_PRIORITY, 20);
    }
}
