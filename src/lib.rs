//! # chia-peer — DIG Chia light-client connectivity (dig-node seam 1)
//!
//! A thin wrapper that CONFIGURES and DRIVES [`chia_wallet_sdk`]'s light-client `Peer` so a DIG node
//! can act as a Chia wallet-protocol client: connect to full nodes, subscribe to coin/puzzle-hash
//! state, track the peak, handle reorgs, and submit spend bundles.
//!
//! The heavy lifting — the wallet-protocol wire, TLS, DNS-introducer discovery, coin-state
//! subscription, and transaction submission — lives in the SDK. This crate adds only DIG glue:
//! IPv6-first dialing (§5.2), a reorg-aware subscription cache, reconnect-with-re-arm, and a
//! [`ChainSource`] read facade.
//!
//! ## Shape
//!
//! - [`ChiaLightClient`] — connect, [`subscribe_coins`](ChiaLightClient::subscribe_coins),
//!   [`subscribe_puzzle_hashes`](ChiaLightClient::subscribe_puzzle_hashes),
//!   [`submit_spend`](ChiaLightClient::submit_spend), [`peak`](ChiaLightClient::peak),
//!   [`unsubscribe_coins`](ChiaLightClient::unsubscribe_coins),
//!   [`reconnect`](ChiaLightClient::reconnect), and
//!   [`as_chain_source_provider`](ChiaLightClient::as_chain_source_provider).
//! - [`ChiaPeerProvider`] — the synchronous, fail-closed [`ChainSource`] provider dig-node registers.
//!
//! ## Reads are fail-closed
//!
//! Through the provider, `Ok(None)`/empty means the peer reliably reported absence; any
//! transport/subscription-gap failure is an `Err`, never a false `Ok(None)` — the crux of the
//! [`dig_chainsource_interface`] contract.
//!
//! ## The SDK version pairing
//!
//! This crate pins `chia-wallet-sdk = 0.30`, which shares `chia-protocol = 0.26` with
//! `dig-chainsource-interface`. That single `chia-protocol` version is REQUIRED: a newer SDK pulls a
//! newer `chia-protocol` whose types would not unify with the interface the provider implements.

mod bridge;
mod cache;
mod client;
mod config;
mod connect;
mod error;
mod fetcher;
mod ordering;
mod provider;

pub use client::{ChiaLightClient, SubmitOutcome, DEFAULT_PROVIDER_PRIORITY};
pub use config::{ChiaNetwork, ChiaPeerConfig};
pub use error::ChiaPeerError;
pub use fetcher::{CoinStateFetcher, PeerFetcher};
pub use provider::ChiaPeerProvider;

// Re-export the canonical interface types a consumer needs to register the provider, so it need not
// depend on `dig-chainsource-interface` separately just to name them.
pub use dig_chainsource_interface::{
    ChainSource, ChainSourceError, ChainSourceProvider, ProviderInfo, ProviderKind,
};

/// The crate version, sourced from `Cargo.toml` at build time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_is_reported() {
        assert!(!VERSION.is_empty());
    }
}
