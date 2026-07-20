//! # chia-peer — DIG Chia light-client connectivity (dig-node seam 1)
//!
//! A thin wrapper that CONFIGURES and DRIVES [`chia_wallet_sdk`]'s light-client `Peer` so dig-node
//! can act as a Chia wallet-protocol client: connect to full nodes, subscribe to coin/puzzle-hash
//! state, track the peak, handle reorgs, and submit spend bundles.
//!
//! The heavy lifting — the wallet-protocol wire, TLS, DNS-introducer discovery, coin-state
//! subscription, and transaction submission — lives in the SDK. This crate adds only DIG glue and
//! exposes the read side through the canonical [`dig_chainsource_interface::ChainSource`] trait.
//!
//! (Scaffold in progress — the public API lands in `feat/chia-peer-light-client`.)

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
