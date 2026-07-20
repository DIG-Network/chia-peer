//! Peer connection glue: build a TLS connector, discover candidate full nodes, order them IPv6-first
//! (CLAUDE.md §5.2), and hand back the first that completes the wallet-protocol handshake.
//!
//! The heavy lifting is the SDK's:
//! [`connect_peer`](chia_wallet_sdk::client::connect_peer) performs the websocket + TLS connect AND
//! sends the `Handshake { node_type: Wallet, .. }`. This module only chooses *which* address to dial
//! and in *what order*.

use std::net::SocketAddr;

use chia::ssl::ChiaCertificate;
use chia_protocol::Message;
use chia_wallet_sdk::client::{
    connect_peer, create_native_tls_connector, load_ssl_cert, Network, Peer, PeerOptions,
};
use rand::seq::SliceRandom;
use tokio::sync::mpsc;
use tokio_tungstenite::Connector;

use crate::config::{ChiaNetwork, ChiaPeerConfig};
use crate::error::ChiaPeerError;
use crate::ordering::order_candidates;

/// How many introducer results to resolve per DNS batch.
const DISCOVERY_BATCH: usize = 10;

/// Builds the native-TLS connector, using the configured cert/key paths when present and generating
/// an ephemeral self-signed identity otherwise (the anonymous-read case, §5.3).
pub(crate) fn build_connector(config: &ChiaPeerConfig) -> Result<Connector, ChiaPeerError> {
    let cert = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            load_ssl_cert(cert_path, key_path).map_err(|e| ChiaPeerError::Tls(e.to_string()))?
        }
        _ => ChiaCertificate::generate().map_err(|e| ChiaPeerError::Tls(e.to_string()))?,
    };
    create_native_tls_connector(&cert).map_err(|e| ChiaPeerError::Tls(e.to_string()))
}

/// Chooses the dial order: an explicit trusted endpoint alone, otherwise the discovered set —
/// deduped, shuffled to spread load, then ordered IPv6-first.
///
/// Pure over its inputs so the ordering policy is unit-testable without touching the network.
pub(crate) fn candidate_order(
    config: &ChiaPeerConfig,
    mut discovered: Vec<SocketAddr>,
) -> Vec<SocketAddr> {
    if let Some(endpoint) = config.endpoint {
        return order_candidates(&[endpoint]);
    }
    discovered.shuffle(&mut rand::thread_rng());
    discovered.dedup();
    order_candidates(&discovered)
}

/// Resolves the network's DNS introducers into candidate peer addresses.
async fn discover(config: &ChiaPeerConfig) -> Vec<SocketAddr> {
    let network = match config.network {
        ChiaNetwork::Mainnet => Network::default_mainnet(),
        ChiaNetwork::Testnet11 => Network::default_testnet11(),
    };
    network
        .lookup_all(config.connect_timeout, DISCOVERY_BATCH)
        .await
}

/// Connects to the first reachable full node, returning the SDK [`Peer`] and its inbound message
/// receiver (the drive-loop's input).
pub(crate) async fn connect(
    config: &ChiaPeerConfig,
    tls: &Connector,
) -> Result<(Peer, mpsc::Receiver<Message>), ChiaPeerError> {
    let discovered = if config.endpoint.is_some() {
        Vec::new()
    } else {
        discover(config).await
    };
    let candidates = candidate_order(config, discovered);
    if candidates.is_empty() {
        return Err(ChiaPeerError::PeerDiscoveryFailed);
    }

    let network_id = config.network.network_id().to_string();
    for addr in candidates {
        let attempt = tokio::time::timeout(
            config.connect_timeout,
            connect_peer(
                network_id.clone(),
                tls.clone(),
                addr,
                PeerOptions::default(),
            ),
        )
        .await;
        match attempt {
            Ok(Ok(connected)) => return Ok(connected),
            Ok(Err(e)) => log::debug!("connect to {addr} failed: {e}"),
            Err(_) => log::debug!("connect to {addr} timed out"),
        }
    }
    Err(ChiaPeerError::PeerDiscoveryFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn explicit_endpoint_is_the_only_candidate() {
        let endpoint = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let config = ChiaPeerConfig::mainnet().with_trusted_endpoint(endpoint);
        assert_eq!(candidate_order(&config, vec![]), vec![endpoint]);
    }

    #[test]
    fn discovered_candidates_are_ordered_ipv6_first() {
        let config = ChiaPeerConfig::mainnet();
        let v4 = SocketAddr::new(Ipv4Addr::new(1, 2, 3, 4).into(), 8444);
        let v6 = SocketAddr::new(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).into(), 8444);
        let ordered = candidate_order(&config, vec![v4, v6]);
        assert!(
            ordered[0].is_ipv6(),
            "IPv6 must be dialed first: {ordered:?}"
        );
    }

    #[test]
    fn ephemeral_connector_is_built_without_cert_paths() {
        let config = ChiaPeerConfig::mainnet();
        assert!(build_connector(&config).is_ok());
    }
}
