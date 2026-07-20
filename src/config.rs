//! [`ChiaPeerConfig`] — how a [`ChiaLightClient`](crate::ChiaLightClient) is pointed at a Chia
//! network and, optionally, at the operator's own trusted full node.

use std::net::SocketAddr;
use std::time::Duration;

use chia_protocol::Bytes32;
use chia_wallet_sdk::types::{MAINNET_CONSTANTS, TESTNET11_CONSTANTS};

/// The Chia network a client speaks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChiaNetwork {
    /// Chia mainnet.
    Mainnet,
    /// The current public testnet (testnet11).
    Testnet11,
}

const MAINNET_PORT: u16 = 8444;
const TESTNET11_PORT: u16 = 58444;

impl ChiaNetwork {
    /// The wallet-protocol `network_id` handshake string for this network.
    pub fn network_id(self) -> &'static str {
        match self {
            ChiaNetwork::Mainnet => "mainnet",
            ChiaNetwork::Testnet11 => "testnet11",
        }
    }

    /// The default full-node port peers listen on for this network.
    pub fn default_port(self) -> u16 {
        match self {
            ChiaNetwork::Mainnet => MAINNET_PORT,
            ChiaNetwork::Testnet11 => TESTNET11_PORT,
        }
    }

    /// The genesis challenge, used as the `header_hash` when querying coin state from height 0
    /// (`Bytes32::default()` is rejected by the peer protocol).
    pub fn genesis_challenge(self) -> Bytes32 {
        match self {
            ChiaNetwork::Mainnet => MAINNET_CONSTANTS.genesis_challenge,
            ChiaNetwork::Testnet11 => TESTNET11_CONSTANTS.genesis_challenge,
        }
    }
}

/// Connection + trust configuration for a [`ChiaLightClient`](crate::ChiaLightClient).
///
/// When [`endpoint`](Self::endpoint) names the operator's own node (`trusted == true`), the derived
/// provider is a [`LocalNode`](dig_chainsource_interface::ProviderKind::LocalNode); otherwise peers
/// are discovered from the network's DNS introducers and the provider is
/// [`Custom`](dig_chainsource_interface::ProviderKind::Custom).
#[derive(Debug, Clone)]
pub struct ChiaPeerConfig {
    /// The network to connect to.
    pub network: ChiaNetwork,
    /// An explicit peer to dial (the operator's own node). `None` = discover via DNS introducers.
    pub endpoint: Option<SocketAddr>,
    /// Whether [`endpoint`](Self::endpoint) is the operator's own trusted node. Governs the derived
    /// [`ProviderKind`](dig_chainsource_interface::ProviderKind).
    pub trusted: bool,
    /// Per-attempt connection timeout.
    pub connect_timeout: Duration,
    /// Per-request response timeout.
    pub request_timeout: Duration,
    /// Filesystem path to the client TLS certificate (PEM).
    pub tls_cert_path: Option<String>,
    /// Filesystem path to the client TLS key (PEM).
    pub tls_key_path: Option<String>,
}

impl ChiaPeerConfig {
    /// A mainnet config that discovers public peers via DNS introducers.
    pub fn mainnet() -> Self {
        Self::discovering(ChiaNetwork::Mainnet)
    }

    /// A testnet11 config that discovers public peers via DNS introducers.
    pub fn testnet11() -> Self {
        Self::discovering(ChiaNetwork::Testnet11)
    }

    /// A config that discovers untrusted public peers for `network` via DNS introducers.
    pub fn discovering(network: ChiaNetwork) -> Self {
        Self {
            network,
            endpoint: None,
            trusted: false,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
            tls_cert_path: None,
            tls_key_path: None,
        }
    }

    /// Points the client at the operator's OWN trusted node at `endpoint` (e.g. a local full node),
    /// deriving a [`LocalNode`](dig_chainsource_interface::ProviderKind::LocalNode) provider.
    pub fn with_trusted_endpoint(mut self, endpoint: SocketAddr) -> Self {
        self.endpoint = Some(endpoint);
        self.trusted = true;
        self
    }

    /// Sets the client TLS certificate + key paths (PEM).
    pub fn with_tls(mut self, cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        self.tls_cert_path = Some(cert_path.into());
        self.tls_key_path = Some(key_path.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn network_ids_and_ports_are_canonical() {
        assert_eq!(ChiaNetwork::Mainnet.network_id(), "mainnet");
        assert_eq!(ChiaNetwork::Testnet11.network_id(), "testnet11");
        assert_eq!(ChiaNetwork::Mainnet.default_port(), 8444);
        assert_eq!(ChiaNetwork::Testnet11.default_port(), 58444);
    }

    #[test]
    fn genesis_challenge_differs_per_network() {
        assert_ne!(
            ChiaNetwork::Mainnet.genesis_challenge(),
            ChiaNetwork::Testnet11.genesis_challenge()
        );
    }

    #[test]
    fn discovering_config_is_untrusted_with_no_endpoint() {
        let cfg = ChiaPeerConfig::mainnet();
        assert!(!cfg.trusted);
        assert!(cfg.endpoint.is_none());
    }

    #[test]
    fn trusted_endpoint_marks_config_trusted() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8444);
        let cfg = ChiaPeerConfig::testnet11().with_trusted_endpoint(addr);
        assert!(cfg.trusted);
        assert_eq!(cfg.endpoint, Some(addr));
    }

    #[test]
    fn with_tls_sets_both_paths() {
        let cfg = ChiaPeerConfig::mainnet().with_tls("cert.pem", "key.pem");
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("cert.pem"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("key.pem"));
    }
}
