//! [`ChiaPeerError`] — the crate's own transport/protocol error, and its fail-closed mapping onto
//! the canonical [`dig_chainsource_interface::ChainSourceError`].
//!
//! Every variant means the same thing to a consumer: **the peer could not reliably answer**. None of
//! them is ever collapsed into an absence (`Ok(None)`) — that distinction is the crux of the
//! [`ChainSource`](dig_chainsource_interface::ChainSource) fail-closed contract, so the mapping below
//! turns every `ChiaPeerError` into an `Err`, never a value.

use dig_chainsource_interface::ChainSourceError;
use thiserror::Error;

/// The reason a Chia light-client peer read could not complete reliably.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ChiaPeerError {
    /// A transport/connection failure reaching the peer (socket, websocket, TLS). Carries the
    /// backend's own message for diagnostics.
    #[error("chia peer transport error: {0}")]
    Transport(String),

    /// The peer explicitly rejected the request (e.g. a reorg or subscription-limit rejection). The
    /// answer is unknown, so the consumer must fail closed.
    #[error("chia peer rejected the request: {0}")]
    Rejected(String),

    /// The peer responded but the payload could not be parsed into the expected chain type. The read
    /// is untrustworthy, so fail closed.
    #[error("malformed chia peer response: {0}")]
    Malformed(String),

    /// A request did not complete within the configured deadline. Whether the answer would have been
    /// present is unknown, so fail closed.
    #[error("chia peer request timed out")]
    Timeout,

    /// No usable full-node peer could be discovered/connected.
    #[error("chia peer discovery failed")]
    PeerDiscoveryFailed,

    /// The light client is not currently connected to any peer.
    #[error("chia light client is not connected")]
    NotConnected,

    /// Setting up the TLS connector failed.
    #[error("chia peer TLS setup error: {0}")]
    Tls(String),
}

impl ChiaPeerError {
    /// Whether this error's message names a timeout, so a timed-out transport string classifies as
    /// [`ChainSourceError::Timeout`] rather than a generic transport failure.
    fn looks_like_timeout(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("timed out") || lower.contains("timeout")
    }
}

impl From<ChiaPeerError> for ChainSourceError {
    /// Maps every peer error to a fail-closed [`ChainSourceError`]. Each variant is a "could not
    /// reliably answer" signal — NEVER an `Ok(None)`; only the reason class differs, for diagnostics.
    fn from(error: ChiaPeerError) -> Self {
        match error {
            ChiaPeerError::Timeout => ChainSourceError::Timeout,
            ChiaPeerError::Transport(msg) if ChiaPeerError::looks_like_timeout(&msg) => {
                ChainSourceError::Timeout
            }
            ChiaPeerError::Transport(msg) => ChainSourceError::Transport(msg),
            ChiaPeerError::Rejected(msg) => ChainSourceError::Transport(msg),
            ChiaPeerError::Malformed(msg) => ChainSourceError::Malformed(msg),
            ChiaPeerError::PeerDiscoveryFailed => {
                ChainSourceError::Transport("peer discovery failed".to_string())
            }
            ChiaPeerError::NotConnected => {
                ChainSourceError::Transport("light client is not connected".to_string())
            }
            ChiaPeerError::Tls(msg) => ChainSourceError::Transport(format!("TLS: {msg}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_variant_maps_to_timeout() {
        assert_eq!(
            ChainSourceError::from(ChiaPeerError::Timeout),
            ChainSourceError::Timeout
        );
    }

    #[test]
    fn timed_out_transport_message_maps_to_timeout() {
        let mapped = ChainSourceError::from(ChiaPeerError::Transport("request timed out".into()));
        assert_eq!(mapped, ChainSourceError::Timeout);
    }

    #[test]
    fn plain_transport_maps_to_transport_never_absence() {
        let mapped = ChainSourceError::from(ChiaPeerError::Transport("socket reset".into()));
        assert_eq!(mapped, ChainSourceError::Transport("socket reset".into()));
    }

    #[test]
    fn rejected_maps_to_transport() {
        let mapped = ChainSourceError::from(ChiaPeerError::Rejected("reorg".into()));
        assert!(matches!(mapped, ChainSourceError::Transport(_)));
    }

    #[test]
    fn malformed_maps_to_malformed() {
        let mapped = ChainSourceError::from(ChiaPeerError::Malformed("bad bytes".into()));
        assert!(matches!(mapped, ChainSourceError::Malformed(_)));
    }

    #[test]
    fn not_connected_and_discovery_and_tls_map_to_transport() {
        assert!(matches!(
            ChainSourceError::from(ChiaPeerError::NotConnected),
            ChainSourceError::Transport(_)
        ));
        assert!(matches!(
            ChainSourceError::from(ChiaPeerError::PeerDiscoveryFailed),
            ChainSourceError::Transport(_)
        ));
        assert!(matches!(
            ChainSourceError::from(ChiaPeerError::Tls("x".into())),
            ChainSourceError::Transport(_)
        ));
    }
}
