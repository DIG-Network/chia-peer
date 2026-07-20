//! IPv6-first peer-candidate ordering (CLAUDE.md §5.2).
//!
//! All peer-to-peer networking is IPv6-first, with IPv4 used only as a fallback when IPv6 is
//! unreachable. The SDK's [`Network::lookup_all`](chia_wallet_sdk::client::Network::lookup_all)
//! returns a mixed list of A/AAAA addresses in DNS order; [`order_candidates`] rearranges it so
//! every IPv6 candidate is dialed before any IPv4 candidate, and the IPv6 loopback (`::1`) is tried
//! before the IPv4 loopback (`127.0.0.1`).
//!
//! Ordering is a **stable partition** — it never drops or duplicates a candidate, so IPv4 remains a
//! full fallback. Randomisation (so we do not always hammer the same peer) is applied by the caller
//! *before* ordering, keeping this function deterministic and unit-testable.

use std::net::SocketAddr;

/// Returns `candidates` reordered IPv6-first, IPv4-last, with loopback addresses tried first within
/// each family. The relative order of non-loopback addresses within a family is preserved, so a
/// caller may shuffle the input to spread load and still get IPv6-before-IPv4 ordering.
pub fn order_candidates(candidates: &[SocketAddr]) -> Vec<SocketAddr> {
    let mut v6_loopback = Vec::new();
    let mut v6_rest = Vec::new();
    let mut v4_loopback = Vec::new();
    let mut v4_rest = Vec::new();

    for &addr in candidates {
        match addr {
            SocketAddr::V6(_) if addr.ip().is_loopback() => v6_loopback.push(addr),
            SocketAddr::V6(_) => v6_rest.push(addr),
            SocketAddr::V4(_) if addr.ip().is_loopback() => v4_loopback.push(addr),
            SocketAddr::V4(_) => v4_rest.push(addr),
        }
    }

    let mut ordered = Vec::with_capacity(candidates.len());
    ordered.extend(v6_loopback);
    ordered.extend(v6_rest);
    ordered.extend(v4_loopback);
    ordered.extend(v4_rest);
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn v4(a: u8, b: u8, c: u8, d: u8) -> SocketAddr {
        SocketAddr::new(Ipv4Addr::new(a, b, c, d).into(), 8444)
    }

    fn v6(seg: u16) -> SocketAddr {
        SocketAddr::new(
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, seg).into(),
            8444,
        )
    }

    #[test]
    fn all_ipv6_come_before_any_ipv4() {
        let mixed = vec![v4(1, 1, 1, 1), v6(1), v4(2, 2, 2, 2), v6(2)];
        let ordered = order_candidates(&mixed);

        let first_v4 = ordered.iter().position(|a| a.is_ipv4()).unwrap();
        let last_v6 = ordered.iter().rposition(|a| a.is_ipv6()).unwrap();
        assert!(
            last_v6 < first_v4,
            "every IPv6 must precede every IPv4: {ordered:?}"
        );
    }

    #[test]
    fn ipv6_loopback_precedes_ipv4_loopback() {
        let v6_lo = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let v4_lo = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 8444);
        let ordered = order_candidates(&[v4_lo, v6_lo]);
        assert_eq!(ordered, vec![v6_lo, v4_lo]);
    }

    #[test]
    fn loopback_precedes_public_within_family() {
        let v6_lo = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 8444);
        let ordered = order_candidates(&[v6(9), v6_lo]);
        assert_eq!(
            ordered[0], v6_lo,
            "loopback should be tried first: {ordered:?}"
        );
    }

    #[test]
    fn ordering_preserves_every_candidate() {
        let mixed = vec![v4(1, 1, 1, 1), v6(1), v4(2, 2, 2, 2), v6(2), v6(3)];
        let ordered = order_candidates(&mixed);
        assert_eq!(ordered.len(), mixed.len());
        for addr in &mixed {
            assert!(ordered.contains(addr), "{addr} was dropped");
        }
    }

    #[test]
    fn ipv4_only_input_is_returned_as_fallback() {
        let only_v4 = vec![v4(1, 1, 1, 1), v4(2, 2, 2, 2)];
        let ordered = order_candidates(&only_v4);
        assert_eq!(ordered, only_v4, "IPv4 must remain a usable fallback");
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(order_candidates(&[]).is_empty());
    }
}
