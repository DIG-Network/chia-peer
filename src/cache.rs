//! [`CoinStateCache`] — the local mirror of the coin/puzzle-hash state a light client has subscribed
//! to, kept current by the drive-loop from the peer's `CoinStateUpdate` stream and consulted first by
//! reads.
//!
//! ## Reorg handling (the subtle part)
//!
//! A `CoinStateUpdate` carries a `fork_height`: the last block the new peak still agrees with. Any
//! coin state the cache learned *above* that fork is now suspect:
//! - a coin **created** above the fork that the update does not re-assert no longer exists → drop it;
//! - a coin **spent** above the fork had its spend rolled back → clear its spent height (the coin is
//!   unspent again) unless the update says otherwise.
//!
//! The update's own `items` are authoritative for every coin they mention, so they overwrite the
//! cache last. This keeps a read after a reorg from returning a coin state that the reorg erased.

use std::collections::{HashMap, HashSet};

use chia_protocol::{Bytes32, CoinState};

/// A light client's local view of subscribed coin/puzzle-hash state plus the current peak.
#[derive(Debug, Default)]
pub struct CoinStateCache {
    /// Latest known state of every cached coin, keyed by coin id.
    coins: HashMap<Bytes32, CoinState>,
    /// Coin ids the client has an active subscription for.
    subscribed_coins: HashSet<Bytes32>,
    /// Puzzle hashes the client has an active subscription for.
    subscribed_puzzle_hashes: HashSet<Bytes32>,
    /// The current peak `(height, header_hash)` learned from `NewPeakWallet`/`CoinStateUpdate`.
    peak: Option<(u32, Bytes32)>,
}

impl CoinStateCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current peak `(height, header_hash)`, if one has been observed.
    pub fn peak(&self) -> Option<(u32, Bytes32)> {
        self.peak
    }

    /// Records a new peak observed from a `NewPeakWallet` message.
    pub fn set_peak(&mut self, height: u32, header_hash: Bytes32) {
        // Only advance the peak; a stale/out-of-order lower peak never regresses a higher one.
        if self.peak.is_none_or(|(h, _)| height >= h) {
            self.peak = Some((height, header_hash));
        }
    }

    /// The cached state of `coin_id`, if the client holds one.
    pub fn get(&self, coin_id: Bytes32) -> Option<CoinState> {
        self.coins.get(&coin_id).cloned()
    }

    /// Seeds the cache with the coin states returned by an initial subscribe response, overwriting
    /// any prior state for the same coin ids.
    pub fn seed(&mut self, states: impl IntoIterator<Item = CoinState>) {
        for state in states {
            self.coins.insert(state.coin.coin_id(), state);
        }
    }

    /// Records that `coin_ids` are now subscribed.
    pub fn track_coins(&mut self, coin_ids: impl IntoIterator<Item = Bytes32>) {
        self.subscribed_coins.extend(coin_ids);
    }

    /// Records that `puzzle_hashes` are now subscribed.
    pub fn track_puzzle_hashes(&mut self, puzzle_hashes: impl IntoIterator<Item = Bytes32>) {
        self.subscribed_puzzle_hashes.extend(puzzle_hashes);
    }

    /// Drops `coin_ids` from the subscription set (their cached state is retained until overwritten).
    pub fn untrack_coins(&mut self, coin_ids: &[Bytes32]) {
        for id in coin_ids {
            self.subscribed_coins.remove(id);
        }
    }

    /// Whether `coin_id` is currently subscribed.
    pub fn is_subscribed_coin(&self, coin_id: Bytes32) -> bool {
        self.subscribed_coins.contains(&coin_id)
    }

    /// The set of currently-subscribed coin ids (used to re-arm subscriptions after a reconnect).
    pub fn subscribed_coins(&self) -> Vec<Bytes32> {
        self.subscribed_coins.iter().copied().collect()
    }

    /// The set of currently-subscribed puzzle hashes (used to re-arm after a reconnect).
    pub fn subscribed_puzzle_hashes(&self) -> Vec<Bytes32> {
        self.subscribed_puzzle_hashes.iter().copied().collect()
    }

    /// Applies a `CoinStateUpdate`: rolls the cache back across the reported `fork_height`, then
    /// overwrites with the update's authoritative `items`, and advances the peak.
    ///
    /// See the module docs for the reorg-rollback rules.
    pub fn apply_update(
        &mut self,
        items: &[CoinState],
        height: u32,
        fork_height: u32,
        peak_hash: Bytes32,
    ) {
        let reasserted: HashSet<Bytes32> = items.iter().map(|s| s.coin.coin_id()).collect();

        self.coins.retain(|id, state| {
            if reasserted.contains(id) {
                return true; // overwritten below with the authoritative state
            }
            if state.created_height.is_some_and(|h| h > fork_height) {
                return false; // created in a block the reorg erased and not re-asserted
            }
            if state.spent_height.is_some_and(|h| h > fork_height) {
                state.spent_height = None; // its spend was rolled back — unspend it
            }
            true
        });

        for state in items {
            self.coins.insert(state.coin.coin_id(), *state);
        }

        // A reorg's new peak is authoritative even if numerically lower than the old one.
        self.peak = Some((height, peak_hash));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chia_protocol::Coin;

    fn coin(seed: u8, amount: u64) -> Coin {
        Coin::new(
            Bytes32::new([seed; 32]),
            Bytes32::new([seed ^ 0xff; 32]),
            amount,
        )
    }

    fn state(seed: u8, created: Option<u32>, spent: Option<u32>) -> CoinState {
        CoinState {
            coin: coin(seed, 1),
            created_height: created,
            spent_height: spent,
        }
    }

    #[test]
    fn seed_then_get_returns_state() {
        let mut cache = CoinStateCache::new();
        let s = state(1, Some(100), None);
        let id = s.coin.coin_id();
        cache.seed([s]);
        assert_eq!(cache.get(id), Some(s));
    }

    #[test]
    fn peak_only_advances() {
        let mut cache = CoinStateCache::new();
        cache.set_peak(100, Bytes32::new([1; 32]));
        cache.set_peak(90, Bytes32::new([2; 32])); // stale lower peak
        assert_eq!(cache.peak().map(|(h, _)| h), Some(100));
    }

    #[test]
    fn subscription_tracking_roundtrips() {
        let mut cache = CoinStateCache::new();
        let id = coin(5, 1).coin_id();
        cache.track_coins([id]);
        assert!(cache.is_subscribed_coin(id));
        assert_eq!(cache.subscribed_coins(), vec![id]);
        cache.untrack_coins(&[id]);
        assert!(!cache.is_subscribed_coin(id));
        cache.track_puzzle_hashes([Bytes32::new([7; 32])]);
        assert_eq!(cache.subscribed_puzzle_hashes().len(), 1);
    }

    /// Test #3 (the reorg crux): a `CoinStateUpdate` at a forked, numerically-lower peak overwrites
    /// re-asserted coins, drops coins created above the fork, and un-spends coins spent above it.
    #[test]
    fn reorg_update_rolls_cache_back_across_fork() {
        let mut cache = CoinStateCache::new();

        // X exists in the pre-reorg chain, spent at 92.
        let x_pre = state(1, Some(80), Some(92));
        let x_id = x_pre.coin.coin_id();
        // Y was created at 95 (above the fork) and is not re-asserted by the update.
        let y_pre = state(2, Some(95), None);
        let y_id = y_pre.coin.coin_id();
        // Z was created at 50 but SPENT at 93 (above the fork).
        let z_pre = state(3, Some(50), Some(93));
        let z_id = z_pre.coin.coin_id();
        cache.seed([x_pre, y_pre, z_pre]);
        cache.set_peak(100, Bytes32::new([0xaa; 32]));

        // Reorg to a lower peak (90), fork point 89. The update re-asserts X as UNSPENT.
        let x_post = state(1, Some(80), None);
        cache.apply_update(&[x_post], 90, 89, Bytes32::new([0xbb; 32]));

        // X was overwritten with the authoritative (unspent) state.
        assert_eq!(cache.get(x_id), Some(x_post));
        // Y (created above the fork, not re-asserted) was dropped.
        assert_eq!(cache.get(y_id), None);
        // Z stays but its rolled-back spend is cleared.
        assert_eq!(cache.get(z_id).and_then(|s| s.spent_height), None);
        // The peak follows the reorg even though it is numerically lower.
        assert_eq!(cache.peak(), Some((90, Bytes32::new([0xbb; 32]))));
    }

    #[test]
    fn normal_update_inserts_without_dropping_lower_coins() {
        let mut cache = CoinStateCache::new();
        let old = state(1, Some(10), None);
        let old_id = old.coin.coin_id();
        cache.seed([old]);

        let fresh = state(2, Some(200), None);
        let fresh_id = fresh.coin.coin_id();
        cache.apply_update(&[fresh], 200, 199, Bytes32::new([0xcc; 32]));

        assert!(
            cache.get(old_id).is_some(),
            "coin below the fork is retained"
        );
        assert!(cache.get(fresh_id).is_some(), "new coin is inserted");
    }
}
