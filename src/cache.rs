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

/// Max cached coins admitted per subscribed puzzle hash, bounding the memory a puzzle-hash
/// subscription can pull in from an untrusted peer's discovery stream.
const MAX_COINS_PER_PUZZLE_HASH: usize = 10_000;

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

    /// Applies a `CoinStateUpdate` from an UNTRUSTED peer: rolls the cache back across the reported
    /// `fork_height`, then inserts the update's `items` that belong to the subscribed set (bounded),
    /// and advances the peak.
    ///
    /// Only items in the subscribed set are accepted: an item whose coin id is a subscribed coin OR
    /// whose puzzle hash is a subscribed puzzle hash (the latter preserves puzzle-hash-subscription
    /// DISCOVERY — a new coin under a watched puzzle hash is legitimate). An unsolicited item
    /// matching neither is DROPPED, so a hostile peer cannot inject coins that later answer a
    /// cache-first read. Inserts are further bounded by [`max_cached_coins`](Self::max_cached_coins)
    /// so a puzzle-hash subscription cannot be used to exhaust memory.
    ///
    /// The peak ADVANCES on a normal update, and is set DOWN ONLY for a GENUINE authoritative reorg —
    /// one where the rollback actually changed subscribed state, `fork_height` is below the current
    /// peak, and the update is well-formed (`height >= fork_height`). An empty/garbage update that
    /// rolls back nothing, or one with `height < fork_height`, cannot lower the peak.
    ///
    /// See the module docs for the reorg-rollback rules.
    pub fn apply_update(
        &mut self,
        items: &[CoinState],
        height: u32,
        fork_height: u32,
        peak_hash: Bytes32,
    ) {
        let reasserted: HashSet<Bytes32> = items
            .iter()
            .filter(|s| self.is_subscribed(s))
            .map(|s| s.coin.coin_id())
            .collect();

        // Track whether the rollback ACTUALLY changed subscribed state — peak-down is gated on this
        // so it can never fire with a weaker precondition than the rollback itself.
        let mut rolled_back = false;
        self.coins.retain(|id, state| {
            if reasserted.contains(id) {
                return true; // overwritten below with the authoritative state
            }
            if state.created_height.is_some_and(|h| h > fork_height) {
                rolled_back = true;
                return false; // created in a block the reorg erased and not re-asserted
            }
            if state.spent_height.is_some_and(|h| h > fork_height) {
                state.spent_height = None; // its spend was rolled back — unspend it
                rolled_back = true;
            }
            true
        });

        // A peak-down is honoured ONLY for a GENUINE authoritative reorg: the rollback above changed
        // real subscribed state, the fork is below the current peak, AND the update is well-formed
        // (a real reorg tip is never below its own fork point). Otherwise the peak stays advance-only,
        // so a hostile empty/garbage update cannot pin the peak arbitrarily low.
        let is_genuine_reorg = rolled_back
            && height >= fork_height
            && self.peak.is_some_and(|(current, _)| fork_height < current);

        let max_coins = self.max_cached_coins();
        for state in items {
            if !self.is_subscribed(state) {
                continue; // drop unsolicited coins from a hostile/noisy peer
            }
            // On an authoritative reorg, refuse a coin claimed to be created ABOVE the new tip — it
            // cannot exist on the post-reorg chain, and caching it would break the
            // "no cached coin created above the peak" invariant.
            if is_genuine_reorg && state.created_height.is_some_and(|h| h > height) {
                continue;
            }
            let id = state.coin.coin_id();
            // Re-asserting an already-cached coin never grows the map; a NEW coin is admitted only
            // under the cap.
            if !self.coins.contains_key(&id) && self.coins.len() >= max_coins {
                log::warn!("chia-peer cache at cap ({max_coins}); dropping overflow coin state");
                continue;
            }
            self.coins.insert(id, *state);
        }

        if is_genuine_reorg {
            self.peak = Some((height, peak_hash));
        } else {
            // Advance-only (matching the bare-`NewPeakWallet` path): a normal forward update advances
            // the peak; a stale/empty/malformed update never regresses it.
            self.set_peak(height, peak_hash);
        }
    }

    /// Whether `state` belongs to the subscribed set: its coin id is subscribed, or its puzzle hash
    /// is a subscribed puzzle hash (discovery).
    fn is_subscribed(&self, state: &CoinState) -> bool {
        self.subscribed_coins.contains(&state.coin.coin_id())
            || self
                .subscribed_puzzle_hashes
                .contains(&state.coin.puzzle_hash)
    }

    /// The upper bound on cached coins: one per subscribed coin plus a per-puzzle-hash allowance for
    /// discovery. Bounds memory an untrusted peer can pull in via a puzzle-hash subscription.
    fn max_cached_coins(&self) -> usize {
        self.subscribed_coins.len().saturating_add(
            self.subscribed_puzzle_hashes
                .len()
                .saturating_mul(MAX_COINS_PER_PUZZLE_HASH),
        )
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
        cache.track_coins([x_id]); // X is subscribed, so its re-assertion is accepted
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
        // This is an authoritative reorg (fork 89 < peak 100), so the peak is set DOWN to 90.
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
        cache.track_coins([old_id, fresh_id]); // both subscribed
        cache.apply_update(&[fresh], 200, 199, Bytes32::new([0xcc; 32]));

        assert!(
            cache.get(old_id).is_some(),
            "coin below the fork is retained"
        );
        assert!(cache.get(fresh_id).is_some(), "new coin is inserted");
    }

    #[test]
    fn unsolicited_update_item_is_dropped_not_cached() {
        let mut cache = CoinStateCache::new();
        // Nothing is subscribed. A hostile peer streams an unsolicited coin.
        let injected = state(9, Some(10), None);
        let injected_id = injected.coin.coin_id();
        cache.apply_update(&[injected], 10, 9, Bytes32::new([1; 32]));
        assert_eq!(
            cache.get(injected_id),
            None,
            "an unsubscribed coin must never be cached (nor served on a read)"
        );
    }

    #[test]
    fn update_item_matching_a_subscribed_puzzle_hash_is_accepted() {
        let mut cache = CoinStateCache::new();
        let watched = state(4, Some(20), None);
        let ph = watched.coin.puzzle_hash;
        cache.track_puzzle_hashes([ph]);
        // A freshly-discovered coin under the watched puzzle hash is legitimate.
        cache.apply_update(&[watched], 20, 19, Bytes32::new([2; 32]));
        assert_eq!(
            cache.get(watched.coin.coin_id()),
            Some(watched),
            "a coin discovered under a subscribed puzzle hash is accepted"
        );
    }

    // ---- #1311: peak-down on an authoritative reorg, advance-only otherwise ----

    /// A GENUINE authoritative reorg — one that actually drops a subscribed coin created above the
    /// fork — LOWERS the peak to the update's height/hash, so confirmation counts do not overstate.
    #[test]
    fn genuine_reorg_rollback_lowers_peak() {
        let mut cache = CoinStateCache::new();
        let orphaned = state(1, Some(95), None); // created above the fork
        cache.track_coins([orphaned.coin.coin_id()]);
        cache.seed([orphaned]);
        cache.set_peak(100, Bytes32::new([0xaa; 32]));

        // fork 90 < peak 100, height 92 >= fork, and the rollback drops the coin created at 95.
        cache.apply_update(&[], 92, 90, Bytes32::new([0xbb; 32]));
        assert_eq!(cache.peak(), Some((92, Bytes32::new([0xbb; 32]))));
    }

    /// Alias kept for the historical name: an authoritative reorg lowers the peak (same as
    /// [`genuine_reorg_rollback_lowers_peak`]).
    #[test]
    fn authoritative_reorg_lowers_peak() {
        let mut cache = CoinStateCache::new();
        let orphaned = state(2, Some(95), None);
        cache.track_coins([orphaned.coin.coin_id()]);
        cache.seed([orphaned]);
        cache.set_peak(100, Bytes32::new([0xaa; 32]));
        cache.apply_update(&[], 90, 89, Bytes32::new([0xbb; 32]));
        assert_eq!(cache.peak(), Some((90, Bytes32::new([0xbb; 32]))));
    }

    /// A `CoinStateUpdate` that rolls back NOTHING (empty items, no subscribed coin above the fork)
    /// must NOT lower the peak, even though `fork_height < peak` — a hostile empty update cannot pin
    /// the peak arbitrarily low.
    #[test]
    fn empty_update_with_low_fork_does_not_lower_peak() {
        let mut cache = CoinStateCache::new();
        cache.set_peak(1_000_000, Bytes32::new([0xaa; 32]));
        cache.apply_update(&[], 7, 5, Bytes32::new([0xbb; 32]));
        assert_eq!(cache.peak(), Some((1_000_000, Bytes32::new([0xaa; 32]))));
    }

    /// A malformed reorg whose `height < fork_height` (impossible on a real chain) must NOT lower the
    /// peak, even if the rollback changed state.
    #[test]
    fn peak_height_below_fork_height_is_rejected() {
        let mut cache = CoinStateCache::new();
        let orphaned = state(3, Some(60), None); // created above the (malformed) fork
        cache.track_coins([orphaned.coin.coin_id()]);
        cache.seed([orphaned]);
        cache.set_peak(100, Bytes32::new([0xaa; 32]));

        // height 40 < fork 50 → malformed; rollback may drop the coin but the peak must not lower.
        cache.apply_update(&[], 40, 50, Bytes32::new([0xbb; 32]));
        assert_eq!(cache.peak(), Some((100, Bytes32::new([0xaa; 32]))));
    }

    /// A bare `NewPeakWallet` (the `set_peak` path) with a LOWER height must NOT lower the peak —
    /// this is the hostile-low-peak resistance and stays advance-only.
    #[test]
    fn bare_new_peak_lower_does_not_lower_peak() {
        let mut cache = CoinStateCache::new();
        cache.set_peak(100, Bytes32::new([0xaa; 32]));
        cache.set_peak(90, Bytes32::new([0xbb; 32])); // bare NewPeakWallet, lower
        assert_eq!(cache.peak(), Some((100, Bytes32::new([0xaa; 32]))));
    }

    /// A normal forward `CoinStateUpdate` (no rollback: `fork_height` at/above the current peak)
    /// still advances the peak.
    #[test]
    fn normal_forward_update_advances_peak() {
        let mut cache = CoinStateCache::new();
        cache.set_peak(100, Bytes32::new([0xaa; 32]));
        cache.apply_update(&[], 101, 100, Bytes32::new([0xcc; 32]));
        assert_eq!(cache.peak(), Some((101, Bytes32::new([0xcc; 32]))));
    }

    /// Invariant: after an authoritative peak-down, no cached coin has a `created_height` above the
    /// new peak — coins created above the fork are dropped, and an item claimed above the new tip is
    /// refused.
    #[test]
    fn invariant_no_cached_coin_created_above_peak_after_reorg() {
        let mut cache = CoinStateCache::new();
        let below = state(1, Some(50), None); // survives the reorg
        let orphaned = state(2, Some(95), None); // created above the fork → dropped
        cache.track_coins([below.coin.coin_id(), orphaned.coin.coin_id()]);
        cache.seed([below, orphaned]);
        cache.set_peak(100, Bytes32::new([0xaa; 32]));

        // A hostile item claims to be created ABOVE the new tip (99 > 92); it must be refused.
        let above_tip = state(3, Some(99), None);
        let above_tip_id = above_tip.coin.coin_id();
        cache.track_coins([above_tip_id]);
        cache.apply_update(&[above_tip], 92, 90, Bytes32::new([0xbb; 32]));

        let (peak_height, _) = cache.peak().expect("peak set");
        assert_eq!(peak_height, 92);
        assert_eq!(cache.get(above_tip_id), None, "above-tip coin is refused");
        for id in [below.coin.coin_id(), orphaned.coin.coin_id(), above_tip_id] {
            if let Some(cs) = cache.get(id) {
                assert!(
                    cs.created_height.is_none_or(|h| h <= peak_height),
                    "cached coin {id:?} created above peak {peak_height}"
                );
            }
        }
    }
}
