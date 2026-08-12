//! [`ChiaPeerProvider`] — a synchronous [`ChainSource`] facade over the light client's subscription
//! cache + wallet-protocol peer.
//!
//! Reads answer from the local [`CoinStateCache`](crate::cache::CoinStateCache) first; a miss falls
//! through to a **non-subscribing** peer query (so a read never silently grows the subscription set).
//! Every outcome honours the interface's fail-closed contract: `Ok(None)`/empty means the peer
//! reliably reported absence, while any transport/subscription-gap failure is an `Err` — NEVER a
//! false `Ok(None)`.
//!
//! ## Singleton lineage
//!
//! [`resolve_singleton_lineage`](ChainSource::resolve_singleton_lineage) is answered by the
//! ecosystem's ONE canonical launcher→tip walk,
//! [`resolve_singleton_lineage_via_walk`], composed purely from this provider's own
//! [`coin_record`](ChainSource::coin_record) + [`coin_spend`](ChainSource::coin_spend) reads.
//!
//! There is nothing spoofable about answering it here, because the walk never *recognises* the next
//! coin — it **derives** it, by reading the current coin's own spend, proving the reveal hashes to
//! that coin's puzzle hash, running the inner puzzle, and reconstructing the successor's full puzzle
//! hash. It deliberately never consults [`coin_records_by_parent`](ChainSource::coin_records_by_parent),
//! since picking a successor out of a source-supplied child list would let the source steer the
//! lineage. This crate therefore delegates and adds nothing: no child-list lookup, no puzzle-hash
//! shortcut, no caching of lineage results.
//!
//! ## Boundary
//!
//! A subscribing light client is not a full archival index. ONE read remains deliberately reported
//! as [`ChainSourceError::Unsupported`] rather than answered unreliably:
//! [`block_timestamp`](ChainSource::block_timestamp) — a light source keeps no timestamp index. The
//! registry composes providers, so it falls through to a source that does index timestamps.

use std::sync::Arc;

use chia_protocol::{Bytes32, CoinSpend, CoinState, CoinStateFilters, Program};
use dig_chainsource_interface::{
    resolve_singleton_lineage_via_walk, ChainSource, ChainSourceError, ChainSourceProvider,
    CoinRecord, ProviderInfo, SingletonLineage,
};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::bridge::run_blocking;
use crate::cache::CoinStateCache;
use crate::fetcher::CoinStateFetcher;

/// A [`ChainSource`] provider backed by a subscribing Chia light client.
///
/// Cloning shares the underlying cache, fetcher, and runtime handle.
#[derive(Clone)]
pub struct ChiaPeerProvider {
    fetcher: Arc<dyn CoinStateFetcher>,
    cache: Arc<RwLock<CoinStateCache>>,
    handle: Handle,
    info: ProviderInfo,
}

impl ChiaPeerProvider {
    /// Builds a provider reading through `fetcher` + `cache`, driving async reads on `handle` (which
    /// MUST belong to a multi-thread runtime — see the crate's async→sync bridge), described by
    /// `info`.
    pub fn new(
        fetcher: Arc<dyn CoinStateFetcher>,
        cache: Arc<RwLock<CoinStateCache>>,
        handle: Handle,
        info: ProviderInfo,
    ) -> Self {
        Self {
            fetcher,
            cache,
            handle,
            info,
        }
    }

    /// Resolves a coin's current state: cache first, then a non-subscribing peer read.
    fn coin_state(&self, coin_id: Bytes32) -> Result<Option<CoinState>, ChainSourceError> {
        let fetcher = self.fetcher.clone();
        let cache = self.cache.clone();
        run_blocking(&self.handle, async move {
            if let Some(cached) = cache.read().await.get(coin_id) {
                return Ok(Some(cached));
            }
            let states = fetcher.coin_states(vec![coin_id], false).await?;
            Ok::<_, crate::error::ChiaPeerError>(
                states.into_iter().find(|s| s.coin.coin_id() == coin_id),
            )
        })?
        .map_err(ChainSourceError::from)
    }
}

impl ChainSource for ChiaPeerProvider {
    type Error = ChainSourceError;

    fn coin_record(&self, coin_id: Bytes32) -> Result<Option<CoinRecord>, Self::Error> {
        let peak = self.peak_height()?;
        Ok(self
            .coin_state(coin_id)?
            .map(CoinRecord::from_coin_state)
            .map(|record| clamp_record_heights_to_peak(record, peak)))
    }

    fn coin_records_by_puzzle_hash(
        &self,
        puzzle_hash: Bytes32,
        include_spent: bool,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        let fetcher = self.fetcher.clone();
        let filters = CoinStateFilters {
            include_spent,
            include_unspent: true,
            include_hinted: true,
            min_amount: 0,
        };
        let states = run_blocking(&self.handle, async move {
            fetcher
                .puzzle_states(vec![puzzle_hash], filters, false)
                .await
        })?
        .map_err(ChainSourceError::from)?;
        let peak = self.peak_height()?;
        Ok(states
            .into_iter()
            .map(CoinRecord::from_coin_state)
            .map(|record| clamp_record_heights_to_peak(record, peak))
            .collect())
    }

    fn coin_records_by_parent(
        &self,
        parent_coin_id: Bytes32,
    ) -> Result<Vec<CoinRecord>, Self::Error> {
        let fetcher = self.fetcher.clone();
        let states = run_blocking(&self.handle, async move {
            fetcher.children(parent_coin_id).await
        })?
        .map_err(ChainSourceError::from)?;
        let peak = self.peak_height()?;
        Ok(states
            .into_iter()
            .map(CoinRecord::from_coin_state)
            .map(|record| clamp_record_heights_to_peak(record, peak))
            .collect())
    }

    fn coin_spend(&self, coin_id: Bytes32) -> Result<Option<CoinSpend>, Self::Error> {
        // The spend that spent `coin_id` exists only once the coin has a spent height; the coin
        // itself supplies the real puzzle hash the CoinSpend needs (never a placeholder).
        let Some(state) = self.coin_state(coin_id)? else {
            return Ok(None);
        };
        let Some(spent_height) = state.spent_height else {
            return Ok(None);
        };
        let fetcher = self.fetcher.clone();
        let (puzzle, solution) = run_blocking(&self.handle, async move {
            fetcher.puzzle_and_solution(coin_id, spent_height).await
        })?
        .map_err(ChainSourceError::from)?;

        // Defend against a lying peer: the reveal MUST hash to the coin's own puzzle hash, else the
        // spend is not this coin's. Fail closed on a mismatch or an unparseable reveal.
        verify_reveal_matches(&puzzle, state.coin.puzzle_hash)?;
        Ok(Some(CoinSpend::new(state.coin, puzzle, solution)))
    }

    /// Delegates to the canonical walk, which composes the answer from this provider's own
    /// `coin_record` + `coin_spend` reads (see the module docs for why derivation, not recognition,
    /// is the only sound construction). Nothing is added here — a second hand-rolled singleton
    /// authentication is a byte-drift bug on a money path.
    fn resolve_singleton_lineage(
        &self,
        launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        resolve_singleton_lineage_via_walk(self, launcher_id)
    }

    fn peak_height(&self) -> Result<Option<u32>, Self::Error> {
        let cache = self.cache.clone();
        let peak = run_blocking(&self.handle, async move { cache.read().await.peak() })?;
        Ok(peak.map(|(height, _)| height))
    }

    fn block_timestamp(&self, _height: u32) -> Result<Option<u64>, Self::Error> {
        Err(ChainSourceError::Unsupported(
            "block timestamps are not indexed by the light-client source",
        ))
    }
}

impl ChainSourceProvider for ChiaPeerProvider {
    fn provider_info(&self) -> ProviderInfo {
        self.info.clone()
    }
}

/// Bounds a record's reported block heights (`confirmed_height` and `spent_height`) by the current
/// known peak.
///
/// The cache read path already upholds "no coin has a height `> peak_height`" structurally (see
/// [`CoinStateCache`](crate::cache::CoinStateCache)), but the cache-MISS *live-fetch* path surfaces
/// the peer's heights directly. A coin created or spent in the current tip block — read in the
/// one-block window before the drive loop processes the matching `NewPeakWallet` — would otherwise
/// report a height `> peak_height`, underflowing a consumer's `peak_height - height` (u32) depth count
/// (confirmations for `confirmed_height`, spend-depth for `spent_height`) into a spurious ~4.29-billion
/// value on a money path.
///
/// Clamping each height to `min(height, peak)` makes such a coin report 0 confirmations / 0 spend-depth
/// — the conservative, understating direction — while keeping it PRESENT (never omitted) and keeping
/// `spent_height` `Some` (the coin IS spent; only the reported HEIGHT is clamped, never the
/// spent-vs-unspent flag). The peak is left untouched (a lying peer must not be able to inflate it via
/// a fetched coin). When no peak is known yet, `peak_height` is `None`, so no `peak - height`
/// subtraction is possible and the heights are left as reported.
fn clamp_record_heights_to_peak(mut record: CoinRecord, peak: Option<u32>) -> CoinRecord {
    let Some(peak) = peak else { return record };
    if let Some(confirmed) = record.confirmed_height {
        record.confirmed_height = Some(confirmed.min(peak));
    }
    if let Some(spent) = record.spent_height {
        record.spent_height = Some(spent.min(peak));
    }
    record
}

/// Verifies a puzzle reveal hashes to `expected` (the coin's own puzzle hash), failing closed on a
/// mismatch or an unparseable reveal. A lying peer cannot pass off a wrong reveal as this coin's.
fn verify_reveal_matches(puzzle: &Program, expected: Bytes32) -> Result<(), ChainSourceError> {
    let actual: Bytes32 = chia_wallet_sdk::clvm_utils::tree_hash_from_bytes(puzzle.as_ref())
        .map_err(|e| ChainSourceError::Malformed(format!("undecodable puzzle reveal: {e}")))?
        .into();
    if actual != expected {
        return Err(ChainSourceError::Malformed(
            "puzzle reveal does not hash to the coin's puzzle hash".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ChiaPeerError;
    use async_trait::async_trait;
    use chia_protocol::{Coin, Program};
    use dig_chainsource_interface::{ProviderId, ProviderKind};
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A scripted fetcher: each read returns the configured `Ok(..)` states or a forced error, so
    /// the provider's fail-closed mapping can be exercised without a live node.
    #[derive(Default, Clone)]
    struct MockFetcher {
        coin_states: Vec<CoinState>,
        fail: Option<ChiaPeerError>,
        children: Vec<CoinState>,
        puzzle_states: Vec<CoinState>,
        reveal: Option<(Program, Program)>,
        /// How many `coin_states` reads reached the network — 0 proves a read was served from cache.
        coin_state_reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CoinStateFetcher for MockFetcher {
        async fn coin_states(
            &self,
            _coin_ids: Vec<Bytes32>,
            _subscribe: bool,
        ) -> Result<Vec<CoinState>, ChiaPeerError> {
            self.coin_state_reads.fetch_add(1, Ordering::SeqCst);
            match &self.fail {
                Some(e) => Err(e.clone()),
                None => Ok(self.coin_states.clone()),
            }
        }
        async fn puzzle_states(
            &self,
            _puzzle_hashes: Vec<Bytes32>,
            _filters: CoinStateFilters,
            _subscribe: bool,
        ) -> Result<Vec<CoinState>, ChiaPeerError> {
            match &self.fail {
                Some(e) => Err(e.clone()),
                None => Ok(self.puzzle_states.clone()),
            }
        }
        async fn children(&self, _coin_id: Bytes32) -> Result<Vec<CoinState>, ChiaPeerError> {
            match &self.fail {
                Some(e) => Err(e.clone()),
                None => Ok(self.children.clone()),
            }
        }
        async fn puzzle_and_solution(
            &self,
            _coin_id: Bytes32,
            _height: u32,
        ) -> Result<(Program, Program), ChiaPeerError> {
            if let Some(e) = &self.fail {
                return Err(e.clone());
            }
            match &self.reveal {
                Some(reveal) => Ok(reveal.clone()),
                // Absence is impossible on this path (caller confirmed spent) → fail closed.
                None => Err(ChiaPeerError::Rejected("no reveal".into())),
            }
        }
    }

    /// A puzzle reveal and the coin puzzle hash it hashes to, so `coin_spend`'s reveal verification
    /// passes for a legitimately-served spend.
    fn reveal_and_matching_puzzle_hash() -> (Program, Bytes32) {
        let puzzle = Program::from(vec![1u8]);
        let ph: Bytes32 = chia_wallet_sdk::clvm_utils::tree_hash_from_bytes(puzzle.as_ref())
            .unwrap()
            .into();
        (puzzle, ph)
    }

    fn info() -> ProviderInfo {
        ProviderInfo {
            id: ProviderId(Cow::Borrowed("chia-peer-test")),
            kind: ProviderKind::Custom,
            priority: 20,
            trustless: false,
        }
    }

    fn provider_with(fetcher: MockFetcher) -> (tokio::runtime::Runtime, ChiaPeerProvider) {
        provider_with_peak(fetcher, None)
    }

    /// Builds a provider whose cache has been advanced to `peak` (if any), so the live-fetch clamp
    /// against the known peak can be exercised.
    fn provider_with_peak(
        fetcher: MockFetcher,
        peak: Option<u32>,
    ) -> (tokio::runtime::Runtime, ChiaPeerProvider) {
        let mut cache = CoinStateCache::new();
        if let Some(height) = peak {
            cache.set_peak(height, Bytes32::new([0xAB; 32]));
        }
        provider_with_cache(fetcher, cache)
    }

    /// Builds a provider over an ARBITRARY, already-populated cache.
    ///
    /// Every other builder starts from an empty cache, which leaves the cache-first branch of
    /// [`ChiaPeerProvider::coin_state`] unexercised by construction — the exact blind spot that let
    /// #2792 (a stale cache answering for a spent coin) go unnoticed. Tests that need the cache to
    /// ANSWER go through here.
    fn provider_with_cache(
        fetcher: MockFetcher,
        cache: CoinStateCache,
    ) -> (tokio::runtime::Runtime, ChiaPeerProvider) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let provider = ChiaPeerProvider::new(
            Arc::new(fetcher),
            Arc::new(RwLock::new(cache)),
            rt.handle().clone(),
            info(),
        );
        (rt, provider)
    }

    /// Runs the sync facade method off any ambient runtime (bridge's "outside a runtime" path).
    fn call<T: Send>(f: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|s| s.spawn(f).join().expect("thread panicked"))
    }

    fn coin(seed: u8) -> Coin {
        Coin::new(Bytes32::new([seed; 32]), Bytes32::new([seed ^ 1; 32]), 1)
    }

    // ---- Test #1: the fail-closed crux ----

    #[test]
    fn coin_record_returns_some_for_a_known_coin() {
        let c = coin(7);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(100),
                spent_height: None,
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let record = call(move || provider.coin_record(id)).expect("read ok");
        assert!(record.is_some());
        assert_eq!(record.unwrap().confirmed_height, Some(100));
    }

    /// #1326 regression: a cache-miss live fetch returning a coin created ABOVE the current peak (the
    /// one-block window before the matching NewPeakWallet lands) must report `confirmed_height`
    /// clamped to the peak (0 confirmations), NEVER above it — and the coin must stay PRESENT, not
    /// omitted, since it genuinely exists.
    #[test]
    fn live_fetched_coin_above_peak_reports_clamped_confirmed_height() {
        let c = coin(11);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(1_000_001), // above the peak below
                spent_height: None,
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let record = call(move || provider.coin_record(id))
            .expect("read ok")
            .expect("coin present, never omitted");
        assert_eq!(
            record.confirmed_height,
            Some(1_000_000),
            "an above-peak live coin must clamp to the peak (0 confirmations), never overstate"
        );
    }

    /// A live-fetched coin created at/below the peak keeps its real confirmation height (the clamp is
    /// a no-op on the normal path).
    #[test]
    fn live_fetched_coin_at_or_below_peak_is_unaffected() {
        let c = coin(12);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(900_000),
                spent_height: None,
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let record = call(move || provider.coin_record(id))
            .expect("read ok")
            .expect("coin present");
        assert_eq!(record.confirmed_height, Some(900_000));
    }

    /// The same clamp holds on the discovery read paths, which are always live (never cache-first).
    #[test]
    fn discovery_reads_clamp_above_peak_confirmed_height() {
        let fetcher = MockFetcher {
            puzzle_states: vec![CoinState {
                coin: coin(13),
                created_height: Some(2_000_000),
                spent_height: Some(2_000_000),
            }],
            children: vec![CoinState {
                coin: coin(14),
                created_height: Some(2_000_000),
                spent_height: Some(2_000_000),
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let ph = Bytes32::new([8; 32]);
        let parent = Bytes32::new([9; 32]);
        let p = provider.clone();
        let by_ph = call(move || p.coin_records_by_puzzle_hash(ph, true)).unwrap();
        assert_eq!(by_ph[0].confirmed_height, Some(1_000_000));
        assert_eq!(by_ph[0].spent_height, Some(1_000_000));
        let by_parent = call(move || provider.coin_records_by_parent(parent)).unwrap();
        assert_eq!(by_parent[0].confirmed_height, Some(1_000_000));
        assert_eq!(by_parent[0].spent_height, Some(1_000_000));
    }

    /// #1346 regression (symmetric to #1326): a cache-miss live fetch returning a coin SPENT above
    /// the current peak (the one-block window before the matching NewPeakWallet lands) must report
    /// `spent_height` clamped to the peak (0 spend-depth), NEVER above it — closing the identical
    /// `peak_height - spent_height` (u32) underflow. The coin stays PRESENT and stays marked SPENT
    /// (`spent_height` remains `Some`); only the reported height is clamped.
    #[test]
    fn live_fetched_coin_spent_above_peak_reports_clamped_spent_height() {
        let c = coin(15);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(999_999),
                spent_height: Some(1_000_001), // spent above the peak below
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let record = call(move || provider.coin_record(id))
            .expect("read ok")
            .expect("coin present, never omitted");
        assert_eq!(
            record.spent_height,
            Some(1_000_000),
            "an above-peak spent coin must clamp spent_height to the peak (0 spend-depth), never overstate"
        );
        assert!(
            record.is_spent(),
            "the coin IS spent — clamping the reported height must never drop the spent flag"
        );
    }

    /// A live-fetched coin spent at/below the peak keeps its real spend height (the clamp is a no-op
    /// on the normal path).
    #[test]
    fn live_fetched_coin_spent_at_or_below_peak_is_unaffected() {
        let c = coin(16);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(800_000),
                spent_height: Some(900_000),
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let record = call(move || provider.coin_record(id))
            .expect("read ok")
            .expect("coin present");
        assert_eq!(record.spent_height, Some(900_000));
    }

    /// The clamp must not flip a spent coin to unspent: `coin_spend` keys spentness on the RAW peer
    /// state (not the clamped record), so a coin spent above the lagged peak still assembles its spend.
    #[test]
    fn coin_spend_of_coin_spent_above_peak_still_identified_as_spent() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([17; 32]), ph, 1);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(999_999),
                spent_height: Some(1_000_001), // above the peak
            }],
            reveal: Some((puzzle, Program::from(vec![2u8]))),
            ..Default::default()
        };
        let (_rt, provider) = provider_with_peak(fetcher, Some(1_000_000));
        let spend = call(move || provider.coin_spend(id))
            .unwrap()
            .expect("a coin spent above the lagged peak is still spent");
        assert_eq!(spend.coin, c);
    }

    #[test]
    fn coin_record_returns_none_for_provable_absence() {
        let (_rt, provider) = provider_with(MockFetcher::default());
        let id = coin(9).coin_id();
        let record = call(move || provider.coin_record(id)).expect("read ok");
        assert_eq!(record, None);
    }

    #[test]
    fn transport_failure_is_err_never_false_absence() {
        let fetcher = MockFetcher {
            fail: Some(ChiaPeerError::Transport("socket reset".into())),
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let id = coin(3).coin_id();
        let result = call(move || provider.coin_record(id));
        assert!(
            matches!(result, Err(ChainSourceError::Transport(_))),
            "a transport failure MUST be Err, never Ok(None): {result:?}"
        );
    }

    #[test]
    fn coin_spend_of_unspent_coin_is_none() {
        let c = coin(4);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(10),
                spent_height: None,
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        assert_eq!(call(move || provider.coin_spend(id)).unwrap(), None);
    }

    #[test]
    fn coin_spend_of_spent_coin_assembles_from_real_coin() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([5; 32]), ph, 1);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(10),
                spent_height: Some(20),
            }],
            reveal: Some((puzzle, Program::from(vec![2u8]))),
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let spend = call(move || provider.coin_spend(id))
            .unwrap()
            .expect("spend");
        assert_eq!(spend.coin, c);
    }

    /// Fix 3 regression: a KNOWN-SPENT coin whose reveal the peer rejects must fail closed with
    /// `Err` — NEVER `Ok(None)` (which would corrupt the interface's parent-walk authentication).
    #[test]
    fn coin_spend_of_spent_coin_with_rejected_reveal_is_err_never_none() {
        let c = coin(6);
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(10),
                spent_height: Some(20),
            }],
            reveal: None, // peer rejects / has no reveal
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let result = call(move || provider.coin_spend(id));
        assert!(
            matches!(result, Err(ChainSourceError::Transport(_))),
            "a rejected reveal for a spent coin must be Err, never Ok(None): {result:?}"
        );
    }

    /// Fix 4 regression: a reveal that does NOT hash to the coin's puzzle hash (a lying peer) is
    /// rejected as malformed, never assembled into a bogus spend.
    #[test]
    fn coin_spend_rejects_a_reveal_that_does_not_hash_to_the_coin() {
        let c = coin(8); // puzzle_hash is [8^1;32], which the reveal below will NOT hash to
        let id = c.coin_id();
        let fetcher = MockFetcher {
            coin_states: vec![CoinState {
                coin: c,
                created_height: Some(10),
                spent_height: Some(20),
            }],
            reveal: Some((Program::from(vec![1u8]), Program::from(vec![2u8]))),
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let result = call(move || provider.coin_spend(id));
        assert!(
            matches!(result, Err(ChainSourceError::Malformed(_))),
            "a mismatched reveal must be Malformed: {result:?}"
        );
    }

    #[test]
    fn records_by_puzzle_hash_and_parent_map_states() {
        let fetcher = MockFetcher {
            puzzle_states: vec![CoinState {
                coin: coin(1),
                created_height: Some(1),
                spent_height: None,
            }],
            children: vec![CoinState {
                coin: coin(2),
                created_height: Some(2),
                spent_height: None,
            }],
            ..Default::default()
        };
        let (_rt, provider) = provider_with(fetcher);
        let ph = Bytes32::new([8; 32]);
        let parent = Bytes32::new([9; 32]);
        let p = provider.clone();
        assert_eq!(
            call(move || p.coin_records_by_puzzle_hash(ph, true))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            call(move || provider.coin_records_by_parent(parent))
                .unwrap()
                .len(),
            1
        );
    }

    /// `block_timestamp` is the ONE read this source genuinely cannot answer: a light client keeps
    /// no timestamp index. It reports `Unsupported` rather than a false `Ok(None)`, so a composing
    /// registry falls through to a source that does index timestamps.
    #[test]
    fn timestamp_is_unsupported_not_false_absence() {
        let (_rt, provider) = provider_with(MockFetcher::default());
        assert!(matches!(
            call(move || provider.block_timestamp(1)),
            Err(ChainSourceError::Unsupported(_))
        ));
    }

    // ---- #2792: the cache never answers for state it is no longer receiving updates for ----

    /// A fetcher holding the TRUTH about `coin`: spent at `spent_height`, with a verifiable reveal.
    fn fetcher_reporting_spent(coin: Coin, spent_height: u32, puzzle: Program) -> MockFetcher {
        MockFetcher {
            coin_states: vec![CoinState {
                coin,
                created_height: Some(10),
                spent_height: Some(spent_height),
            }],
            reveal: Some((puzzle, Program::from(vec![2u8]))),
            ..Default::default()
        }
    }

    /// Probe (a) — the REORG route to a stranded entry. A coin is cached as spent, then untracked
    /// (the drive loop drops tracking of spent coins), then a reorg rolls its spend back: the
    /// rollback loop UN-SPENDS the now-unsubscribed entry, leaving an unspent state no update can
    /// ever correct. `coin_spend` short-circuits off that same cached `spent_height`, so the lie is
    /// self-consistent and the real spend is never seen.
    #[test]
    fn a_stranded_entry_unspent_by_a_reorg_does_not_hide_the_real_spend() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([21; 32]), ph, 1);
        let id = c.coin_id();

        let mut cache = CoinStateCache::new();
        cache.track_coins([id]);
        cache.seed([CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: Some(150),
        }]);
        cache.set_peak(200, Bytes32::new([0xAB; 32]));
        cache.untrack_coins(&[id]); // spent → the drive loop stops tracking it
                                    // A reorg forking below the spend un-spends whatever is still cached.
        cache.apply_update(&[], 160, 100, Bytes32::new([0xCD; 32]));

        let (_rt, provider) = provider_with_cache(fetcher_reporting_spent(c, 150, puzzle), cache);
        let spend = call(move || provider.coin_spend(id))
            .expect("read ok")
            .expect("the real spend must be reported, never masked by a stranded cache entry");
        assert_eq!(spend.coin, c);
    }

    /// Probe (b) — the EXPLICIT-UNSUBSCRIBE route. Nothing exotic: the caller unsubscribes, so the
    /// cache stops receiving updates for the coin. Any state it keeps is frozen at that instant, and
    /// a coin unspent then may be spent now.
    #[test]
    fn an_unsubscribed_coin_is_not_served_from_a_frozen_cache_entry() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([22; 32]), ph, 1);
        let id = c.coin_id();

        let mut cache = CoinStateCache::new();
        cache.track_coins([id]);
        cache.seed([CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: None, // unspent at subscribe time
        }]);
        cache.untrack_coins(&[id]); // the `unsubscribe_coins` path

        let (_rt, provider) = provider_with_cache(fetcher_reporting_spent(c, 150, puzzle), cache);
        let spend = call(move || provider.coin_spend(id))
            .expect("read ok")
            .expect("a coin spent after the unsubscribe must still report its spend");
        assert_eq!(spend.coin, c);
    }

    /// A coin still covered by a subscribed PUZZLE HASH is RETAINED when its coin-id subscription is
    /// dropped — the sweep is a coverage rule, not a blind delete of the listed ids.
    #[test]
    fn untracking_a_coin_still_covered_by_a_puzzle_hash_retains_it() {
        let c = coin(23);
        let id = c.coin_id();
        let state = CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: None,
        };

        let mut cache = CoinStateCache::new();
        cache.track_puzzle_hashes([c.puzzle_hash]);
        cache.track_coins([id]);
        cache.seed([state]);
        cache.untrack_coins(&[id]);

        assert_eq!(
            cache.get(id),
            Some(state),
            "a coin under a still-subscribed puzzle hash keeps receiving updates, so it is retained"
        );
    }

    /// The SEED-BYPASS route, grounded in its real production instance: a HINTED coin. Subscriptions
    /// request `include_hinted: true`, and a hinted coin is returned because it is hinted TO the
    /// subscribed puzzle hash — its OWN `puzzle_hash` is something else (a CAT outer puzzle hash,
    /// typically). So `apply_update` will never accept an update for it, and a cached copy would be
    /// frozen at first-subscribe forever. It must not enter the cache at all.
    #[test]
    fn a_hinted_coin_outside_the_subscribed_set_is_not_cached_by_seed() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let hinted = Coin::new(Bytes32::new([24; 32]), ph, 1); // its own puzzle hash != the subscribed one
        let id = hinted.coin_id();

        let mut cache = CoinStateCache::new();
        cache.track_puzzle_hashes([Bytes32::new([0xEE; 32])]); // the hint target, not the coin's ph
        cache.seed([CoinState {
            coin: hinted,
            created_height: Some(10),
            spent_height: None,
        }]);
        assert_eq!(
            cache.get(id),
            None,
            "seed must not smuggle in an uncovered coin"
        );

        // ...and the provider therefore reads through to the peer, which reports the real spend.
        let (_rt, provider) =
            provider_with_cache(fetcher_reporting_spent(hinted, 150, puzzle), cache);
        assert!(
            call(move || provider.coin_spend(id))
                .expect("read ok")
                .is_some(),
            "an uncached hinted coin is read live, so its spend is visible"
        );
    }

    /// Probe (c) — the DEAD-STREAM route. The drive loop exited, so nothing can update the cache;
    /// every read must fall through to the peer. A successful re-seed restores cache-first service.
    #[test]
    fn a_dead_update_stream_stops_the_cache_answering_until_it_is_re_seeded() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([25; 32]), ph, 1);
        let id = c.coin_id();
        let unspent = CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: None,
        };

        let mut cache = CoinStateCache::new();
        cache.track_coins([id]);
        cache.seed([unspent]);
        cache.mark_stream_ended();

        let fetcher = fetcher_reporting_spent(c, 150, puzzle);
        let reads = fetcher.coin_state_reads.clone();
        let (_rt, provider) = provider_with_cache(fetcher, cache);

        let p = provider.clone();
        assert!(
            call(move || p.coin_spend(id)).expect("read ok").is_some(),
            "a dead stream must send the read to the peer, which reports the spend"
        );
        assert!(
            reads.load(Ordering::SeqCst) > 0,
            "the read reached the peer"
        );

        // A reconnect re-seeds through `subscribe_coins`, which restores liveness.
        provider
            .handle
            .block_on(async { provider.cache.write().await.seed([unspent]) });
        let before = reads.load(Ordering::SeqCst);
        let p = provider.clone();
        assert_eq!(
            call(move || p.coin_spend(id)).expect("read ok"),
            None,
            "after the re-seed the cache answers again (and it says unspent)"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            before,
            "the re-seeded read is served from cache, with no peer round trip"
        );
    }

    /// The wall-clock backstop: a SILENTLY WEDGED connection never closes, so the drive loop never
    /// exits and `mark_stream_ended` never fires. Silence alone must stop the cache answering.
    /// Driven by an injected zero-length window, never by elapsing real time.
    #[test]
    fn silence_beyond_max_silence_stops_the_cache_answering() {
        let (puzzle, ph) = reveal_and_matching_puzzle_hash();
        let c = Coin::new(Bytes32::new([26; 32]), ph, 1);
        let id = c.coin_id();

        let mut cache = CoinStateCache::new().with_max_silence(Duration::ZERO);
        cache.track_coins([id]);
        cache.seed([CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: None,
        }]);

        let (_rt, provider) = provider_with_cache(fetcher_reporting_spent(c, 150, puzzle), cache);
        assert!(
            call(move || provider.coin_spend(id))
                .expect("read ok")
                .is_some(),
            "a wedged connection's stale entry must not answer; the peer reports the spend"
        );
    }

    /// The control that keeps the fix from degrading into verify-on-use: a SUBSCRIBED coin on a LIVE
    /// stream is still served entirely from cache, with ZERO peer round trips.
    #[test]
    fn a_subscribed_coin_on_a_live_stream_is_served_with_no_peer_read() {
        let c = coin(27);
        let id = c.coin_id();
        let mut cache = CoinStateCache::new();
        cache.track_coins([id]);
        cache.seed([CoinState {
            coin: c,
            created_height: Some(10),
            spent_height: None,
        }]);

        let fetcher = MockFetcher::default();
        let reads = fetcher.coin_state_reads.clone();
        let (_rt, provider) = provider_with_cache(fetcher, cache);
        let record = call(move || provider.coin_record(id))
            .expect("read ok")
            .expect("a live subscribed coin is served from cache");
        assert_eq!(record.confirmed_height, Some(10));
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "the subscribed-and-current hot path must not touch the network"
        );
    }

    #[test]
    fn provider_info_is_reported() {
        let (_rt, provider) = provider_with(MockFetcher::default());
        assert_eq!(provider.provider_info().priority, 20);
        assert_eq!(provider.peak_height().unwrap(), None);
    }
}

/// Singleton-lineage resolution, exercised through [`ChiaPeerProvider`] against a SNAPSHOT of real
/// simulated chain state.
///
/// The fixtures are genuine: a real launcher, a real eve, and real recreation spends produced by the
/// in-process Chia simulator, then served back through the crate's own [`CoinStateFetcher`] seam. So
/// the walk runs on the production path — `ChiaPeerProvider::coin_record` + `coin_spend`, reveal
/// verification included — rather than on a direct call into `dig_chainsource_interface`, which
/// would prove nothing about this crate.
#[cfg(test)]
mod lineage_tests {
    use super::*;
    use crate::error::ChiaPeerError;
    use async_trait::async_trait;
    use chia_protocol::Coin;
    use chia_wallet_sdk::chia::puzzle_types::singleton::{SingletonArgs, SingletonSolution};
    use chia_wallet_sdk::chia::puzzle_types::{EveProof, LineageProof, Memos, Proof};
    use chia_wallet_sdk::clvm_utils::TreeHash;
    use chia_wallet_sdk::driver::{
        Launcher, Layer, SingletonLayer, Spend, SpendContext, SpendWithConditions, StandardLayer,
    };
    use chia_wallet_sdk::test::Simulator;
    use chia_wallet_sdk::types::Conditions;
    use dig_chainsource_interface::{ProviderId, ProviderKind};
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fetcher answering from a snapshot of real simulated chain state, keyed by coin id.
    ///
    /// Unlike the provider module's scripted `MockFetcher`, every read here is per-coin, which is
    /// what a multi-hop walk needs: hop `n` must see hop `n`'s own spend, never a single canned one.
    #[derive(Default)]
    struct ChainSnapshotFetcher {
        states: HashMap<Bytes32, CoinState>,
        spends: HashMap<Bytes32, (Program, Program)>,
        children: HashMap<Bytes32, Vec<CoinState>>,
        /// When set, this coin's spend read FAILS (an honest source that lost the read), so the
        /// three-valued discipline can be checked mid-walk rather than only at the first hop.
        fail_spend_of: Option<Bytes32>,
        /// How many times the child list was consulted. The canonical walk DERIVES its successor
        /// from the parent's own spend and must never pick one out of a source-supplied child list,
        /// so this stays zero.
        children_reads: AtomicUsize,
    }

    #[async_trait]
    impl CoinStateFetcher for ChainSnapshotFetcher {
        async fn coin_states(
            &self,
            coin_ids: Vec<Bytes32>,
            _subscribe: bool,
        ) -> Result<Vec<CoinState>, ChiaPeerError> {
            Ok(coin_ids
                .into_iter()
                .filter_map(|id| self.states.get(&id).copied())
                .collect())
        }

        async fn puzzle_states(
            &self,
            _puzzle_hashes: Vec<Bytes32>,
            _filters: CoinStateFilters,
            _subscribe: bool,
        ) -> Result<Vec<CoinState>, ChiaPeerError> {
            Ok(Vec::new())
        }

        async fn children(&self, coin_id: Bytes32) -> Result<Vec<CoinState>, ChiaPeerError> {
            self.children_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.children.get(&coin_id).cloned().unwrap_or_default())
        }

        async fn puzzle_and_solution(
            &self,
            coin_id: Bytes32,
            _height: u32,
        ) -> Result<(Program, Program), ChiaPeerError> {
            if self.fail_spend_of == Some(coin_id) {
                return Err(ChiaPeerError::Transport("socket reset".into()));
            }
            self.spends
                .get(&coin_id)
                .cloned()
                // The caller reached here only after confirming the coin is spent → fail closed.
                .ok_or_else(|| ChiaPeerError::Rejected("no reveal".into()))
        }
    }

    /// A live singleton in the simulator, tracked as the fixture advances it.
    struct Singleton {
        launcher_id: Bytes32,
        /// Launcher -> ... -> tip, in walk order.
        trail: Vec<Coin>,
        proof: Proof,
        inner_puzzle_hash: Bytes32,
        pk: chia_wallet_sdk::chia::bls::PublicKey,
        sk: chia_wallet_sdk::chia::bls::SecretKey,
    }

    impl Singleton {
        fn tip(&self) -> Coin {
            *self.trail.last().expect("a launched singleton has a tip")
        }

        fn outer_puzzle_hash(&self) -> Bytes32 {
            SingletonArgs::curry_tree_hash(self.launcher_id, TreeHash::from(self.inner_puzzle_hash))
                .into()
        }
    }

    /// Launches a real singleton with a standard p2 inner puzzle and settles it.
    fn launch(sim: &mut Simulator, ctx: &mut SpendContext) -> Singleton {
        let owner = sim.bls(1);
        let launcher = Launcher::new(owner.coin.coin_id(), 1);
        let launcher_coin = launcher.coin();
        let (conditions, eve) = launcher
            .spend(ctx, owner.puzzle_hash, ())
            .expect("the launcher spends");
        StandardLayer::new(owner.pk)
            .spend(ctx, owner.coin, conditions)
            .expect("the funding coin spends");
        sim.spend_coins(ctx.take(), std::slice::from_ref(&owner.sk))
            .expect("the launch settles");

        Singleton {
            launcher_id: launcher_coin.coin_id(),
            trail: vec![launcher_coin, eve],
            proof: Proof::Eve(EveProof {
                parent_parent_coin_info: launcher_coin.parent_coin_info,
                parent_amount: launcher_coin.amount,
            }),
            inner_puzzle_hash: owner.puzzle_hash,
            pk: owner.pk,
            sk: owner.sk,
        }
    }

    /// Advances the singleton by one genuine recreation spend, appending the new tip to the trail.
    fn advance(sim: &mut Simulator, ctx: &mut SpendContext, singleton: &mut Singleton) {
        let tip = singleton.tip();
        let sk = singleton.sk.clone();
        let conditions =
            Conditions::new().create_coin(singleton.inner_puzzle_hash, tip.amount, Memos::None);
        let inner = StandardLayer::new(singleton.pk)
            .spend_with_conditions(ctx, conditions)
            .expect("the inner puzzle spends");
        let layer = SingletonLayer::new(singleton.launcher_id, StandardLayer::new(singleton.pk));
        let solution = SingletonSolution {
            lineage_proof: singleton.proof,
            amount: tip.amount,
            inner_solution: inner.solution,
        };
        let puzzle = layer
            .construct_puzzle(ctx)
            .expect("the singleton puzzle builds");
        let solution = ctx.alloc(&solution).expect("the solution allocates");
        ctx.spend(tip, Spend::new(puzzle, solution))
            .expect("the singleton spends");
        sim.spend_coins(ctx.take(), std::slice::from_ref(&sk))
            .expect("the recreation settles");

        singleton.proof = Proof::Lineage(LineageProof {
            parent_parent_coin_info: tip.parent_coin_info,
            parent_inner_puzzle_hash: singleton.inner_puzzle_hash,
            parent_amount: tip.amount,
        });
        singleton.trail.push(Coin::new(
            tip.coin_id(),
            singleton.outer_puzzle_hash(),
            tip.amount,
        ));
    }

    /// Snapshots every coin on `trail` — its state, its spend, and its children — out of the
    /// simulator, so the fetcher serves real chain data without borrowing the simulator itself.
    fn snapshot(sim: &Simulator, trail: &[Coin]) -> ChainSnapshotFetcher {
        let mut fetcher = ChainSnapshotFetcher::default();
        for coin in trail {
            let id = coin.coin_id();
            if let Some(state) = sim.coin_state(id) {
                fetcher.states.insert(id, state);
            }
            if let Some(spend) = sim.coin_spend(id) {
                fetcher
                    .spends
                    .insert(id, (spend.puzzle_reveal, spend.solution));
            }
            fetcher.children.insert(id, sim.children(id));
        }
        fetcher
    }

    fn provider_over(
        fetcher: Arc<ChainSnapshotFetcher>,
    ) -> (tokio::runtime::Runtime, ChiaPeerProvider) {
        provider_with_cache(fetcher, CoinStateCache::new())
    }

    /// Builds a lineage provider over an ARBITRARY, already-populated cache — the seam the custody
    /// proof needs, since every other builder starts empty and so cannot exercise a lying cache.
    fn provider_with_cache(
        fetcher: Arc<ChainSnapshotFetcher>,
        cache: CoinStateCache,
    ) -> (tokio::runtime::Runtime, ChiaPeerProvider) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let provider = ChiaPeerProvider::new(
            fetcher,
            Arc::new(RwLock::new(cache)),
            rt.handle().clone(),
            ProviderInfo {
                id: ProviderId(Cow::Borrowed("chia-peer-lineage-test")),
                kind: ProviderKind::Custom,
                priority: 20,
                trustless: false,
            },
        );
        (rt, provider)
    }

    /// Runs a sync facade method off any ambient runtime (the bridge's "outside a runtime" path).
    fn call<T: Send>(f: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|s| s.spawn(f).join().expect("thread panicked"))
    }

    /// A multi-hop singleton — launcher, eve, and two genuine recreations — resolves THROUGH the
    /// provider, with every coin on the trail a member of the returned lineage.
    #[test]
    fn a_multi_hop_singleton_resolves_through_the_provider() {
        let mut sim = Simulator::new();
        let ctx = &mut SpendContext::new();
        let mut singleton = launch(&mut sim, ctx);
        advance(&mut sim, ctx, &mut singleton);
        advance(&mut sim, ctx, &mut singleton);
        assert_eq!(singleton.trail.len(), 4, "launcher + eve + two recreations");

        let fetcher = Arc::new(snapshot(&sim, &singleton.trail));
        let (_rt, provider) = provider_over(fetcher.clone());
        let launcher_id = singleton.launcher_id;
        let lineage = call(move || provider.resolve_singleton_lineage(launcher_id))
            .expect("an honest source resolves")
            .expect("a live singleton has a lineage");

        assert_eq!(lineage.tip(), singleton.tip().coin_id());
        assert_eq!(lineage.len(), singleton.trail.len());
        for coin in &singleton.trail {
            assert!(
                lineage.contains(coin.coin_id()),
                "genuine lineage coin {} is missing",
                coin.coin_id()
            );
        }
        assert_eq!(
            fetcher.children_reads.load(Ordering::SeqCst),
            0,
            "the successor MUST be derived from the parent's own spend, never picked out of a \
             source-supplied child list"
        );
    }

    /// THE CUSTODY PROOF (#2792). The walk is only as honest as the reads under it: a cache that
    /// still answers for a coin it no longer receives updates for reports that coin as UNSPENT, the
    /// walk stops there, and a SUPERSEDED coin authenticates as the singleton's tip. That is exactly
    /// the melt scenario `dig-chainsource-interface-0.3.1/src/walk.rs:539` names — a destroyed
    /// singleton passing as live.
    ///
    /// The pair with `a_multi_hop_singleton_resolves_through_the_provider` (identical chain, EMPTY
    /// cache) is what discriminates: only the stale-cache variant can expose the lie.
    #[test]
    fn a_stale_cache_entry_cannot_authenticate_a_superseded_coin_as_the_tip() {
        let mut sim = Simulator::new();
        let ctx = &mut SpendContext::new();
        let mut singleton = launch(&mut sim, ctx);
        advance(&mut sim, ctx, &mut singleton);
        advance(&mut sim, ctx, &mut singleton);

        let fetcher = Arc::new(snapshot(&sim, &singleton.trail));

        // The eve is an INTERMEDIATE coin: long spent, but the cache holds its pre-spend state and
        // has since stopped receiving updates for it.
        let eve = singleton.trail[1];
        let eve_id = eve.coin_id();
        let mut cache = CoinStateCache::new();
        cache.track_coins([eve_id]);
        cache.seed([CoinState {
            coin: eve,
            created_height: Some(1),
            spent_height: None,
        }]);
        cache.untrack_coins(&[eve_id]);

        let (_rt, provider) = provider_with_cache(fetcher, cache);
        let launcher_id = singleton.launcher_id;
        let lineage = call(move || provider.resolve_singleton_lineage(launcher_id))
            .expect("an honest source resolves")
            .expect("a live singleton has a lineage");

        assert_eq!(
            lineage.tip(),
            singleton.tip().coin_id(),
            "a stale cache entry must not stop the walk at a superseded coin"
        );
        assert_eq!(lineage.len(), singleton.trail.len());
    }

    /// A launcher id naming no coin is a REAL absence — `Ok(None)`, not an error.
    #[test]
    fn a_launcher_id_naming_no_coin_is_a_real_absence() {
        let (_rt, provider) = provider_over(Arc::new(ChainSnapshotFetcher::default()));
        let result = call(move || provider.resolve_singleton_lineage(Bytes32::new([0x5A; 32])));
        assert_eq!(result.expect("a provable absence is not an error"), None);
    }

    /// A source read that FAILS mid-walk must be `Err(_)` — never collapsed into `Ok(None)`.
    ///
    /// The failure is placed on the EVE's spend, after the launcher hop has already succeeded, so a
    /// walk that returned the partial lineage it had accumulated (or read the failure as "no more
    /// lineage") would be caught. A transport failure read as an absence is the bug class that
    /// spends money twice.
    #[test]
    fn a_source_failure_mid_walk_is_err_never_a_false_absence() {
        let mut sim = Simulator::new();
        let ctx = &mut SpendContext::new();
        let mut singleton = launch(&mut sim, ctx);
        advance(&mut sim, ctx, &mut singleton);

        let mut fetcher = snapshot(&sim, &singleton.trail);
        fetcher.fail_spend_of = Some(singleton.trail[1].coin_id()); // the eve's spend
        let (_rt, provider) = provider_over(Arc::new(fetcher));
        let launcher_id = singleton.launcher_id;
        let result = call(move || provider.resolve_singleton_lineage(launcher_id));
        assert!(
            matches!(result, Err(ChainSourceError::Transport(_))),
            "a failed source read MUST be Err, never Ok(None) or a partial lineage: {result:?}"
        );
    }
}
