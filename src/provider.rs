//! [`ChiaPeerProvider`] — a synchronous [`ChainSource`] facade over the light client's subscription
//! cache + wallet-protocol peer.
//!
//! Reads answer from the local [`CoinStateCache`](crate::cache::CoinStateCache) first; a miss falls
//! through to a **non-subscribing** peer query (so a read never silently grows the subscription set).
//! Every outcome honours the interface's fail-closed contract: `Ok(None)`/empty means the peer
//! reliably reported absence, while any transport/subscription-gap failure is an `Err` — NEVER a
//! false `Ok(None)`.
//!
//! ## Boundary
//!
//! A subscribing light client is not a full archival index. Two reads are deliberately reported as
//! [`ChainSourceError::Unsupported`] rather than answered unreliably:
//! - [`resolve_singleton_lineage`](ChainSource::resolve_singleton_lineage) — a money-critical forward
//!   walk better served by an aggregating source; answering it from subscription state would risk a
//!   spoofable, partial lineage.
//! - [`block_timestamp`](ChainSource::block_timestamp) — a light source keeps no timestamp index.
//!
//! The registry composes providers, so these fall through to a source that does support them.

use std::sync::Arc;

use chia_protocol::{Bytes32, CoinSpend, CoinState, CoinStateFilters};
use dig_chainsource_interface::{
    ChainSource, ChainSourceError, ChainSourceProvider, CoinRecord, ProviderInfo, SingletonLineage,
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
        Ok(self.coin_state(coin_id)?.map(CoinRecord::from_coin_state))
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
        Ok(states
            .into_iter()
            .map(CoinRecord::from_coin_state)
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
        Ok(states
            .into_iter()
            .map(CoinRecord::from_coin_state)
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
        let reveal = run_blocking(&self.handle, async move {
            fetcher.puzzle_and_solution(coin_id, spent_height).await
        })?
        .map_err(ChainSourceError::from)?;
        Ok(reveal.map(|(puzzle, solution)| CoinSpend::new(state.coin, puzzle, solution)))
    }

    fn resolve_singleton_lineage(
        &self,
        _launcher_id: Bytes32,
    ) -> Result<Option<SingletonLineage>, Self::Error> {
        Err(ChainSourceError::Unsupported(
            "singleton lineage resolution is not provided by the light-client source; \
             use an aggregating chain source",
        ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ChiaPeerError;
    use async_trait::async_trait;
    use chia_protocol::{Coin, Program};
    use dig_chainsource_interface::{ProviderId, ProviderKind};
    use std::borrow::Cow;

    /// A scripted fetcher: each read returns the configured `Ok(..)` states or a forced error, so
    /// the provider's fail-closed mapping can be exercised without a live node.
    #[derive(Default, Clone)]
    struct MockFetcher {
        coin_states: Vec<CoinState>,
        fail: Option<ChiaPeerError>,
        children: Vec<CoinState>,
        puzzle_states: Vec<CoinState>,
        reveal: Option<(Program, Program)>,
    }

    #[async_trait]
    impl CoinStateFetcher for MockFetcher {
        async fn coin_states(
            &self,
            _coin_ids: Vec<Bytes32>,
            _subscribe: bool,
        ) -> Result<Vec<CoinState>, ChiaPeerError> {
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
        ) -> Result<Option<(Program, Program)>, ChiaPeerError> {
            match &self.fail {
                Some(e) => Err(e.clone()),
                None => Ok(self.reveal.clone()),
            }
        }
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let provider = ChiaPeerProvider::new(
            Arc::new(fetcher),
            Arc::new(RwLock::new(CoinStateCache::new())),
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
        let c = coin(5);
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
        let spend = call(move || provider.coin_spend(id))
            .unwrap()
            .expect("spend");
        assert_eq!(spend.coin, c);
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

    #[test]
    fn lineage_and_timestamp_are_unsupported_not_false_absence() {
        let (_rt, provider) = provider_with(MockFetcher::default());
        let p = provider.clone();
        assert!(matches!(
            call(move || p.resolve_singleton_lineage(Bytes32::new([1; 32]))),
            Err(ChainSourceError::Unsupported(_))
        ));
        assert!(matches!(
            call(move || provider.block_timestamp(1)),
            Err(ChainSourceError::Unsupported(_))
        ));
    }

    #[test]
    fn provider_info_is_reported() {
        let (_rt, provider) = provider_with(MockFetcher::default());
        assert_eq!(provider.provider_info().priority, 20);
        assert_eq!(provider.peak_height().unwrap(), None);
    }
}
