//! [`CoinStateFetcher`] — the async seam the [`ChiaPeerProvider`](crate::ChiaPeerProvider) reads
//! through, and [`PeerFetcher`], its real implementation over a connected wallet-protocol
//! [`Peer`](chia_wallet_sdk::client::Peer).
//!
//! Isolating the peer behind a trait keeps the provider's fail-closed logic unit-testable with an
//! in-memory mock (no live full node), and lets [`reconnect`](crate::ChiaLightClient::reconnect)
//! swap the underlying peer without disturbing readers.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chia_protocol::{Bytes32, CoinState, CoinStateFilters, Program, SpendBundle};
use chia_wallet_sdk::client::Peer;
use tokio::sync::RwLock;

use crate::error::ChiaPeerError;

/// Max pages an UNTRUSTED peer may return for one paged puzzle-state read before we fail closed.
/// Bounds a hostile peer that never sets `is_finished` (would otherwise hang + OOM).
const MAX_PUZZLE_STATE_PAGES: usize = 10_000;

/// Max coin states accumulated across a single paged read before we fail closed. Bounds unbounded
/// memory growth from a peer that streams coins forever.
const MAX_ACCUMULATED_COIN_STATES: usize = 500_000;

/// The reads a provider issues against a Chia full node when its local cache misses.
///
/// `subscribe = true` arms a server-side subscription so future changes stream back as
/// `CoinStateUpdate`s; `subscribe = false` is a one-shot read that never grows the subscription set.
#[async_trait]
pub trait CoinStateFetcher: Send + Sync {
    /// Reads the current state of `coin_ids`. An empty result is a PROVABLE absence.
    async fn coin_states(
        &self,
        coin_ids: Vec<Bytes32>,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, ChiaPeerError>;

    /// Reads every coin paying to `puzzle_hashes` (paging through the peer's `is_finished` protocol
    /// until complete), applying `filters`.
    async fn puzzle_states(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, ChiaPeerError>;

    /// Reads the direct children created by spending `coin_id`.
    async fn children(&self, coin_id: Bytes32) -> Result<Vec<CoinState>, ChiaPeerError>;

    /// Reads the puzzle reveal + solution of the coin spent at `height`.
    ///
    /// Callers reach this ONLY after confirming the coin is spent, so absence is impossible: a
    /// rejection/absence is a "could not answer" and is returned as `Err(_)` (fail closed), NEVER a
    /// misleading `Ok(None)` that would corrupt the interface's parent-walk authentication.
    async fn puzzle_and_solution(
        &self,
        coin_id: Bytes32,
        height: u32,
    ) -> Result<(Program, Program), ChiaPeerError>;
}

/// A [`CoinStateFetcher`] backed by a live wallet-protocol [`Peer`].
///
/// The peer sits behind an `RwLock<Option<_>>` so [`swap_peer`](Self::swap_peer) can replace it on
/// reconnect while in-flight readers keep working against the peer they cloned.
#[derive(Clone)]
pub struct PeerFetcher {
    peer: Arc<RwLock<Option<Peer>>>,
    genesis_challenge: Bytes32,
    request_timeout: Duration,
}

impl PeerFetcher {
    /// Builds a fetcher over `peer`, using `genesis_challenge` as the height-0 `header_hash` and
    /// bounding every request with `request_timeout`.
    pub fn new(peer: Peer, genesis_challenge: Bytes32, request_timeout: Duration) -> Self {
        Self {
            peer: Arc::new(RwLock::new(Some(peer))),
            genesis_challenge,
            request_timeout,
        }
    }

    /// A fetcher with no peer, so every read fails closed with [`ChiaPeerError::NotConnected`].
    #[cfg(test)]
    fn disconnected(genesis_challenge: Bytes32, request_timeout: Duration) -> Self {
        Self {
            peer: Arc::new(RwLock::new(None)),
            genesis_challenge,
            request_timeout,
        }
    }

    /// Replaces the underlying peer (used by reconnect).
    pub async fn swap_peer(&self, peer: Peer) {
        *self.peer.write().await = Some(peer);
    }

    /// Clones the current peer, or fails closed if the client is not connected.
    async fn peer(&self) -> Result<Peer, ChiaPeerError> {
        self.peer
            .read()
            .await
            .clone()
            .ok_or(ChiaPeerError::NotConnected)
    }

    /// Submits `bundle` to the network, returning the ack `status` byte (`1` = success/pending).
    pub async fn send_transaction(&self, bundle: SpendBundle) -> Result<u8, ChiaPeerError> {
        let peer = self.peer().await?;
        let ack = self
            .with_timeout(peer.send_transaction(bundle))
            .await?
            .map_err(|e| ChiaPeerError::Transport(e.to_string()))?;
        Ok(ack.status)
    }

    /// Removes the server-side subscription to `coin_ids` (wraps `remove_coin_subscriptions`).
    pub async fn remove_coin_subscriptions(
        &self,
        coin_ids: Vec<Bytes32>,
    ) -> Result<(), ChiaPeerError> {
        let peer = self.peer().await?;
        self.with_timeout(peer.remove_coin_subscriptions(Some(coin_ids)))
            .await?
            .map_err(|e| ChiaPeerError::Transport(e.to_string()))?;
        Ok(())
    }

    /// Wraps `fut` with the configured request timeout, mapping an elapsed deadline to
    /// [`ChiaPeerError::Timeout`].
    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = T>,
    ) -> Result<T, ChiaPeerError> {
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| ChiaPeerError::Timeout)
    }
}

/// One page of a paged `request_puzzle_state` read.
struct PuzzleStatePage {
    coin_states: Vec<CoinState>,
    height: u32,
    header_hash: Bytes32,
    is_finished: bool,
}

/// Drives a paged puzzle-state read to completion, bounding an UNTRUSTED peer three ways so it can
/// neither hang the caller nor exhaust memory: a page cap, a total accumulated-coin cap, and a
/// strict-progress requirement (each unfinished page MUST advance `height`). Any violation fails
/// closed with `Err`, never an unbounded loop.
///
/// The page fetch is injected so the bounding policy is unit-testable without a live peer.
async fn collect_paged<F, Fut>(
    genesis_challenge: Bytes32,
    mut fetch_page: F,
) -> Result<Vec<CoinState>, ChiaPeerError>
where
    F: FnMut(Option<u32>, Bytes32) -> Fut,
    Fut: std::future::Future<Output = Result<PuzzleStatePage, ChiaPeerError>>,
{
    let mut all = Vec::new();
    let mut previous_height: Option<u32> = None;
    let mut header_hash = genesis_challenge;

    for _page in 0..MAX_PUZZLE_STATE_PAGES {
        let page = fetch_page(previous_height, header_hash).await?;
        all.extend(page.coin_states);
        if all.len() > MAX_ACCUMULATED_COIN_STATES {
            return Err(ChiaPeerError::Rejected(format!(
                "puzzle-state response exceeded {MAX_ACCUMULATED_COIN_STATES} coins"
            )));
        }
        if page.is_finished {
            return Ok(all);
        }
        if previous_height.is_some_and(|prev| page.height <= prev) {
            return Err(ChiaPeerError::Rejected(
                "puzzle-state paging did not advance the height".into(),
            ));
        }
        previous_height = Some(page.height);
        header_hash = page.header_hash;
    }
    Err(ChiaPeerError::Rejected(format!(
        "puzzle-state paging exceeded {MAX_PUZZLE_STATE_PAGES} pages"
    )))
}

#[async_trait]
impl CoinStateFetcher for PeerFetcher {
    async fn coin_states(
        &self,
        coin_ids: Vec<Bytes32>,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, ChiaPeerError> {
        let peer = self.peer().await?;
        let response = self
            .with_timeout(peer.request_coin_state(
                coin_ids,
                None,
                self.genesis_challenge,
                subscribe,
            ))
            .await?
            .map_err(|e| ChiaPeerError::Transport(e.to_string()))?
            .map_err(|_| ChiaPeerError::Rejected("coin-state request rejected".into()))?;
        Ok(response.coin_states)
    }

    async fn puzzle_states(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
        subscribe: bool,
    ) -> Result<Vec<CoinState>, ChiaPeerError> {
        let peer = self.peer().await?;
        // Subscribe only once the final page arrives, so a single subscription covers the set.
        collect_paged(self.genesis_challenge, |previous_height, header_hash| {
            let peer = peer.clone();
            let puzzle_hashes = puzzle_hashes.clone();
            let filters = filters.clone();
            let this = self;
            async move {
                let response = this
                    .with_timeout(peer.request_puzzle_state(
                        puzzle_hashes,
                        previous_height,
                        header_hash,
                        filters,
                        subscribe,
                    ))
                    .await?
                    .map_err(|e| ChiaPeerError::Transport(e.to_string()))?
                    .map_err(|_| ChiaPeerError::Rejected("puzzle-state request rejected".into()))?;
                Ok(PuzzleStatePage {
                    coin_states: response.coin_states,
                    height: response.height,
                    header_hash: response.header_hash,
                    is_finished: response.is_finished,
                })
            }
        })
        .await
    }

    async fn children(&self, coin_id: Bytes32) -> Result<Vec<CoinState>, ChiaPeerError> {
        let peer = self.peer().await?;
        let response = self
            .with_timeout(peer.request_children(coin_id))
            .await?
            .map_err(|e| ChiaPeerError::Transport(e.to_string()))?;
        Ok(response.coin_states)
    }

    async fn puzzle_and_solution(
        &self,
        coin_id: Bytes32,
        height: u32,
    ) -> Result<(Program, Program), ChiaPeerError> {
        let peer = self.peer().await?;
        let outcome = self
            .with_timeout(peer.request_puzzle_and_solution(coin_id, height))
            .await?
            .map_err(|e| ChiaPeerError::Transport(e.to_string()))?;
        match outcome {
            Ok(response) => Ok((response.puzzle, response.solution)),
            // The caller only asks after confirming a spent height, so a reject here is NOT a
            // genuine absence — it is a peer that could not/would not answer. Fail closed with Err,
            // never a misleading Ok(None) (mirrors how coin-state rejects map to Err(Rejected)).
            Err(_) => Err(ChiaPeerError::Rejected(
                "peer rejected puzzle/solution for a known-spent coin".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChiaNetwork;
    use chia_protocol::{Coin, SpendBundle};
    use chia_wallet_sdk::test::PeerSimulator;
    use std::time::Duration;

    fn genesis() -> Bytes32 {
        ChiaNetwork::Testnet11.genesis_challenge()
    }

    async fn fetcher_over_sim() -> (PeerSimulator, PeerFetcher, Coin) {
        let sim = PeerSimulator::new().await.expect("start simulator");
        let coin = sim.lock().await.new_coin(Bytes32::new([7; 32]), 1_000);
        let (peer, _receiver) = sim.connect_raw().await.expect("connect to simulator");
        let fetcher = PeerFetcher::new(peer, genesis(), Duration::from_secs(5));
        (sim, fetcher, coin)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coin_states_reads_an_inserted_coin() {
        let (_sim, fetcher, coin) = fetcher_over_sim().await;
        let states = fetcher
            .coin_states(vec![coin.coin_id()], false)
            .await
            .unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].coin, coin);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coin_states_of_unknown_coin_is_empty() {
        let (_sim, fetcher, _coin) = fetcher_over_sim().await;
        let states = fetcher
            .coin_states(vec![Bytes32::new([0xee; 32])], false)
            .await
            .unwrap();
        assert!(states.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn puzzle_states_reads_by_puzzle_hash() {
        let (_sim, fetcher, coin) = fetcher_over_sim().await;
        let filters = CoinStateFilters {
            include_spent: true,
            include_unspent: true,
            include_hinted: true,
            min_amount: 0,
        };
        let states = fetcher
            .puzzle_states(vec![coin.puzzle_hash], filters, false)
            .await
            .unwrap();
        assert!(states.iter().any(|s| s.coin == coin));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn children_reads_child_coins_of_a_parent() {
        let sim = PeerSimulator::new().await.unwrap();
        let parent = Bytes32::new([9; 32]);
        let child = Coin::new(parent, Bytes32::new([3; 32]), 5);
        sim.lock().await.insert_coin(child);
        let (peer, _receiver) = sim.connect_raw().await.unwrap();
        let fetcher = PeerFetcher::new(peer, genesis(), Duration::from_secs(5));

        let kids = fetcher.children(parent).await.unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].coin, child);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn puzzle_and_solution_of_unspent_coin_never_yields_a_spend() {
        let (_sim, fetcher, coin) = fetcher_over_sim().await;
        let result = fetcher.puzzle_and_solution(coin.coin_id(), 1).await;
        // An unspent coin has no reveal. Fail closed with `Err` — NEVER fabricate a spend, and never
        // a misleading `Ok` (the caller only asks after confirming a spent height).
        assert!(
            result.is_err(),
            "unspent coin must fail closed, not yield a reveal: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submitting_an_invalid_bundle_returns_a_failure_ack() {
        let (_sim, fetcher, _coin) = fetcher_over_sim().await;
        let bundle = SpendBundle::new(vec![], chia::bls::Signature::default());
        let status = fetcher.send_transaction(bundle).await.unwrap();
        assert_eq!(status, 3, "an empty bundle is rejected with a failure ack");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_then_remove_coin_subscriptions_succeeds() {
        let (_sim, fetcher, coin) = fetcher_over_sim().await;
        fetcher
            .coin_states(vec![coin.coin_id()], true)
            .await
            .unwrap();
        fetcher
            .remove_coin_subscriptions(vec![coin.coin_id()])
            .await
            .unwrap();
    }

    fn page(states: usize, height: u32, is_finished: bool) -> PuzzleStatePage {
        PuzzleStatePage {
            coin_states: (0..states)
                .map(|_| CoinState {
                    coin: Coin::new(Bytes32::new([1; 32]), Bytes32::new([2; 32]), 1),
                    created_height: Some(height),
                    spent_height: None,
                })
                .collect(),
            height,
            header_hash: Bytes32::new([height as u8; 32]),
            is_finished,
        }
    }

    #[tokio::test]
    async fn paging_that_never_finishes_fails_closed_not_hangs() {
        // A hostile peer that always advances height but never sets is_finished must hit the page cap.
        let mut next_height = 0u32;
        let result = collect_paged(Bytes32::default(), |_prev, _hdr| {
            next_height += 1;
            let h = next_height;
            async move { Ok(page(1, h, false)) }
        })
        .await;
        assert!(
            matches!(result, Err(ChiaPeerError::Rejected(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn paging_without_progress_fails_closed() {
        // A peer that returns unfinished pages at a NON-advancing height is rejected immediately.
        let result = collect_paged(Bytes32::default(), |_prev, _hdr| async move {
            Ok(page(1, 42, false))
        })
        .await;
        assert!(
            matches!(result, Err(ChiaPeerError::Rejected(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn paging_over_coin_cap_fails_closed() {
        let result = collect_paged(Bytes32::default(), |_prev, _hdr| async move {
            Ok(page(MAX_ACCUMULATED_COIN_STATES + 1, 1, false))
        })
        .await;
        assert!(
            matches!(result, Err(ChiaPeerError::Rejected(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn paging_finishes_normally_returns_all() {
        let mut calls = 0u32;
        let result = collect_paged(Bytes32::default(), |_prev, _hdr| {
            calls += 1;
            let finished = calls == 2;
            let h = calls;
            async move { Ok(page(1, h, finished)) }
        })
        .await
        .unwrap();
        assert_eq!(result.len(), 2, "both pages accumulated then finished");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_fetcher_fails_closed_on_every_read() {
        let fetcher = PeerFetcher::disconnected(genesis(), Duration::from_secs(1));
        let id = Bytes32::new([1; 32]);
        assert_eq!(
            fetcher.coin_states(vec![id], false).await,
            Err(ChiaPeerError::NotConnected)
        );
        let filters = CoinStateFilters {
            include_spent: true,
            include_unspent: true,
            include_hinted: true,
            min_amount: 0,
        };
        assert_eq!(
            fetcher.puzzle_states(vec![id], filters, false).await,
            Err(ChiaPeerError::NotConnected)
        );
        assert_eq!(fetcher.children(id).await, Err(ChiaPeerError::NotConnected));
        assert_eq!(
            fetcher.puzzle_and_solution(id, 1).await,
            Err(ChiaPeerError::NotConnected)
        );
        assert_eq!(
            fetcher.remove_coin_subscriptions(vec![id]).await,
            Err(ChiaPeerError::NotConnected)
        );
        assert_eq!(
            fetcher
                .send_transaction(SpendBundle::new(vec![], chia::bls::Signature::default()))
                .await,
            Err(ChiaPeerError::NotConnected)
        );
    }
}
