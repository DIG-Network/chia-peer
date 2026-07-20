//! [`ChiaLightClient`] — the crate's public entry point: connect to a Chia full node as a
//! wallet-protocol light client, subscribe to coin/puzzle-hash state, track the peak, submit spend
//! bundles, and expose the read side as a [`ChiaPeerProvider`].
//!
//! It is a thin driver over the SDK: connecting, subscribing, and submitting are all SDK `Peer`
//! calls; this type adds the subscription cache, the drive-loop that keeps that cache current from
//! the peer's `CoinStateUpdate` stream, IPv6-first dialing, and reconnect-with-re-arm.

use std::sync::{Arc, Mutex};

use chia::traits::Streamable;
use chia_protocol::{
    Bytes32, CoinStateFilters, CoinStateUpdate, Message, NewPeakWallet, ProtocolMessageTypes,
    SpendBundle,
};
use dig_chainsource_interface::{ProviderId, ProviderInfo, ProviderKind};
use std::borrow::Cow;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tokio_tungstenite::Connector;

use crate::cache::CoinStateCache;
use crate::config::ChiaPeerConfig;
use crate::connect::{build_connector, connect};
use crate::error::ChiaPeerError;
use crate::fetcher::PeerFetcher;
use crate::provider::ChiaPeerProvider;

/// The default try-order priority a chia-peer provider registers with (lower = tried earlier).
pub const DEFAULT_PROVIDER_PRIORITY: i32 = 20;

/// The outcome of submitting a spend bundle, mapped from the node's `TransactionAck` status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Accepted into the mempool (ack status `1`) — pending block confirmation.
    Accepted,
    /// Held pending by the node (ack status `2`).
    Pending,
    /// Rejected by the node (ack status `3`).
    Failed,
    /// An unrecognised ack status byte.
    Unknown(u8),
}

impl SubmitOutcome {
    fn from_status(status: u8) -> Self {
        match status {
            1 => SubmitOutcome::Accepted,
            2 => SubmitOutcome::Pending,
            3 => SubmitOutcome::Failed,
            other => SubmitOutcome::Unknown(other),
        }
    }

    /// Whether the node took custody of the bundle (accepted or pending), rather than rejecting it.
    pub fn is_accepted(self) -> bool {
        matches!(self, SubmitOutcome::Accepted | SubmitOutcome::Pending)
    }
}

/// A connected Chia wallet-protocol light client.
pub struct ChiaLightClient {
    config: ChiaPeerConfig,
    tls: Connector,
    fetcher: Arc<PeerFetcher>,
    cache: Arc<RwLock<CoinStateCache>>,
    drive: Mutex<Option<JoinHandle<()>>>,
}

impl ChiaLightClient {
    /// Connects to a full node per `config` (IPv6-first, §5.2), starts the drive-loop that keeps the
    /// subscription cache current, and returns the ready client.
    pub async fn connect(config: ChiaPeerConfig) -> Result<Self, ChiaPeerError> {
        let tls = build_connector(&config)?;
        let (peer, receiver) = connect(&config, &tls).await?;
        Ok(Self::from_connection(config, tls, peer, receiver))
    }

    /// Assembles a ready client from an already-connected peer: builds the cache + fetcher and starts
    /// the drive-loop. Shared by [`connect`](Self::connect) and (in tests) the peer simulator.
    fn from_connection(
        config: ChiaPeerConfig,
        tls: Connector,
        peer: chia_wallet_sdk::client::Peer,
        receiver: mpsc::Receiver<Message>,
    ) -> Self {
        let cache = Arc::new(RwLock::new(CoinStateCache::new()));
        let fetcher = Arc::new(PeerFetcher::new(
            peer,
            config.network.genesis_challenge(),
            config.request_timeout,
        ));
        let drive = spawn_drive_loop(receiver, cache.clone());

        Self {
            config,
            tls,
            fetcher,
            cache,
            drive: Mutex::new(Some(drive)),
        }
    }

    /// Subscribes to `coin_ids`, seeds the cache with their current state, and returns that state.
    ///
    /// Wraps `request_coin_state(subscribe = true)`; future changes stream back via the drive-loop.
    pub async fn subscribe_coins(&self, coin_ids: Vec<Bytes32>) -> Result<(), ChiaPeerError> {
        use crate::fetcher::CoinStateFetcher;
        let states = self.fetcher.coin_states(coin_ids.clone(), true).await?;
        let mut cache = self.cache.write().await;
        cache.track_coins(coin_ids);
        cache.seed(states);
        Ok(())
    }

    /// Subscribes to every coin paying to `puzzle_hashes` under `filters`, seeding the cache.
    ///
    /// Wraps `request_puzzle_state(subscribe = true)` (paging until finished).
    pub async fn subscribe_puzzle_hashes(
        &self,
        puzzle_hashes: Vec<Bytes32>,
        filters: CoinStateFilters,
    ) -> Result<(), ChiaPeerError> {
        use crate::fetcher::CoinStateFetcher;
        let states = self
            .fetcher
            .puzzle_states(puzzle_hashes.clone(), filters, true)
            .await?;
        let mut cache = self.cache.write().await;
        cache.track_puzzle_hashes(puzzle_hashes);
        cache.seed(states);
        Ok(())
    }

    /// Submits `bundle` to the network, mapping the node's ack to a typed [`SubmitOutcome`].
    ///
    /// This is a WRITE path and is deliberately NOT part of the reads-only `ChainSource` surface.
    pub async fn submit_spend(&self, bundle: SpendBundle) -> Result<SubmitOutcome, ChiaPeerError> {
        let status = self.fetcher.send_transaction(bundle).await?;
        Ok(SubmitOutcome::from_status(status))
    }

    /// The current peak `(height, header_hash)` as tracked by the drive-loop, if known.
    pub async fn peak(&self) -> Option<(u32, Bytes32)> {
        self.cache.read().await.peak()
    }

    /// Removes the client's subscription to `coin_ids` (wraps `remove_coin_subscriptions`) and stops
    /// tracking them locally.
    pub async fn unsubscribe_coins(&self, coin_ids: Vec<Bytes32>) -> Result<(), ChiaPeerError> {
        self.fetcher
            .remove_coin_subscriptions(coin_ids.clone())
            .await?;
        self.cache.write().await.untrack_coins(&coin_ids);
        Ok(())
    }

    /// Reconnects to a (possibly different) full node and re-arms the existing subscription set, so a
    /// dropped connection recovers without the caller re-subscribing.
    pub async fn reconnect(&self) -> Result<(), ChiaPeerError> {
        let (peer, receiver) = connect(&self.config, &self.tls).await?;
        self.fetcher.swap_peer(peer).await;

        // Replace the drive-loop with one reading the new peer's stream.
        if let Some(previous) = self.drive.lock().expect("drive lock").take() {
            previous.abort();
        }
        let handle = spawn_drive_loop(receiver, self.cache.clone());
        *self.drive.lock().expect("drive lock") = Some(handle);

        self.rearm_subscriptions().await
    }

    /// Re-issues the tracked coin + puzzle-hash subscriptions against the current peer.
    async fn rearm_subscriptions(&self) -> Result<(), ChiaPeerError> {
        let (coins, puzzle_hashes) = {
            let cache = self.cache.read().await;
            (cache.subscribed_coins(), cache.subscribed_puzzle_hashes())
        };
        if !coins.is_empty() {
            self.subscribe_coins(coins).await?;
        }
        if !puzzle_hashes.is_empty() {
            let filters = CoinStateFilters {
                include_spent: true,
                include_unspent: true,
                include_hinted: true,
                min_amount: 0,
            };
            self.subscribe_puzzle_hashes(puzzle_hashes, filters).await?;
        }
        Ok(())
    }

    /// Exposes the read side as a [`ChiaPeerProvider`] for registration in a chain-source registry.
    ///
    /// `handle` MUST belong to a multi-thread tokio runtime (the sync facade blocks on it).
    pub fn as_chain_source_provider(&self, handle: tokio::runtime::Handle) -> ChiaPeerProvider {
        ChiaPeerProvider::new(
            self.fetcher.clone(),
            self.cache.clone(),
            handle,
            self.provider_info(),
        )
    }

    /// The provider descriptor this client registers with: a [`LocalNode`](ProviderKind::LocalNode)
    /// when pointed at the operator's own trusted node, else a [`Custom`](ProviderKind::Custom)
    /// introducer-discovered source. Always `trustless = false` (answers are taken on trust).
    pub fn provider_info(&self) -> ProviderInfo {
        let kind = if self.config.trusted {
            ProviderKind::LocalNode
        } else {
            ProviderKind::Custom
        };
        ProviderInfo {
            id: ProviderId(Cow::Borrowed("chia-peer")),
            kind,
            priority: DEFAULT_PROVIDER_PRIORITY,
            trustless: false,
        }
    }
}

impl Drop for ChiaLightClient {
    fn drop(&mut self) {
        if let Some(handle) = self.drive.lock().expect("drive lock").take() {
            handle.abort();
        }
    }
}

/// Spawns the background task that keeps `cache` current from a peer's inbound message stream:
/// `NewPeakWallet` advances the peak, `CoinStateUpdate` applies the reorg-aware state update and
/// drops spent coins from local tracking. Other message types are ignored.
fn spawn_drive_loop(
    mut receiver: mpsc::Receiver<Message>,
    cache: Arc<RwLock<CoinStateCache>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            match message.msg_type {
                ProtocolMessageTypes::NewPeakWallet => {
                    match NewPeakWallet::from_bytes(&message.data) {
                        Ok(peak) => cache.write().await.set_peak(peak.height, peak.header_hash),
                        // A malformed push is non-fatal (drop it), but log it to aid diagnosis.
                        Err(error) => log::debug!("undecodable NewPeakWallet push: {error}"),
                    }
                }
                ProtocolMessageTypes::CoinStateUpdate => {
                    match CoinStateUpdate::from_bytes(&message.data) {
                        Ok(update) => apply_coin_state_update(&cache, update).await,
                        Err(error) => log::debug!("undecodable CoinStateUpdate push: {error}"),
                    }
                }
                _ => {}
            }
        }
    })
}

/// Applies a decoded `CoinStateUpdate` to the cache and stops tracking any coins the update reports
/// as spent (their state is retained for reads; only the live subscription is dropped locally).
async fn apply_coin_state_update(cache: &RwLock<CoinStateCache>, update: CoinStateUpdate) {
    let spent: Vec<Bytes32> = update
        .items
        .iter()
        .filter(|state| state.spent_height.is_some())
        .map(|state| state.coin.coin_id())
        .collect();

    let mut cache = cache.write().await;
    cache.apply_update(
        &update.items,
        update.height,
        update.fork_height,
        update.peak_hash,
    );
    cache.untrack_coins(&spent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chia_protocol::{Coin, CoinState};

    #[test]
    fn submit_outcome_maps_ack_status() {
        assert_eq!(SubmitOutcome::from_status(1), SubmitOutcome::Accepted);
        assert_eq!(SubmitOutcome::from_status(2), SubmitOutcome::Pending);
        assert_eq!(SubmitOutcome::from_status(3), SubmitOutcome::Failed);
        assert_eq!(SubmitOutcome::from_status(9), SubmitOutcome::Unknown(9));
        assert!(SubmitOutcome::Accepted.is_accepted());
        assert!(SubmitOutcome::Pending.is_accepted());
        assert!(!SubmitOutcome::Failed.is_accepted());
    }

    fn coin(seed: u8) -> Coin {
        Coin::new(Bytes32::new([seed; 32]), Bytes32::new([seed ^ 2; 32]), 1)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn drive_loop_update_advances_cache_and_drops_spent_tracking() {
        let cache = Arc::new(RwLock::new(CoinStateCache::new()));
        let spent = coin(1);
        let spent_id = spent.coin_id();
        cache.write().await.track_coins([spent_id]);

        let update = CoinStateUpdate {
            height: 200,
            fork_height: 199,
            peak_hash: Bytes32::new([0xab; 32]),
            items: vec![CoinState {
                coin: spent,
                created_height: Some(100),
                spent_height: Some(150),
            }],
        };
        apply_coin_state_update(&cache, update).await;

        let cache = cache.read().await;
        assert_eq!(cache.peak(), Some((200, Bytes32::new([0xab; 32]))));
        assert!(
            cache.get(spent_id).is_some(),
            "spent coin state is retained for reads"
        );
        assert!(
            !cache.is_subscribed_coin(spent_id),
            "spent coin is untracked"
        );
    }
}

#[cfg(test)]
mod simulator_tests {
    use super::*;
    use chia_protocol::SpendBundle;
    use chia_wallet_sdk::test::PeerSimulator;
    use dig_chainsource_interface::ChainSource;
    use std::time::Duration;

    async fn client_over_sim() -> (PeerSimulator, ChiaLightClient, chia_protocol::Coin) {
        let sim = PeerSimulator::new().await.expect("start simulator");
        let coin = sim.lock().await.new_coin(Bytes32::new([7; 32]), 500);
        let (peer, receiver) = sim.connect_raw().await.expect("connect");
        let config = ChiaPeerConfig::testnet11();
        let tls = build_connector(&config).expect("connector");
        let client = ChiaLightClient::from_connection(config, tls, peer, receiver);
        (sim, client, coin)
    }

    /// Runs a blocking provider read off the async runtime (bridge's "outside a runtime" path).
    fn blocking_read<T: Send>(f: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|s| s.spawn(f).join().expect("thread"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_seeds_cache_and_provider_reads_it() {
        let (_sim, client, coin) = client_over_sim().await;
        client.subscribe_coins(vec![coin.coin_id()]).await.unwrap();

        let provider = client.as_chain_source_provider(tokio::runtime::Handle::current());
        let id = coin.coin_id();
        let record = blocking_read(move || provider.coin_record(id)).unwrap();
        assert!(record.is_some(), "subscribed coin is served from cache");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drive_loop_tracks_peak_from_new_peak_wallet() {
        let (_sim, client, _coin) = client_over_sim().await;
        let mut peak = None;
        for _ in 0..100 {
            if let Some(p) = client.peak().await {
                peak = Some(p);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            peak.is_some(),
            "the drive-loop records a peak from NewPeakWallet"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_invalid_bundle_reports_failure() {
        let (_sim, client, _coin) = client_over_sim().await;
        let outcome = client
            .submit_spend(SpendBundle::new(vec![], chia::bls::Signature::default()))
            .await
            .unwrap();
        assert_eq!(outcome, SubmitOutcome::Failed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_puzzle_hashes_and_unsubscribe_coins() {
        let (_sim, client, coin) = client_over_sim().await;
        let filters = CoinStateFilters {
            include_spent: true,
            include_unspent: true,
            include_hinted: true,
            min_amount: 0,
        };
        client
            .subscribe_puzzle_hashes(vec![coin.puzzle_hash], filters)
            .await
            .unwrap();
        client.subscribe_coins(vec![coin.coin_id()]).await.unwrap();
        client
            .unsubscribe_coins(vec![coin.coin_id()])
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn provider_info_reflects_trusted_configuration() {
        let sim = PeerSimulator::new().await.unwrap();
        let (peer, receiver) = sim.connect_raw().await.unwrap();
        let endpoint = "127.0.0.1:8444".parse().unwrap();
        let config = ChiaPeerConfig::testnet11().with_trusted_endpoint(endpoint);
        let tls = build_connector(&config).unwrap();
        let client = ChiaLightClient::from_connection(config, tls, peer, receiver);
        assert_eq!(client.provider_info().kind, ProviderKind::LocalNode);
    }
}
