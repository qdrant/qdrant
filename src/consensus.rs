use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::Context;
use api::grpc::qdrant::raft_client::RaftClient;
use api::grpc::qdrant::{PeerId, RaftMessage as GrpcRaftMessage};
use api::grpc::transport_channel_pool::TransportChannelPool;
use collection::shard::ChannelService;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;
use raft::{SoftState, StateRole};
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::consensus_state::ConsensusStateRef;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::types::PeerAddressById;
use tokio::runtime::Runtime;
use tonic::transport::Uri;

use crate::common::telemetry::TonicTelemetryCollector;
use crate::settings::ConsensusConfig;
use crate::tonic::init_internal;

type Node = RawNode<ConsensusStateRef>;

pub enum Message {
    FromClient(ConsensusOperations),
    FromPeer(Box<RaftMessage>),
}

pub struct Consensus {
    node: Node,
    receiver: Receiver<Message>,
    runtime: Runtime,
    bootstrap_uri: Option<Uri>,
    config: ConsensusConfig,
    channel_service: ChannelService,
}

impl Consensus {
    /// Create and run consensus node
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        logger: &slog::Logger,
        state_ref: ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_host: String,
        p2p_port: u16,
        config: ConsensusConfig,
        channel_service: ChannelService,
        propose_receiver: mpsc::Receiver<ConsensusOperations>,
        telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
        toc: Arc<TableOfContent>,
    ) -> anyhow::Result<JoinHandle<std::io::Result<()>>> {
        let (mut consensus, message_sender) = Self::new(
            logger,
            state_ref.clone(),
            bootstrap_peer,
            uri,
            p2p_port,
            config,
            channel_service,
        )?;

        let state_ref_clone = state_ref.clone();
        thread::Builder::new()
            .name("consensus".to_string())
            .spawn(move || {
                if let Err(err) = consensus.start() {
                    log::error!("Consensus stopped with error: {err}");
                    state_ref_clone.on_consensus_thread_err(err);
                } else {
                    log::info!("Consensus stopped");
                    state_ref_clone.on_consensus_stopped();
                }
            })?;

        let message_sender_moved = message_sender.clone();
        thread::Builder::new()
            .name("forward-proposals".to_string())
            .spawn(move || {
                while let Ok(entry) = propose_receiver.recv() {
                    if message_sender_moved
                        .send(Message::FromClient(entry))
                        .is_err()
                    {
                        log::error!("Can not forward new entry to consensus as it was stopped.");
                        break;
                    }
                }
            })?;

        let handle = thread::Builder::new()
            .name("grpc_internal".to_string())
            .spawn(move || {
                init_internal(
                    toc,
                    state_ref,
                    telemetry_collector,
                    p2p_host,
                    p2p_port,
                    message_sender,
                )
            })
            .unwrap();

        Ok(handle)
    }

    /// If `bootstrap_peer` peer is supplied, then either `uri` or `p2p_port` should be also supplied
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        logger: &slog::Logger,
        state_ref: ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: u16,
        config: ConsensusConfig,
        channel_service: ChannelService,
    ) -> anyhow::Result<(Self, SyncSender<Message>)> {
        // raft will not return entries to the application smaller or equal to `applied`
        let last_applied = state_ref.last_applied_entry().unwrap_or_default();
        let raft_config = Config {
            id: state_ref.this_peer_id(),
            applied: last_applied,
            ..Default::default()
        };
        raft_config.validate()?;
        let op_wait = storage::content_manager::consensus_state::DEFAULT_META_OP_WAIT;
        // Commit might take up to 4 ticks as:
        // 1 tick - send proposal to leader
        // 2 tick - leader sends append entries to peers
        // 3 tick - peer answers leader, that entry is persisted
        // 4 tick - leader increases commit index and sends it
        if 4 * Duration::from_millis(config.tick_period_ms) > op_wait {
            log::warn!("With current tick period of {}ms, operation commit time might exceed default wait timeout: {}ms",
                 config.tick_period_ms, op_wait.as_millis())
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("consensus-tokio-rt-{}", id)
            })
            .enable_all()
            .build()?;
        let (sender, receiver) = mpsc::sync_channel(config.max_message_queue_size);
        // State might be initialized but the node might be shutdown without actually syncing or committing anything.
        let is_new_deployment = state_ref.hard_state().term == 0;
        if is_new_deployment {
            let leader_established_in_ms =
                config.tick_period_ms * raft_config.max_election_tick() as u64;
            Self::init(
                &state_ref,
                bootstrap_peer.clone(),
                uri,
                p2p_port,
                &config,
                &runtime,
                leader_established_in_ms,
            )
            .context("Failed to initialize Consensus for new Raft state")?;
        } else {
            if bootstrap_peer.is_some() || uri.is_some() {
                log::debug!("Local raft state found - bootstrap and uri cli arguments were ignored")
            }
            log::debug!("Local raft state found - skipping initialization");
        };
        let mut node = Node::new(&raft_config, state_ref.clone(), logger)?;
        // Before consensus has started apply any unapplied committed entries
        // They might have not been applied due to unplanned Qdrant shutdown
        let _stop_consensus = state_ref.apply_entries(&mut node);

        let consensus = Self {
            node,
            receiver,
            runtime,
            bootstrap_uri: bootstrap_peer,
            config,
            channel_service,
        };

        Ok((consensus, sender))
    }

    #[allow(clippy::too_many_arguments)]
    fn init(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
        runtime: &Runtime,
        leader_established_in_ms: u64,
    ) -> anyhow::Result<()> {
        if let Some(bootstrap_peer) = bootstrap_peer {
            log::debug!("Bootstrapping from peer with address: {bootstrap_peer}");
            runtime.block_on(Self::bootstrap(
                state_ref,
                bootstrap_peer,
                uri,
                p2p_port,
                config,
            ))?;
            Ok(())
        } else {
            log::debug!(
                "Bootstrapping is disabled. Assuming this peer is the first in the network"
            );
            let tick_period = config.tick_period_ms;
            log::info!("With current tick period of {tick_period}ms, leader will be established in approximately {leader_established_in_ms}ms. To avoid rejected operations - add peers and submit operations only after this period.");
            // First peer needs to add its own address
            state_ref.add_peer(
                state_ref.this_peer_id(),
                uri.ok_or_else(|| anyhow::anyhow!("First peer should specify its uri."))?
                    .parse()?,
            )?;
            Ok(())
        }
    }

    async fn bootstrap(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Uri,
        uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
    ) -> anyhow::Result<()> {
        // Use dedicated transport channel for bootstrapping because of specific timeout
        let channel = TransportChannelPool::make_channel(
            Duration::from_secs(config.bootstrap_timeout_sec),
            Duration::from_secs(config.bootstrap_timeout_sec),
            bootstrap_peer,
        )
        .await
        .context("Failed to create timeout channel")?;
        let mut client = RaftClient::new(channel);
        let id = state_ref.this_peer_id();
        let all_peers = client
            .add_peer_to_known(tonic::Request::new(
                api::grpc::qdrant::AddPeerToKnownMessage {
                    uri,
                    port: Some(p2p_port as u32),
                    id,
                },
            ))
            .await
            .context("Failed to add peer to known")?
            .into_inner();
        // Although peer addresses are synchronized with consensus, addresses need to be pre-fetched in the case of a new peer
        // or it will not know how to answer the Raft leader
        for peer in all_peers.all_peers {
            state_ref
                .add_peer(
                    peer.id,
                    peer.uri
                        .parse()
                        .context(format!("Failed to parse peer URI: {}", peer.uri))?,
                )
                .context("Failed to add peer")?;
        }
        // Only first peer has itself as a voter in the initial conf state.
        // This needs to be propagated manually to other peers as it is not contained in any log entry.
        state_ref.set_first_voter(all_peers.first_peer_id);
        state_ref.set_conf_state(ConfState::from((vec![all_peers.first_peer_id], vec![])))?;
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(self.config.tick_period_ms);

        loop {
            if !self
                .try_promote_learner()
                .context("Failed to promote learner")?
            {
                // If learner promotion was proposed - do not add other proposals.
                self.propose_updates(timeout)?;
            }
            let d = t.elapsed();
            t = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(self.config.tick_period_ms);
                // We drive Raft every `tick_period_ms`.
                self.node.tick();
                // Try to reapply entries if some were not applied due to errors.
                let store = self.node.store().clone();
                let stop_consensus = store.apply_entries(&mut self.node);
                if stop_consensus {
                    return Ok(());
                }
            } else {
                timeout -= d;
            }
            let stop_consensus = self.on_ready()?;
            if stop_consensus {
                return Ok(());
            }
        }
    }

    fn propose_updates(&mut self, timeout: Duration) -> anyhow::Result<()> {
        match self.receiver.recv_timeout(timeout) {
            Ok(Message::FromPeer(message)) => {
                if message.get_msg_type() == MessageType::MsgHeartbeat
                    || message.get_msg_type() == MessageType::MsgHeartbeatResponse
                {
                    log::trace!(
                        "Received a message from peer with progress: {:?}. Message: {:?}",
                        self.node.raft.prs().get(message.from),
                        message
                    );
                } else {
                    log::debug!(
                        "Received a message from peer with progress: {:?}. Message: {:?}",
                        self.node.raft.prs().get(message.from),
                        message
                    );
                }
                if let Err(error) = self.node.step(*message) {
                    log::warn!("Failed to step message: {:?}", error);
                }
            }
            Ok(Message::FromClient(operation)) => {
                let result = match operation {
                    ConsensusOperations::RemovePeer(peer_id) => {
                        let mut change = ConfChangeV2::default();
                        change.set_changes(vec![raft_proto::new_conf_change_single(
                            peer_id,
                            ConfChangeType::RemoveNode,
                        )]);
                        log::debug!("Proposing network configuration change: {:?}", change);
                        self.node.propose_conf_change(vec![], change)
                    }
                    ConsensusOperations::AddPeer(peer_id, uri) => {
                        let mut change = ConfChangeV2::default();
                        change.set_changes(vec![raft_proto::new_conf_change_single(
                            peer_id,
                            ConfChangeType::AddLearnerNode,
                        )]);
                        log::debug!("Proposing network configuration change: {:?}", change);
                        self.node.propose_conf_change(uri.into_bytes(), change)
                    }
                    _ => {
                        let message = match serde_cbor::to_vec(&operation) {
                            Ok(message) => message,
                            Err(err) => {
                                log::error!("Failed to serialize operation: {}", err);
                                return Ok(());
                            }
                        };
                        log::debug!("Proposing entry from client with length: {}", message.len());
                        self.node.propose(vec![], message)
                    }
                };

                match result {
                    Ok(_) => {}
                    Err(consensus_err) => {
                        // Do not stop consensus if client proposal failed.
                        log::error!("Failed to propose entry: {:?}", consensus_err);
                        return Ok(());
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => {
                log::warn!("Stopping Raft as message sender was dropped");
                return Ok(());
            }
        }
        Ok(())
    }

    /// Returns `true` if learner promotion was proposed, `false` otherwise.
    fn try_promote_learner(&mut self) -> anyhow::Result<bool> {
        let learner = if let Some(learner) = self.find_learner_to_promote() {
            learner
        } else {
            return Ok(false);
        };
        let store = self.node.store();
        let commit = store.hard_state().commit;
        let last_log_entry = store.last_index()?;
        // Promote only when there are no uncommitted changes.
        if commit != last_log_entry {
            return Ok(false);
        }
        let status = self.node.status();
        // Promote only if leader
        if status.ss.raft_state != StateRole::Leader {
            return Ok(false);
        }
        let mut change = ConfChangeV2::default();
        change.set_changes(vec![raft_proto::new_conf_change_single(
            learner,
            ConfChangeType::AddNode,
        )]);
        log::debug!("Proposing promotion for learner {learner} to voter");
        self.node.propose_conf_change(vec![], change)?;
        Ok(true)
    }

    fn find_learner_to_promote(&self) -> Option<u64> {
        let commit = self.node.store().hard_state().commit;
        let learners: HashSet<_> = self
            .node
            .store()
            .conf_state()
            .learners
            .into_iter()
            .collect();
        let status = self.node.status();
        status
            .progress?
            .iter()
            .find(|(id, progress)| learners.contains(id) && progress.matched == commit)
            .map(|(id, _)| *id)
    }

    /// Returns `true` if consensus should be stopped, `false` otherwise.
    fn on_ready(&mut self) -> anyhow::Result<bool> {
        if !self.node.has_ready() {
            return Ok(false);
        }
        self.store().record_consensus_working();
        // Get the `Ready` with `RawNode::ready` interface.
        let ready = self.node.ready();
        let (light_rd, role_change) = self.process_ready(ready)?;
        if let Some(light_ready) = light_rd {
            let result = self.process_light_ready(light_ready)?;
            if let Some(role_change) = role_change {
                self.process_role_change(role_change);
            }
            Ok(result)
        } else {
            // No light ready, so we need to stop consensus.
            Ok(true)
        }
    }

    fn process_role_change(&self, role_change: StateRole) {
        // Explicit match here for better readability
        match role_change {
            StateRole::Candidate | StateRole::PreCandidate => {
                self.store().is_leader_established.make_not_ready()
            }
            StateRole::Leader | StateRole::Follower => {
                self.store().is_leader_established.make_ready()
            }
        }
    }

    /// Tries to process raft's ready state.
    ///
    /// Returns with err on failure to apply the state.
    /// If it receives message to stop the consensus - returns None instead of LightReady.
    fn process_ready(
        &mut self,
        mut ready: raft::Ready,
    ) -> anyhow::Result<(Option<raft::LightReady>, Option<StateRole>)> {
        let store = self.store();
        let peer_address_by_id = store.peer_address_by_id();

        if !ready.messages().is_empty() {
            log::trace!("Handling {} messages", ready.messages().len());
            if let Err(err) = self.send_messages(ready.take_messages(), peer_address_by_id.clone())
            {
                log::error!("Failed to send messages: {err}")
            }
        }
        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            log::debug!("Applying snapshot");
            store
                .apply_snapshot(&ready.snapshot().clone())
                .context("Failed to apply snapshot")?
        }
        let stop_consensus =
            handle_committed_entries(ready.take_committed_entries(), &store, &mut self.node)
                .context("Failed to apply committed entries")?;

        if stop_consensus {
            return Ok((None, None));
        }

        if !ready.entries().is_empty() {
            // Append entries to the Raft log.
            log::debug!("Appending {} entries to raft log", ready.entries().len());
            store
                .append_entries(ready.take_entries())
                .context("Failed to append entries")?;
        }
        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            log::debug!("Changing hard state. New hard state: {hs:?}");
            store
                .set_hard_state(hs.clone())
                .context("Failed to set hard state")?;
        }
        let role_change = ready.ss().map(|ss| ss.raft_state);
        if let Some(ss) = ready.ss() {
            log::debug!("Changing soft state. New soft state: {ss:?}");
            self.handle_soft_state(ss);
        }
        if !ready.persisted_messages().is_empty() {
            log::trace!(
                "Handling {} persisted messages",
                ready.persisted_messages().len()
            );
            if let Err(err) =
                self.send_messages(ready.take_persisted_messages(), peer_address_by_id)
            {
                log::error!("Failed to send persisted messages: {err}")
            }
        }

        // Advance the Raft.
        let light_rd = self.node.advance(ready);
        Ok((Some(light_rd), role_change))
    }

    /// Tries to process raft's light ready state.
    ///
    /// Returns with err on failure to apply the state.
    /// If it receives message to stop the consensus - returns `true`, otherwise `false`.
    fn process_light_ready(&mut self, mut light_rd: raft::LightReady) -> anyhow::Result<bool> {
        let store = self.store();
        let peer_address_by_id = store.peer_address_by_id();
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            log::debug!("Updating commit index to {commit}");
            store
                .set_commit_index(commit)
                .context("Failed to set commit index")?;
        }
        if let Err(err) = self.send_messages(light_rd.take_messages(), peer_address_by_id) {
            log::error!("Failed to send messages: {err}")
        }
        // Apply all committed entries.
        let stop_consensus =
            handle_committed_entries(light_rd.take_committed_entries(), &store, &mut self.node)
                .context("Failed to apply committed entries")?;
        // Advance the apply index.
        self.node.advance_apply();
        Ok(stop_consensus)
    }

    fn store(&self) -> ConsensusStateRef {
        self.node.store().clone()
    }

    fn handle_soft_state(&self, state: &SoftState) {
        let store = self.node.store();
        store.set_raft_soft_state(state);
    }

    fn send_messages(
        &self,
        messages: Vec<RaftMessage>,
        peer_address_by_id: PeerAddressById,
    ) -> Result<(), StorageError> {
        let messages_with_address: Vec<_> = messages
            .into_iter()
            .map(|message| {
                let address = peer_address_by_id.get(&message.to).cloned();
                (message, address)
            })
            .collect();
        let bootstrap_uri = self.bootstrap_uri.clone();
        let consensus_config_arc = Arc::new(self.config.clone());
        let pool = self.channel_service.channel_pool.clone();
        let future = async move {
            let mut send_futures = Vec::new();
            for (message, address) in messages_with_address {
                let address = match address {
                    Some(address) => address,
                    None => match who_is(
                        message.to,
                        bootstrap_uri.clone(),
                        consensus_config_arc.clone(),
                    )
                    .await
                    {
                        Ok(address) => address,
                        Err(_) => {
                            log::warn!(
                                "Address of peer with ID {} not found. Message {:?} was not sent to it.",
                                message.to, message
                            );
                            continue;
                        }
                    },
                };
                send_futures.push(send_message(address, message, pool.clone()));
            }
            for result in futures::future::join_all(send_futures).await {
                if let Err(err) = result {
                    log::warn!("Failed to send message: {err:#}")
                }
            }
        };
        // Raft does not need the responses and should not wait for timeouts
        // so sending messages in parallel should be ok
        self.runtime.spawn(future);
        Ok(())
    }
}

/// This function actually applies the committed entries to the state machine.
/// Return `true` if consensus should be stopped.
/// `false` otherwise.
fn handle_committed_entries(
    entries: Vec<Entry>,
    state: &ConsensusStateRef,
    raw_node: &mut RawNode<ConsensusStateRef>,
) -> raft::Result<bool> {
    let mut stop_consensus = false;
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        state.set_unapplied_entries(first.index, last.index)?;
        stop_consensus = state.apply_entries(raw_node);
    }
    Ok(stop_consensus)
}

async fn who_is(
    peer_id: collection::shard::PeerId,
    bootstrap_uri: Option<Uri>,
    config: Arc<ConsensusConfig>,
) -> anyhow::Result<Uri> {
    log::debug!("Resolving who is {peer_id}");
    let bootstrap_uri =
        bootstrap_uri.ok_or_else(|| anyhow::anyhow!("No bootstrap uri supplied"))?;
    let bootstrap_timeout = Duration::from_secs(config.bootstrap_timeout_sec);
    // Use dedicated transport channel for who_is because of specific timeout
    let channel =
        TransportChannelPool::make_channel(bootstrap_timeout, bootstrap_timeout, bootstrap_uri)
            .await
            .context("Failed to create timeout channel")?;
    let mut client = RaftClient::new(channel);
    Ok(client
        .who_is(tonic::Request::new(PeerId { id: peer_id }))
        .await?
        .into_inner()
        .uri
        .parse()?)
}

async fn send_message(
    address: Uri,
    message: RaftMessage,
    transport_channel_pool: Arc<TransportChannelPool>,
) -> anyhow::Result<()> {
    let mut bytes = Vec::new();
    <RaftMessage as prost::Message>::encode(&message, &mut bytes)
        .context("Failed to serialize Raft message")?;
    let message = &GrpcRaftMessage { message: bytes };

    let _response = transport_channel_pool
        .with_channel(&address, |channel| async move {
            let mut client = RaftClient::new(channel);
            client.send(tonic::Request::new(message.clone())).await
        })
        .await
        .context("Failed to send message")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::thread;

    use collection::config::VectorParams;
    use collection::shard::ChannelService;
    use segment::types::Distance;
    use slog::Drain;
    use storage::content_manager::collection_meta_ops::{
        CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    };
    use storage::content_manager::consensus::operation_sender::OperationSender;
    use storage::content_manager::consensus::persistent::Persistent;
    use storage::content_manager::consensus_state::{ConsensusState, ConsensusStateRef};
    use storage::content_manager::toc::TableOfContent;
    use storage::dispatcher::Dispatcher;
    use tempfile::Builder;

    use super::Consensus;
    use crate::settings::ConsensusConfig;

    #[test]
    fn collection_creation_passes_consensus() {
        // Given
        let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
        let mut settings = crate::Settings::new().expect("Can't read config.");
        settings.storage.storage_path = storage_dir.path().to_str().unwrap().to_string();
        std::env::set_var("RUST_LOG", log::Level::Debug.as_str());
        env_logger::init();
        let runtime = crate::create_search_runtime(settings.storage.performance.max_search_threads)
            .expect("Can't create runtime.");
        let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
        let persistent_state =
            Persistent::load_or_init(&settings.storage.storage_path, true).unwrap();
        let operation_sender = OperationSender::new(propose_sender);
        let toc = TableOfContent::new(
            &settings.storage,
            runtime,
            ChannelService::default(),
            persistent_state.this_peer_id(),
            operation_sender.clone(),
        );
        let toc_arc = Arc::new(toc);
        let storage_path = toc_arc.storage_path();
        let consensus_state: ConsensusStateRef = ConsensusState::new(
            persistent_state,
            toc_arc.clone(),
            operation_sender,
            storage_path,
        )
        .into();
        let dispatcher = Dispatcher::new(toc_arc.clone()).with_consensus(consensus_state.clone());
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());
        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            consensus_state.clone(),
            None,
            Some("http://127.0.0.1:6335".parse().unwrap()),
            6335,
            ConsensusConfig::default(),
            ChannelService::default(),
        )
        .unwrap();

        let is_leader_established = consensus_state.is_leader_established.clone();
        thread::spawn(move || consensus.start().unwrap());
        thread::spawn(move || {
            while let Ok(entry) = propose_receiver.recv() {
                if message_sender
                    .send(super::Message::FromClient(entry))
                    .is_err()
                {
                    log::error!("Can not forward new entry to consensus as it was stopped.");
                    break;
                }
            }
        });
        // Wait for Raft to establish the leader
        is_leader_established.await_ready();
        // Leader election produces a raft log entry
        assert_eq!(consensus_state.hard_state().commit, 1);
        // Initially there are 0 collections
        assert_eq!(toc_arc.all_collections_sync().len(), 0);

        // When

        // New runtime is used as timers need to be enabled.
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(
                dispatcher.submit_collection_meta_op(
                    CollectionMetaOperations::CreateCollection(CreateCollectionOperation::new(
                        "test".to_string(),
                        CreateCollection {
                            vectors: VectorParams {
                                size: NonZeroU64::new(10).unwrap(),
                                distance: Distance::Cosine,
                            }
                            .into(),
                            hnsw_config: None,
                            wal_config: None,
                            optimizers_config: None,
                            shard_number: Some(2),
                            on_disk_payload: None,
                            replication_factor: None,
                        },
                    )),
                    None,
                ),
            )
            .unwrap();

        // Then
        assert_eq!(consensus_state.hard_state().commit, 2);
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
