use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Context;
use api::grpc::qdrant::raft_client::RaftClient;
use api::grpc::qdrant::{PeerId, RaftMessage as GrpcRaftMessage};
use api::grpc::transport_channel_pool::TransportChannelPool;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;
use raft::{SoftState, StateRole};
use storage::content_manager::consensus_ops::ConsensusOperations;
use storage::content_manager::consensus_state::ConsensusStateRef;
use storage::content_manager::errors::StorageError;
use tokio::runtime::Runtime;
use tonic::transport::Uri;

use crate::common::helpers::IsReady;
use crate::settings::ConsensusConfig;

type Node = RawNode<ConsensusStateRef>;

pub enum Message {
    FromClient(Vec<u8>),
    FromPeer(Box<RaftMessage>),
    ConfChange(ConfChangeV2),
}

pub struct Consensus {
    node: Node,
    receiver: Receiver<Message>,
    runtime: Runtime,
    bootstrap_uri: Option<Uri>,
    config: ConsensusConfig,
    pub is_leader_established: Arc<IsReady>,
    transport_channel_pool: Arc<TransportChannelPool>,
}

impl Consensus {
    /// If `bootstrap_peer` peer is supplied, then either `uri` or `p2p_port` should be also supplied
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        logger: &slog::Logger,
        state_ref: ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: Option<u32>,
        config: ConsensusConfig,
        transport_channel_pool: Arc<TransportChannelPool>,
        state_just_initialized: bool,
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
        let is_leader_established = Arc::new(IsReady::default());
        let (sender, receiver) = mpsc::sync_channel(config.max_message_queue_size);
        // State might be initialized but the node might be shutdown without actually syncing or committing anything.
        let is_new_deployment = state_just_initialized || state_ref.hard_state().term == 0;
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
                sender.clone(),
                is_leader_established.clone(),
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
        state_ref.apply_entries(&mut node);
        Ok((
            Self {
                node,
                receiver,
                runtime,
                bootstrap_uri: bootstrap_peer,
                config,
                is_leader_established,
                transport_channel_pool,
            },
            sender,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn init(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: Option<u32>,
        config: &ConsensusConfig,
        runtime: &Runtime,
        sender: SyncSender<Message>,
        is_leader_established: Arc<IsReady>,
        leader_established_in_ms: u64,
    ) -> anyhow::Result<()> {
        if let Some(bootstrap_peer) = bootstrap_peer {
            log::debug!("Bootstrapping from peer with address: {bootstrap_peer}");
            if uri.is_none() && p2p_port.is_none() {
                return Err(anyhow::anyhow!("Failed to bootstrap peer as neither `internal rpc port` was configured nor `this peer uri` was supplied"));
            }
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
            let message = Message::FromClient(serde_cbor::to_vec(&ConsensusOperations::AddPeer(
                state_ref.this_peer_id(),
                uri.ok_or_else(|| anyhow::anyhow!("First peer should specify its uri."))?,
            ))?);
            thread::spawn(move || {
                // Wait for the leader to be established
                is_leader_established.await_ready();
                if let Err(err) = sender.send(message) {
                    log::error!("Failed to send message to add this peer to known peers: {err}")
                }
            });
            Ok(())
        }
    }

    async fn bootstrap(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Uri,
        uri: Option<String>,
        p2p_port: Option<u32>,
        config: &ConsensusConfig,
    ) -> anyhow::Result<()> {
        // Use dedicated transport channel for bootstrapping because of specific timeout
        let channel = TransportChannelPool::make_channel(
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
                    port: p2p_port,
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
        client
            .add_peer_as_participant(tonic::Request::new(api::grpc::qdrant::PeerId { id }))
            .await
            .context("Failed to add peer as participant")?;
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
                store.apply_entries(&mut self.node);
            } else {
                timeout -= d;
            }
            self.on_ready();
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
                self.node.step(*message)?
            }
            Ok(Message::FromClient(message)) => {
                log::debug!("Proposing entry from client with length: {}", message.len());
                self.node.propose(vec![], message)?
            }
            Ok(Message::ConfChange(change)) => {
                log::debug!("Proposing network configuration change: {:?}", change);
                self.node.propose_conf_change(vec![], change)?
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

    fn on_ready(&mut self) {
        if !self.node.has_ready() {
            return;
        }

        let store = self.node.store().clone();
        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = self.node.ready();
        if !ready.messages().is_empty() {
            log::trace!("Handling {} messages", ready.messages().len());
            if let Err(err) = self.handle_messages(ready.take_messages(), &store) {
                log::error!("Failed to send messages: {err}")
            }
        }
        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            log::debug!("Applying snapshot");
            if let Err(err) = store.apply_snapshot(&ready.snapshot().clone()) {
                log::error!("Failed to apply snapshot: {err}")
            }
        }
        if let Err(err) =
            handle_committed_entries(ready.take_committed_entries(), &store, &mut self.node)
        {
            log::error!("Failed to apply committed entries: {err}")
        }
        if !ready.entries().is_empty() {
            // Append entries to the Raft log.
            log::debug!("Appending {} entries to raft log", ready.entries().len());
            if let Err(err) = store.append_entries(ready.take_entries()) {
                log::error!("Failed to append entries: {err}")
            }
        }
        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            log::debug!("Changing hard state. New hard state: {hs:?}");
            if let Err(err) = store.set_hard_state(hs.clone()) {
                log::error!("Failed to set hard state: {err}")
            }
        }
        if let Some(ss) = ready.ss() {
            log::debug!("Changing soft state. New soft state: {ss:?}");
            self.handle_soft_state(ss);
        }
        if !ready.persisted_messages().is_empty() {
            log::trace!(
                "Handling {} persisted messages",
                ready.persisted_messages().len()
            );
            if let Err(err) = self.handle_messages(ready.take_persisted_messages(), &store) {
                log::error!("Failed to send persisted messages: {err}")
            }
        }

        // Advance the Raft.
        let mut light_rd = self.node.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            log::debug!("Updating commit index to {commit}");
            if let Err(err) = store.set_commit_index(commit) {
                log::error!("Failed to set commit index: {err}")
            }
        }
        if let Err(err) = self.handle_messages(light_rd.take_messages(), &store) {
            log::error!("Failed to send messages: {err}")
        }
        // Apply all committed entries.
        if let Err(err) =
            handle_committed_entries(light_rd.take_committed_entries(), &store, &mut self.node)
        {
            log::error!("Failed to apply committed entries: {err}")
        }
        // Advance the apply index.
        self.node.advance_apply();
    }

    fn handle_soft_state(&self, state: &SoftState) {
        self.node.store().set_raft_soft_state(state);
        if state.raft_state == StateRole::Leader || state.raft_state == StateRole::Follower {
            self.is_leader_established.make_ready()
        } else {
            self.is_leader_established.make_not_ready()
        }
    }

    fn handle_messages(
        &self,
        messages: Vec<RaftMessage>,
        state: &ConsensusStateRef,
    ) -> Result<(), StorageError> {
        let peer_address_by_id = state.peer_address_by_id();
        let messages_with_address: Vec<_> = messages
            .into_iter()
            .map(|message| {
                let address = peer_address_by_id.get(&message.to).cloned();
                (message, address)
            })
            .collect();
        let bootstrap_uri = self.bootstrap_uri.clone();
        let consensus_config_arc = Arc::new(self.config.clone());
        let pool = self.transport_channel_pool.clone();
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
                                "Address of peer with ID {} not found. Message was not sent to it.",
                                message.to
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

fn handle_committed_entries(
    entries: Vec<Entry>,
    state: &ConsensusStateRef,
    raw_node: &mut RawNode<ConsensusStateRef>,
) -> raft::Result<()> {
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        state.set_unapplied_entries(first.index, last.index)?;
        state.apply_entries(raw_node);
    }
    Ok(())
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
    let channel = TransportChannelPool::make_channel(bootstrap_timeout, bootstrap_uri)
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
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use api::grpc::transport_channel_pool::TransportChannelPool;
    use collection::shard::ChannelService;
    use segment::types::Distance;
    use slog::Drain;
    use storage::content_manager::collection_meta_ops::{
        CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    };
    use storage::content_manager::consensus::persistent::Persistent;
    use storage::content_manager::consensus_state::{ConsensusState, ConsensusStateRef};
    use storage::content_manager::toc::TableOfContent;
    use storage::dispatcher::Dispatcher;
    use tempdir::TempDir;

    use super::Consensus;
    use crate::settings::ConsensusConfig;

    #[test]
    fn collection_creation_passes_consensus() {
        // Given
        let storage_dir = TempDir::new("storage").unwrap();
        let mut settings = crate::Settings::new().expect("Can't read config.");
        settings.storage.storage_path = storage_dir.path().to_str().unwrap().to_string();
        std::env::set_var("RUST_LOG", log::Level::Debug.as_str());
        env_logger::init();
        let runtime = crate::create_search_runtime(settings.storage.performance.max_search_threads)
            .expect("Can't create runtime.");
        let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
        let (persistent_state, state_just_initialized) =
            Persistent::load_or_init(&settings.storage.storage_path, true).unwrap();
        let toc = TableOfContent::new(
            &settings.storage,
            runtime,
            ChannelService::default(),
            persistent_state.this_peer_id(),
        );
        let toc_arc = Arc::new(toc);
        let storage_path = toc_arc.storage_path();
        let consensus_state: ConsensusStateRef = ConsensusState::new(
            persistent_state,
            toc_arc.clone(),
            propose_sender,
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
            None,
            ConsensusConfig::default(),
            Arc::new(TransportChannelPool::default()),
            state_just_initialized,
        )
        .unwrap();
        let is_leader_established = consensus.is_leader_established.clone();
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
        // And also wait for it to commit its own address
        thread::sleep(Duration::from_secs(10));
        // Leader election produces a raft log entry
        // Address commit also produces a raft log entry
        assert_eq!(consensus_state.hard_state().commit, 2);
        // Initially there are 0 collections
        assert_eq!(toc_arc.all_collections_sync().len(), 0);

        // When

        // New runtime is used as timers need to be enabled.
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(dispatcher.submit_collection_meta_op(
                CollectionMetaOperations::CreateCollection(CreateCollectionOperation {
                    collection_name: "test".to_string(),
                    create_collection: CreateCollection {
                        vector_size: 10,
                        distance: Distance::Cosine,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: Some(2),
                        on_disk_payload: None,
                    },
                }),
                None,
            ))
            .unwrap();

        // Then
        assert_eq!(consensus_state.hard_state().commit, 3);
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
