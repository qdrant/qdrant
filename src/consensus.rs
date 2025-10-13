use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{cmp, fmt, thread};

use anyhow::{Context as _, anyhow};
use api::grpc::dynamic_channel_pool::make_grpc_channel;
use api::grpc::qdrant::raft_client::RaftClient;
use api::grpc::qdrant::{AllPeers, PeerId as GrpcPeerId, RaftMessage as GrpcRaftMessage};
use api::grpc::transport_channel_pool::TransportChannelPool;
use collection::shards::channel_service::ChannelService;
use collection::shards::shard::PeerId;
#[cfg(target_os = "linux")]
use common::cpu::linux_high_thread_priority;
use raft::eraftpb::Message as RaftMessage;
use raft::prelude::*;
use raft::{INVALID_ID, SoftState, StateRole};
use storage::content_manager::consensus_manager::ConsensusStateRef;
use storage::content_manager::consensus_ops::{ConsensusOperations, SnapshotStatus};
use storage::content_manager::toc::TableOfContent;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::sleep;
use tonic::transport::{ClientTlsConfig, Uri};

use crate::common::helpers;
use crate::common::telemetry_ops::requests_telemetry::TonicTelemetryCollector;
use crate::settings::{ConsensusConfig, Settings};
use crate::tonic::init_internal;

type Node = RawNode<ConsensusStateRef>;

const RECOVERY_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const RECOVERY_MAX_RETRY_COUNT: usize = 3;

pub enum Message {
    FromClient(ConsensusOperations),
    FromPeer(Box<RaftMessage>),
}

/// Aka Consensus Thread
/// Manages proposed changes to consensus state, ensures that everything is ordered properly
pub struct Consensus {
    /// Raft structure which handles raft-related state
    node: Node,
    /// Receives proposals from peers and client for applying in consensus
    receiver: Receiver<Message>,
    /// Runtime for async message sending
    runtime: Handle,
    /// Uri to some other known peer, used to join the consensus
    /// ToDo: Make if many
    config: ConsensusConfig,
    broker: RaftMessageBroker,
    raft_config: Config,
}

impl Consensus {
    /// Create and run consensus node
    #[allow(clippy::too_many_arguments)]
    pub fn run(
        logger: &slog::Logger,
        state_ref: ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        settings: Settings,
        channel_service: ChannelService,
        propose_receiver: mpsc::Receiver<ConsensusOperations>,
        telemetry_collector: Arc<parking_lot::Mutex<TonicTelemetryCollector>>,
        toc: Arc<TableOfContent>,
        runtime: Handle,
        reinit: bool,
    ) -> anyhow::Result<JoinHandle<std::io::Result<()>>> {
        let tls_client_config = helpers::load_tls_client_config(&settings)?;

        let p2p_host = settings.service.host.clone();
        let p2p_port = settings.cluster.p2p.port.expect("P2P port is not set");
        let config = settings.cluster.consensus.clone();

        let (mut consensus, message_sender) = Self::new(
            logger,
            state_ref.clone(),
            bootstrap_peer,
            uri,
            p2p_port,
            config,
            tls_client_config,
            channel_service,
            runtime.clone(),
            reinit,
        )?;

        let state_ref_clone = state_ref.clone();
        thread::Builder::new()
            .name("consensus".to_string())
            .spawn(move || {
                // On Linux, try to use high thread priority because consensus is important
                // Likely fails as we cannot set a higher priority by default due to permissions
                #[cfg(target_os = "linux")]
                if let Err(err) = linux_high_thread_priority() {
                    log::debug!(
                        "Failed to set high thread priority for consensus, ignoring: {err}"
                    );
                }

                if let Err(err) = consensus.start() {
                    log::error!("Consensus stopped with error: {err:#}");
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
                // On Linux, try to use high thread priority because consensus is important
                // Likely fails as we cannot set a higher priority by default due to permissions
                #[cfg(target_os = "linux")]
                if let Err(err) = linux_high_thread_priority() {
                    log::debug!(
                        "Failed to set high thread priority for consensus, ignoring: {err}"
                    );
                }

                while let Ok(entry) = propose_receiver.recv() {
                    if message_sender_moved
                        .blocking_send(Message::FromClient(entry))
                        .is_err()
                    {
                        log::error!("Can not forward new entry to consensus as it was stopped.");
                        break;
                    }
                }
            })?;

        let server_tls = if settings.cluster.p2p.enable_tls {
            let tls_config = settings
                .tls
                .clone()
                .ok_or_else(Settings::tls_config_is_undefined_error)?;

            Some(helpers::load_tls_internal_server_config(&tls_config)?)
        } else {
            None
        };

        let handle = thread::Builder::new()
            .name("grpc_internal".to_string())
            .spawn(move || {
                init_internal(
                    toc,
                    state_ref,
                    telemetry_collector,
                    settings,
                    p2p_host,
                    p2p_port,
                    server_tls,
                    message_sender,
                    runtime,
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
        tls_config: Option<ClientTlsConfig>,
        channel_service: ChannelService,
        runtime: Handle,
        reinit: bool,
    ) -> anyhow::Result<(Self, Sender<Message>)> {
        // If we want to re-initialize consensus, we need to prevent other peers
        // from re-playing consensus WAL operations, as they should already have them applied.
        // Do ensure that we are forcing compacting WAL on the first re-initialized peer,
        // which should trigger snapshot transferring instead of replaying WAL.
        let force_compact_wal = reinit && bootstrap_peer.is_none();

        // On the bootstrap-ed peers during reinit of the consensus
        // we want to make sure only the bootstrap peer will hold the true state
        // Therefore we clear the WAL on the bootstrap peer to force it to request a snapshot
        let clear_wal = reinit && bootstrap_peer.is_some();

        if clear_wal {
            log::debug!("Clearing WAL on the bootstrap peer to force snapshot transfer");
            state_ref.clear_wal()?;
        }

        // raft will not return entries to the application smaller or equal to `applied`
        let last_applied = state_ref.last_applied_entry().unwrap_or_default();
        let raft_config = Config {
            id: state_ref.this_peer_id(),
            applied: last_applied,
            ..Default::default()
        };
        raft_config.validate()?;
        // bounded channel for backpressure
        let (sender, receiver) = tokio::sync::mpsc::channel(config.max_message_queue_size);
        // State might be initialized but the node might be shutdown without actually syncing or committing anything.
        if state_ref.is_new_deployment() || reinit {
            let leader_established_in_ms =
                config.tick_period_ms * raft_config.max_election_tick() as u64;
            Self::init(
                &state_ref,
                bootstrap_peer.clone(),
                uri,
                p2p_port,
                &config,
                tls_config.clone(),
                &runtime,
                leader_established_in_ms,
            )
            .map_err(|err| anyhow!("Failed to initialize Consensus for new Raft state: {err}"))?;
        } else {
            runtime
                .block_on(Self::recover(
                    &state_ref,
                    uri.clone(),
                    p2p_port,
                    &config,
                    tls_config.clone(),
                ))
                .map_err(|err| {
                    anyhow!("Failed to recover Consensus from existing Raft state: {err}")
                })?;

            if bootstrap_peer.is_some() || uri.is_some() {
                log::debug!("Local raft state found - bootstrap and uri cli arguments were ignored")
            }
            log::debug!("Local raft state found - skipping initialization");
        };

        let mut node = Node::new(&raft_config, state_ref.clone(), logger)?;
        node.set_batch_append(true);

        // Before consensus has started apply any unapplied committed entries
        // They might have not been applied due to unplanned Qdrant shutdown
        let _stop_consensus = state_ref.apply_entries(&mut node)?;

        if force_compact_wal {
            // Making sure that the WAL will be compacted on start
            state_ref.compact_wal(1)?;
        } else {
            state_ref.compact_wal(config.compact_wal_entries)?;
        }

        let broker = RaftMessageBroker::new(
            runtime.clone(),
            bootstrap_peer,
            tls_config,
            config.clone(),
            node.store().clone(),
            channel_service.channel_pool,
        );

        let consensus = Self {
            node,
            receiver,
            runtime,
            config,
            broker,
            raft_config,
        };

        if !state_ref.is_new_deployment() {
            state_ref.recover_first_voter()?;
        }

        Ok((consensus, sender))
    }

    #[allow(clippy::too_many_arguments)]
    fn init(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
        tls_config: Option<ClientTlsConfig>,
        runtime: &Handle,
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
                tls_config,
            ))?;
            Ok(())
        } else {
            log::debug!(
                "Bootstrapping is disabled. Assuming this peer is the first in the network"
            );
            let tick_period = config.tick_period_ms;
            log::info!(
                "With current tick period of {tick_period}ms, leader will be established in approximately {leader_established_in_ms}ms. To avoid rejected operations - add peers and submit operations only after this period.",
            );
            // First peer needs to add its own address
            state_ref.add_peer(
                state_ref.this_peer_id(),
                uri.ok_or_else(|| anyhow::anyhow!("First peer should specify its uri."))?
                    .parse()?,
            )?;
            Ok(())
        }
    }

    async fn add_peer_to_known_for(
        this_peer_id: PeerId,
        cluster_uri: Uri,
        current_uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
        tls_config: Option<ClientTlsConfig>,
    ) -> anyhow::Result<AllPeers> {
        // Use dedicated transport channel for bootstrapping because of specific timeout
        let channel = make_grpc_channel(
            Duration::from_secs(config.bootstrap_timeout_sec),
            Duration::from_secs(config.bootstrap_timeout_sec),
            cluster_uri,
            tls_config,
        )
        .await
        .map_err(|err| anyhow!("Failed to create timeout channel: {err}"))?;
        let mut client = RaftClient::new(channel);
        let all_peers = client
            .add_peer_to_known(tonic::Request::new(
                api::grpc::qdrant::AddPeerToKnownMessage {
                    uri: current_uri,
                    port: Some(u32::from(p2p_port)),
                    id: this_peer_id,
                },
            ))
            .await
            .map_err(|err| anyhow!("Failed to add peer to known: {err}"))?
            .into_inner();
        Ok(all_peers)
    }

    // Re-attach peer to the consensus:
    // Notifies the cluster(any node) that this node changed its address
    async fn recover(
        state_ref: &ConsensusStateRef,
        uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
        tls_config: Option<ClientTlsConfig>,
    ) -> anyhow::Result<()> {
        let this_peer_id = state_ref.this_peer_id();
        let mut peer_to_uri = state_ref
            .persistent
            .read()
            .peer_address_by_id
            .read()
            .clone();
        let this_peer_url = peer_to_uri.remove(&this_peer_id);
        // Recover url if a different one is provided
        let do_recover = match (&this_peer_url, &uri) {
            (Some(this_peer_url), Some(uri)) => this_peer_url != &Uri::from_str(uri)?,
            _ => false,
        };

        if do_recover {
            let mut tries = RECOVERY_MAX_RETRY_COUNT;
            while tries > 0 {
                // Try to inform any peer about the change of address
                for (peer_id, peer_uri) in &peer_to_uri {
                    let res = Self::add_peer_to_known_for(
                        this_peer_id,
                        peer_uri.clone(),
                        uri.clone(),
                        p2p_port,
                        config,
                        tls_config.clone(),
                    )
                    .await;
                    if res.is_err() {
                        log::warn!(
                            "Failed to recover from peer with id {peer_id} at {peer_uri} with error {res:?}, trying others"
                        );
                    } else {
                        log::debug!(
                            "Successfully recovered from peer with id {peer_id} at {peer_uri}"
                        );
                        return Ok(());
                    }
                }
                tries -= 1;
                log::warn!(
                    "Retrying recovering from known peers (retry {})",
                    RECOVERY_MAX_RETRY_COUNT - tries
                );
                let exp_timeout =
                    RECOVERY_RETRY_TIMEOUT * (RECOVERY_MAX_RETRY_COUNT - tries) as u32;
                sleep(exp_timeout).await;
            }
            return Err(anyhow::anyhow!("Failed to recover from any known peers"));
        }

        Ok(())
    }

    /// Add node sequence:
    ///
    /// 1. Add current node as a learner
    /// 2. Start applying entries from consensus
    /// 3. Eventually leader submits the promotion proposal
    /// 4. Learners become voters once they read about the promotion from consensus log
    async fn bootstrap(
        state_ref: &ConsensusStateRef,
        bootstrap_peer: Uri,
        uri: Option<String>,
        p2p_port: u16,
        config: &ConsensusConfig,
        tls_config: Option<ClientTlsConfig>,
    ) -> anyhow::Result<()> {
        let this_peer_id = state_ref.this_peer_id();
        let all_peers = Self::add_peer_to_known_for(
            this_peer_id,
            bootstrap_peer,
            uri.clone(),
            p2p_port,
            config,
            tls_config,
        )
        .await?;

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
                .map_err(|err| anyhow!("Failed to add peer: {err}"))?
        }
        // Only first peer has itself as a voter in the initial conf state.
        // This needs to be propagated manually to other peers as it is not contained in any log entry.
        // So we skip the learner phase for the first peer.
        state_ref.set_first_voter(all_peers.first_peer_id)?;
        state_ref.set_conf_state(ConfState::from((vec![all_peers.first_peer_id], vec![])))?;
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        // If this is the only peer in the cluster, tick Raft node a few times to instantly
        // self-elect itself as Raft leader
        if self.node.store().peer_count() == 1 {
            while !self.node.has_ready() {
                self.node.tick();
            }
        }

        // If this is the origin peer of the cluster, try to add origin peer to consensus
        if let Err(err) = self.try_add_origin() {
            log::error!("Failed to add origin peer to consensus: {err}");
        }

        let tick_period = Duration::from_millis(self.config.tick_period_ms);
        let mut previous_tick = Instant::now();
        let mut idle_cycles = 0_usize;

        loop {
            // Wait (for up to `tick_period`) for incoming client requests and Raft messages
            let raft_messages = self.advance_node(tick_period)?;

            // Calculate how many ticks passed since the last one
            let elapsed_ticks = previous_tick.elapsed().div_duration_f32(tick_period) as u32;

            // Update previous tick timestamp
            previous_tick += tick_period * elapsed_ticks;

            // Calculate how many ticks we should *report* to Raft node.
            //
            // If last iteration of the loop took too long to complete, and we report all elapsed
            // ticks to Raft node, it might trigger unnecessary leader election.
            //
            // To prevent this, we check if we received new Raft messages (i.e., we are still
            // connected to Raft leader), and cap how many ticks we report to Raft node.
            //
            // By default, election is triggered if no Raft messages were received for 20 ticks,
            // so we report at most 15 ticks.
            //
            // See https://docs.rs/raft/latest/raft/struct.Config.html#structfield.election_tick.
            let report_ticks = if raft_messages > 0 {
                // Default `election_tick` is 20, so expected value here is 15
                let max_elapsed_ticks =
                    cmp::max(1, self.raft_config.election_tick.saturating_sub(5));

                cmp::min(elapsed_ticks, max_elapsed_ticks as u32)
            } else {
                elapsed_ticks
            };

            // Report elapsed ticks to Raft node
            for _ in 0..report_ticks {
                self.node.tick();
            }

            // Append new entries to the WAL, apply committed entries, etc...
            let (stop_consensus, is_idle) = self.on_ready()?;

            if stop_consensus {
                return Ok(());
            }

            // If we only sent outgoing Raft messages, but did not change any state during `on_ready`,
            // we consider Raft node to be "idle"
            if is_idle {
                // If current node is the only peer in the cluster, or if we received new Raft messages
                // (i.e., we are still connected to Raft leader/peers), and Raft node is idle,
                // count "idle cycle"
                if raft_messages > 0 || (self.is_single_peer() && self.is_leader()) {
                    idle_cycles += 1;
                }
            } else {
                // If Raft state was updated, reset idle cycle counter
                idle_cycles = 0;
            }

            // If Raft node was idle for 3 cycles, try to sync local state to consensus
            if idle_cycles >= 3 {
                self.try_sync_local_state()?;
            }
        }
    }

    fn advance_node(&mut self, tick_period: Duration) -> anyhow::Result<usize> {
        if self
            .try_promote_learner()
            .context("failed to promote learner")?
        {
            return Ok(0);
        }

        // This method propagates incoming client requests and Raft messages to Raft node

        // It's more efficient to process multiple events, so we propagate up to 128 events at a time
        const RAFT_BATCH_SIZE: usize = 128;

        // We have to tick Raft node periodically, so we can wait for new events for up to `tick_period`
        let hard_timeout_at = Instant::now() + tick_period;

        // We also want to react to new events as quickly as possible, so we only wait for `tick_period / 10`
        // for any consecutive events after the first one
        let consecutive_message_timeout = tick_period / 10;

        // Timeout to wait for the *next* event
        let mut timeout_at = hard_timeout_at;

        // Track how many *events* we received...
        let mut events = 0;
        // ...and how many of these events were *Raft messages*
        let mut raft_messages = 0;

        loop {
            let Ok(message) = self.recv_update(timeout_at) else {
                break;
            };

            // When we discover conf-change request, we have to break early and process it ASAP,
            // because Raft node allows to process single conf-change request at a time.
            //
            // E.g., without this condition, if two nodes try to join cluster at the same time and
            // both conf-change requests are processed in the same batch, the second request would
            // be ignored and the node would fail to join.
            let is_conf_change = matches!(
                message,
                Message::FromClient(
                    ConsensusOperations::AddPeer { .. } | ConsensusOperations::RemovePeer(_)
                ),
            );

            let is_raft_message = matches!(message, Message::FromPeer(_));

            if let Err(err) = self.advance_node_impl(message) {
                log::warn!("{err}");
                continue;
            }

            timeout_at = cmp::min(
                hard_timeout_at,
                Instant::now() + consecutive_message_timeout,
            );

            events += 1;
            raft_messages += usize::from(is_raft_message);

            if events >= RAFT_BATCH_SIZE || is_conf_change {
                break;
            }
        }

        Ok(raft_messages)
    }

    fn recv_update(&mut self, timeout_at: Instant) -> Result<Message, TryRecvUpdateError> {
        self.runtime.block_on(async {
            tokio::select! {
                biased;
                message = self.receiver.recv() => message.ok_or(TryRecvUpdateError::Closed),
                _ = tokio::time::sleep_until(timeout_at.into()) => Err(TryRecvUpdateError::Timeout),
            }
        })
    }

    fn advance_node_impl(&mut self, message: Message) -> anyhow::Result<()> {
        match message {
            Message::FromClient(ConsensusOperations::AddPeer { peer_id, uri }) => {
                let existing_uris = self
                    .broker
                    .consensus_state
                    .peer_address_by_id()
                    .into_iter()
                    .map(|(peer_id, url)| (url, peer_id))
                    .collect::<HashMap<_, _>>();

                // Don't allow a peer URI to join if already in consensus
                // - new URIs can always join
                // - existing URIs can re-join with the same peer ID
                // See: <https://github.com/qdrant/qdrant/pull/7375>
                if let Some(registered_peer_id) =
                    existing_uris.get(&uri.parse::<Uri>().context("peer URI is not a valid URI")?)
                    && registered_peer_id != &peer_id
                {
                    log::warn!(
                        "Rejected peer {peer_id} to join consensus, URI is already registered by peer {registered_peer_id} ({uri})",
                    );
                    return Err(anyhow!(
                        "peer URI {uri} already used by peer {registered_peer_id}, remove it first or use a different URI",
                    ));
                }

                let mut change = ConfChangeV2::default();

                change.set_changes(vec![raft_proto::new_conf_change_single(
                    peer_id,
                    ConfChangeType::AddLearnerNode,
                )]);

                log::debug!("Proposing network configuration change: {change:?}");
                self.node
                    .propose_conf_change(uri.into_bytes(), change)
                    .context("failed to propose conf change")?;
            }

            Message::FromClient(ConsensusOperations::RemovePeer(peer_id)) => {
                let mut change = ConfChangeV2::default();

                change.set_changes(vec![raft_proto::new_conf_change_single(
                    peer_id,
                    ConfChangeType::RemoveNode,
                )]);

                log::debug!("Proposing network configuration change: {change:?}");
                self.node
                    .propose_conf_change(vec![], change)
                    .context("failed to propose conf change")?;
            }

            Message::FromClient(ConsensusOperations::RequestSnapshot) => {
                self.node
                    .request_snapshot()
                    .context("failed to request snapshot")?;
            }

            Message::FromClient(ConsensusOperations::ReportSnapshot { peer_id, status }) => {
                self.node.report_snapshot(peer_id, status.into());
            }

            Message::FromClient(operation) => {
                let data =
                    serde_cbor::to_vec(&operation).context("failed to serialize operation")?;

                log::trace!("Proposing entry from client with length: {}", data.len());
                self.node
                    .propose(vec![], data)
                    .context("failed to propose entry")?;
            }

            Message::FromPeer(message) => {
                let is_heartbeat = matches!(
                    message.get_msg_type(),
                    MessageType::MsgHeartbeat | MessageType::MsgHeartbeatResponse,
                );

                if !is_heartbeat {
                    log::trace!(
                        "Received a message from peer with progress: {:?}. Message: {:?}",
                        self.node.raft.prs().get(message.from),
                        message,
                    );
                }

                self.node.step(*message).context("failed to step message")?;
            }
        }

        Ok(())
    }

    fn is_single_peer(&self) -> bool {
        self.node.store().peer_count() == 1
    }

    fn is_leader(&self) -> bool {
        self.node.status().ss.raft_state == raft::StateRole::Leader
    }

    fn try_sync_local_state(&self) -> anyhow::Result<()> {
        if !self.node.has_ready() {
            // No updates to process
            let store = self.node.store();
            let pending_operations = store.persistent.read().unapplied_entities_count();
            if pending_operations == 0 && store.is_leader_established.check_ready() {
                // If leader is established and there is nothing else to do on this iteration,
                // then we can check if there are any un-synchronized local state left.
                store.sync_local_state()?;
            }
        }
        Ok(())
    }

    /// Tries to propose "origin peer" (the very first peer, that starts new cluster) to consensus
    fn try_add_origin(&mut self) -> Result<bool, TryAddOriginError> {
        // We can determine origin peer from consensus state:
        // - it should be the only peer in the cluster
        // - and its commit index should be at 0 or 1
        //
        // When we add a new node to existing cluster, we have to bootstrap it from existing cluster
        // node, and during bootstrap we explicitly add all current peers to consensus state. So,
        // *all* peers added to the cluster after the origin will always have at least two peers.
        //
        // When origin peer starts new cluster, it self-elects itself as a leader and commits empty
        // operation with index 1. It is impossible to commit anything to consensus before this
        // operation is committed. And to add another (second/third/etc) peer to the cluster, we
        // have to commit a conf-change operation. Which means that only origin peer can ever be at
        // commit index 0 or 1.

        // Check that we are the only peer in the cluster
        if self.node.store().peer_count() > 1 {
            return Ok(false);
        }

        let status = self.node.status();

        // Check that we are at index 0 or 1
        if status.hs.commit > 1 {
            return Ok(false);
        }

        // If we reached this point, we are the origin peer, but it's impossible to propose anything
        // to consensus, before leader is elected (`propose_conf_change` will return an error),
        // so we have to wait for a few ticks for self-election
        if status.ss.raft_state != StateRole::Leader {
            return Err(TryAddOriginError::NotLeader);
        }

        // Propose origin peer to consensus
        let mut change = ConfChangeV2::default();

        change.set_changes(vec![raft_proto::new_conf_change_single(
            status.id,
            ConfChangeType::AddNode,
        )]);

        let peer_uri = self
            .node
            .store()
            .persistent
            .read()
            .peer_address_by_id
            .read()
            .get(&status.id)
            .ok_or_else(|| TryAddOriginError::UriNotFound)?
            .to_string();

        self.node.propose_conf_change(peer_uri.into(), change)?;

        Ok(true)
    }

    /// Returns `true` if learner promotion was proposed, `false` otherwise.
    /// Learner node does not vote on elections, cause it might not have a big picture yet.
    /// So consensus should guarantee that learners are promoted one-by-one.
    /// Promotions are done by leader and only after it has no pending entries,
    /// that guarantees that learner will start voting only after it applies all the changes in the log
    fn try_promote_learner(&mut self) -> anyhow::Result<bool> {
        // Promote only if leader
        if self.node.status().ss.raft_state != StateRole::Leader {
            return Ok(false);
        }

        // Promote only when there are no uncommitted changes.
        let store = self.node.store();
        let commit = store.hard_state().commit;
        let last_log_entry = store.last_index()?;

        if commit != last_log_entry {
            return Ok(false);
        }

        let Some(learner) = self.find_learner_to_promote() else {
            return Ok(false);
        };

        log::debug!("Proposing promotion for learner {learner} to voter");

        let mut change = ConfChangeV2::default();

        change.set_changes(vec![raft_proto::new_conf_change_single(
            learner,
            ConfChangeType::AddNode,
        )]);

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

    /// Returns two boolean flags: `stop_consensus` and `is_idle`.
    /// If `stop_consensus` is true, then we should exit consensus loop and stop consensus.
    /// If `is_idle` is true, it means that no on-disk state was updated during this `on_ready` call.
    fn on_ready(&mut self) -> anyhow::Result<(bool, bool)> {
        if !self.node.has_ready() {
            // No updates to process
            return Ok((false, true));
        }

        self.store().record_consensus_working();

        // Get the `Ready` with `RawNode::ready` interface.
        let ready = self.node.ready();

        let (Some(light_ready), role_change, is_idle_ready) = self.process_ready(ready)? else {
            // No light ready, so we need to stop consensus.
            return Ok((true, false));
        };

        let (stop_consensus, is_idle_light_ready) = self.process_light_ready(light_ready)?;

        if let Some(role_change) = role_change {
            self.process_role_change(role_change);
        }

        self.store().compact_wal(self.config.compact_wal_entries)?;

        Ok((stop_consensus, is_idle_ready && is_idle_light_ready))
    }

    fn process_role_change(&self, role_change: StateRole) {
        // Explicit match here for better readability
        match role_change {
            StateRole::Candidate | StateRole::PreCandidate => {
                self.store().is_leader_established.make_not_ready()
            }
            StateRole::Leader | StateRole::Follower => {
                if self.node.raft.leader_id != INVALID_ID {
                    self.store().is_leader_established.make_ready()
                } else {
                    self.store().is_leader_established.make_not_ready()
                }
            }
        }
    }

    /// Tries to process raft's ready state. Happens on each tick.
    ///
    /// The order of operations in this functions is critical, changing it might lead to bugs.
    ///
    /// Returns with err on failure to apply the state.
    /// If it receives message to stop the consensus - returns None instead of LightReady.
    fn process_ready(
        &mut self,
        mut ready: raft::Ready,
    ) -> anyhow::Result<(Option<raft::LightReady>, Option<StateRole>, bool)> {
        let store = self.store();

        // We consider Raft node to be idle if we don't change Raft state during `process_ready`.
        //
        // E.g.:
        // - sending messages does not change Raft state, so it's considered idle
        // - but anything else does, and so is not idle
        let mut is_idle = true;

        if !ready.messages().is_empty() {
            log::trace!("Handling {} messages", ready.messages().len());
            self.send_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            let snapshot = ready.snapshot().clone();
            log::debug!("Applying snapshot {:?}", snapshot.get_metadata());
            is_idle = false;

            if let Err(err) = store.apply_snapshot(&snapshot)? {
                log::error!("Failed to apply snapshot: {err}");
            }
        }

        if !ready.entries().is_empty() {
            // Append entries to the Raft log.
            log::debug!("Appending {} entries to raft log", ready.entries().len());
            is_idle = false;

            store
                .append_entries(ready.take_entries())
                .map_err(|err| anyhow!("Failed to append entries: {err}"))?
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            log::debug!("Changing hard state. New hard state: {hs:?}");
            is_idle = false;

            store
                .set_hard_state(hs.clone())
                .map_err(|err| anyhow!("Failed to set hard state: {err}"))?
        }

        let role_change = ready.ss().map(|ss| ss.raft_state);

        if let Some(ss) = ready.ss() {
            log::debug!("Changing soft state. New soft state: {ss:?}");
            is_idle = false;

            self.handle_soft_state(ss);
        }

        if !ready.persisted_messages().is_empty() {
            log::trace!(
                "Handling {} persisted messages",
                ready.persisted_messages().len()
            );

            self.send_messages(ready.take_persisted_messages());
        }

        let committed_entries = ready.take_committed_entries();
        is_idle &= committed_entries.is_empty();

        // Should be done after Hard State is saved, so that `applied` index is never bigger than `commit`.
        let stop_consensus = handle_committed_entries(&committed_entries, &store, &mut self.node)
            .context("Failed to handle committed entries")?;

        if stop_consensus {
            return Ok((None, None, false));
        }

        // Advance the Raft.
        let light_rd = self.node.advance(ready);
        Ok((Some(light_rd), role_change, is_idle))
    }

    /// Tries to process raft's light ready state.
    ///
    /// The order of operations in this functions is critical, changing it might lead to bugs.
    ///
    /// Returns with err on failure to apply the state.
    /// If it receives message to stop the consensus - returns `true`, otherwise `false`.
    fn process_light_ready(
        &mut self,
        mut light_rd: raft::LightReady,
    ) -> anyhow::Result<(bool, bool)> {
        let store = self.store();

        // We consider Raft node to be idle, if we don't change Raft state during `process_light_ready`.
        //
        // E.g.:
        // - sending messages does not change Raft state, so it's considered idle
        // - but anything else does, and so is not idle
        let mut is_idle = true;

        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            log::debug!("Updating commit index to {commit}");
            is_idle = false;

            store
                .set_commit_index(commit)
                .map_err(|err| anyhow!("Failed to set commit index: {err}"))?;
        }

        self.send_messages(light_rd.take_messages());

        let committed_entries = light_rd.take_committed_entries();
        is_idle &= committed_entries.is_empty();

        // Apply all committed entries.
        let stop_consensus = handle_committed_entries(&committed_entries, &store, &mut self.node)
            .context("Failed to apply committed entries")?;

        // Advance the apply index.
        self.node.advance_apply();
        Ok((stop_consensus, is_idle))
    }

    fn store(&self) -> ConsensusStateRef {
        self.node.store().clone()
    }

    fn handle_soft_state(&self, state: &SoftState) {
        let store = self.node.store();
        store.set_raft_soft_state(state);
    }

    fn send_messages(&mut self, messages: Vec<RaftMessage>) {
        self.broker.send(messages);
    }
}

#[derive(Copy, Clone, Debug, thiserror::Error)]
enum TryRecvUpdateError {
    #[error("timeout elapsed")]
    Timeout,

    #[error("channel closed")]
    Closed,
}

#[derive(Debug, thiserror::Error)]
enum TryAddOriginError {
    #[error("origin peer is not a leader")]
    NotLeader,

    #[error("origin peer URI not found")]
    UriNotFound,

    #[error("failed to propose origin peer URI to consensus: {0}")]
    RaftError(#[from] raft::Error),
}

/// This function actually applies the committed entries to the state machine.
/// Return `true` if consensus should be stopped.
/// `false` otherwise.
fn handle_committed_entries(
    entries: &[Entry],
    state: &ConsensusStateRef,
    raw_node: &mut RawNode<ConsensusStateRef>,
) -> anyhow::Result<bool> {
    let mut stop_consensus = false;
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        state.set_unapplied_entries(first.index, last.index)?;
        stop_consensus = state.apply_entries(raw_node)?;
    }
    Ok(stop_consensus)
}

struct RaftMessageBroker {
    senders: HashMap<PeerId, RaftMessageSenderHandle>,
    runtime: Handle,
    bootstrap_uri: Option<Uri>,
    tls_config: Option<ClientTlsConfig>,
    consensus_config: Arc<ConsensusConfig>,
    consensus_state: ConsensusStateRef,
    transport_channel_pool: Arc<TransportChannelPool>,
}

impl RaftMessageBroker {
    pub fn new(
        runtime: Handle,
        bootstrap_uri: Option<Uri>,
        tls_config: Option<ClientTlsConfig>,
        consensus_config: ConsensusConfig,
        consensus_state: ConsensusStateRef,
        transport_channel_pool: Arc<TransportChannelPool>,
    ) -> Self {
        Self {
            senders: HashMap::new(),
            runtime,
            bootstrap_uri,
            tls_config,
            consensus_config: consensus_config.into(),
            consensus_state,
            transport_channel_pool,
        }
    }

    pub fn send(&mut self, messages: impl IntoIterator<Item = RaftMessage>) {
        let mut messages = messages.into_iter();
        let mut retry = None;

        while let Some(message) = retry.take().or_else(|| messages.next()) {
            let peer_id = message.to;

            let sender = match self.senders.get_mut(&peer_id) {
                Some(sender) => sender,
                None => {
                    log::debug!("Spawning message sender task for peer {peer_id}...");

                    let (task, handle) = self.message_sender();
                    let future = self.runtime.spawn(task.exec());
                    drop(future); // drop `JoinFuture` explicitly to make clippy happy

                    self.senders.insert(peer_id, handle);

                    self.senders
                        .get_mut(&peer_id)
                        .expect("message sender task spawned")
                }
            };

            let failed_to_forward = |message: &RaftMessage, description: &str| {
                let peer_id = message.to;

                let is_debug = log::max_level() >= log::Level::Debug;
                let space = if is_debug { " " } else { "" };
                let message: &dyn fmt::Debug = if is_debug { &message } else { &"" }; // TODO: `fmt::Debug` for `""` prints `""`... 😒

                log::error!(
                    "Failed to forward message{space}{message:?} to message sender task {peer_id}: \
                     {description}"
                );
            };

            match sender.send(message).map_err(|err| *err) {
                Ok(()) => (),

                Err(tokio::sync::mpsc::error::TrySendError::Full((_, message))) => {
                    failed_to_forward(
                        &message,
                        "message sender task queue is full. Message will be dropped.",
                    );
                }

                Err(tokio::sync::mpsc::error::TrySendError::Closed((_, message))) => {
                    failed_to_forward(
                        &message,
                        "message sender task queue is closed. \
                         Message sender task will be restarted and message will be retried.",
                    );

                    self.senders.remove(&peer_id);
                    retry = Some(message);
                }
            }
        }
    }

    fn message_sender(&self) -> (RaftMessageSender, RaftMessageSenderHandle) {
        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(128);
        let (heartbeat_tx, heartbeat_rx) = tokio::sync::watch::channel(Default::default());

        let task = RaftMessageSender {
            messages: messages_rx,
            heartbeat: heartbeat_rx,
            bootstrap_uri: self.bootstrap_uri.clone(),
            tls_config: self.tls_config.clone(),
            consensus_config: self.consensus_config.clone(),
            consensus_state: self.consensus_state.clone(),
            transport_channel_pool: self.transport_channel_pool.clone(),
        };

        let handle = RaftMessageSenderHandle {
            messages: messages_tx,
            heartbeat: heartbeat_tx,
            index: 0,
        };

        (task, handle)
    }
}

#[derive(Debug)]
struct RaftMessageSenderHandle {
    messages: Sender<(usize, RaftMessage)>,
    heartbeat: watch::Sender<(usize, RaftMessage)>,
    index: usize,
}

impl RaftMessageSenderHandle {
    fn send(&mut self, message: RaftMessage) -> RaftMessageSenderResult<()> {
        if !is_heartbeat(&message) {
            self.messages
                .try_send((self.index, message))
                .map_err(Box::new)?;
        } else {
            self.heartbeat.send((self.index, message)).map_err(
                |watch::error::SendError(message)| {
                    Box::new(tokio::sync::mpsc::error::TrySendError::Closed(message))
                },
            )?;
        }

        self.index += 1;

        Ok(())
    }
}

type RaftMessageSenderResult<T, E = RaftMessageSenderError> = Result<T, E>;
type RaftMessageSenderError = Box<tokio::sync::mpsc::error::TrySendError<(usize, RaftMessage)>>;

struct RaftMessageSender {
    messages: Receiver<(usize, RaftMessage)>,
    heartbeat: watch::Receiver<(usize, RaftMessage)>,
    bootstrap_uri: Option<Uri>,
    tls_config: Option<ClientTlsConfig>,
    consensus_config: Arc<ConsensusConfig>,
    consensus_state: ConsensusStateRef,
    transport_channel_pool: Arc<TransportChannelPool>,
}

impl RaftMessageSender {
    pub async fn exec(mut self) {
        // Imagine that `raft` crate put four messages to be sent to some other Raft node into
        // `RaftMessageSender`'s queue:
        //
        // | 4: AppendLog | 3: Heartbeat | 2: Heartbeat | 1: AppendLog |
        //
        // Heartbeat is the most basic message type in Raft. It only carries common "metadata"
        // without any additional "payload". And all other message types in Raft also carry
        // the same basic metadata as the heartbeat message.
        //
        // This way, message `3` instantly "outdates" message `2`: they both carry the same data
        // fields, but message `3` was produced more recently, and so it might contain newer values
        // of these data fields.
        //
        // And because all messages carry the same basic data as the heartbeat message, message `4`
        // instantly "outdates" both message `2` and `3`.
        //
        // This way, if there are more than one message queued for the `RaftMessageSender`,
        // we can optimize delivery a bit and skip any heartbeat message if there's a more
        // recent message scheduled later in the queue.
        //
        // `RaftMessageSender` have two separate "queues":
        // - `messages` queue for non-heartbeat messages
        // - and `heartbeat` "watch" channel for heartbeat messages
        //   - "watch" is a special channel in Tokio, that only retains the *last* sent value
        //   - so any heartbeat received from the `heartbeat` channel is always the *most recent* one
        //
        // We are using `tokio::select` to "simultaneously" check both queues for new messages...
        // but we are using `tokio::select` in a "biased" mode!
        //
        // - in this mode select always polls `messages.recv()` future first
        // - so even if there are new messages in both queues, it will always return a non-heartbeat
        //   message from `messages` queue first
        // - and it will only return a heartbeat message from `heartbeat` channel if there's no
        //   messages left in the `messages` queue
        //
        // There's one special case that we should be careful about with our two queues:
        //
        // If we return to the diagram above, and imagine four messages were sent in the same order
        // into our two queues, then `RaftMessageSender` might pull them from the queues in the
        // `1`, `4`, `3` order.
        //
        // E.g., we pull non-heartbeat messages `1` and `4` first, heartbeat `2` was overwritten
        // by heartbeat `3` (because of the "watch" channel), so once `messages` queue is empty
        // we receive heartbeat `3`, which is now out-of-order.
        //
        // To handle this we explicitly enumerate each message and only send a message if its index
        // is higher-or-equal than the index of a previous one. (This check can be expressed with
        // both strict "higher" or "higher-or-equal" conditional, I just like the "or-equal" version
        // a bit better.)
        //
        // If either `messages` queue or `heartbeat` channel is closed (e.g., `messages.recv()`
        // returns `None` or `heartbeat.changed()` returns an error), we assume that
        // `RaftMessageSenderHandle` has been dropped, and treat it as a "shutdown"/"cancellation"
        // signal (and break from the loop).

        let mut prev_index = 0;

        loop {
            let (index, message) = tokio::select! {
                biased;
                Some(message) = self.messages.recv() => message,
                Ok(()) = self.heartbeat.changed() => self.heartbeat.borrow_and_update().clone(),
                else => break,
            };

            if prev_index <= index {
                self.send(&message).await;
                prev_index = index;
            }
        }
    }

    async fn send(&self, message: &RaftMessage) {
        if let Err(err) = self.try_send(message).await {
            let peer_id = message.to;

            if log::max_level() >= log::Level::Debug {
                log::error!("Failed to send Raft message {message:?} to peer {peer_id}: {err}");
            } else {
                log::error!("Failed to send Raft message to peer {peer_id}: {err}");
            }
        }
    }

    async fn try_send(&self, message: &RaftMessage) -> anyhow::Result<()> {
        let peer_id = message.to;

        let uri = self.uri(peer_id).await?;
        let bytes = <RaftMessage as prost_for_raft::Message>::encode_to_vec(message);
        let grpc_message = GrpcRaftMessage { message: bytes };

        let timeout = Duration::from_millis(
            self.consensus_config.message_timeout_ticks * self.consensus_config.tick_period_ms,
        );

        let res = self
            .transport_channel_pool
            .with_channel_timeout(
                &uri,
                |channel| async {
                    let mut client = RaftClient::new(channel);
                    let mut request = tonic::Request::new(grpc_message.clone());
                    request.set_timeout(timeout);
                    client.send(request).await
                },
                Some(timeout),
                0,
            )
            .await;

        if message.msg_type == raft::eraftpb::MessageType::MsgSnapshot as i32 {
            let res = self.consensus_state.report_snapshot(
                peer_id,
                if res.is_ok() {
                    SnapshotStatus::Finish
                } else {
                    SnapshotStatus::Failure
                },
            );

            // Should we ignore the error? Seems like it will only produce noise.
            //
            // - `send_message` is only called by the sub-task spawned by the consensus thread.
            // - `report_snapshot` sends a message back to the consensus thread.
            // - It can only fail, if the "receiver" end of the channel is closed.
            // - Which means consensus thread either resolved successfully, or failed.
            // - So, if the consensus thread is shutting down, no need to log a misleading error...
            // - ...or, if the consensus thread failed, then we should already have an error,
            //   and it will only produce more noise.

            if let Err(err) = res {
                log::error!("{err}");
            }
        }

        match res {
            Ok(_) => self.consensus_state.record_message_send_success(&uri),
            Err(err) => self.consensus_state.record_message_send_failure(&uri, err),
        }

        Ok(())
    }

    async fn uri(&self, peer_id: PeerId) -> anyhow::Result<Uri> {
        let uri = self
            .consensus_state
            .peer_address_by_id()
            .get(&peer_id)
            .cloned();

        match uri {
            Some(uri) => Ok(uri),
            None => self.who_is(peer_id).await,
        }
    }

    async fn who_is(&self, peer_id: PeerId) -> anyhow::Result<Uri> {
        let bootstrap_uri = self
            .bootstrap_uri
            .clone()
            .ok_or_else(|| anyhow::format_err!("No bootstrap URI provided"))?;

        let bootstrap_timeout = Duration::from_secs(self.consensus_config.bootstrap_timeout_sec);

        // Use dedicated transport channel for who_is because of specific timeout
        let channel = make_grpc_channel(
            bootstrap_timeout,
            bootstrap_timeout,
            bootstrap_uri,
            self.tls_config.clone(),
        )
        .await
        .map_err(|err| anyhow::format_err!("Failed to create who-is channel: {err}"))?;

        let uri = RaftClient::new(channel)
            .who_is(tonic::Request::new(GrpcPeerId { id: peer_id }))
            .await?
            .into_inner()
            .uri
            .parse()?;

        Ok(uri)
    }
}

fn is_heartbeat(message: &RaftMessage) -> bool {
    message.msg_type == raft::eraftpb::MessageType::MsgHeartbeat as i32
        || message.msg_type == raft::eraftpb::MessageType::MsgHeartbeatResponse as i32
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;

    use collection::operations::vector_params_builder::VectorParamsBuilder;
    use collection::shards::channel_service::ChannelService;
    use common::budget::ResourceBudget;
    use segment::types::Distance;
    use slog::Drain;
    use storage::content_manager::collection_meta_ops::{
        CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
    };
    use storage::content_manager::consensus::operation_sender::OperationSender;
    use storage::content_manager::consensus::persistent::Persistent;
    use storage::content_manager::consensus_manager::{ConsensusManager, ConsensusStateRef};
    use storage::content_manager::toc::TableOfContent;
    use storage::dispatcher::Dispatcher;
    use storage::rbac::Access;
    use tempfile::Builder;

    use super::Consensus;
    use crate::common::helpers::create_general_purpose_runtime;
    use crate::settings::ConsensusConfig;

    #[test]
    fn collection_creation_passes_consensus() {
        // Given
        let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();
        let mut settings = crate::Settings::new(None).expect("Can't read config.");
        settings.storage.storage_path = storage_dir.path().to_str().unwrap().to_string();
        tracing_subscriber::fmt::init();
        let search_runtime =
            crate::create_search_runtime(settings.storage.performance.max_search_threads)
                .expect("Can't create search runtime.");
        let update_runtime =
            crate::create_update_runtime(settings.storage.performance.max_search_threads)
                .expect("Can't create update runtime.");
        let general_runtime =
            create_general_purpose_runtime().expect("Can't create general purpose runtime.");
        let handle = general_runtime.handle().clone();
        let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
        let persistent_state =
            Persistent::load_or_init(&settings.storage.storage_path, true, false, None).unwrap();
        let operation_sender = OperationSender::new(propose_sender);
        let toc = TableOfContent::new(
            &settings.storage,
            search_runtime,
            update_runtime,
            general_runtime,
            ResourceBudget::default(),
            ChannelService::new(settings.service.http_port, None),
            persistent_state.this_peer_id(),
            Some(operation_sender.clone()),
        );
        let toc_arc = Arc::new(toc);
        let storage_path = toc_arc.storage_path();
        let consensus_state: ConsensusStateRef = ConsensusManager::new(
            persistent_state,
            toc_arc.clone(),
            operation_sender,
            Path::new(storage_path),
        )
        .into();
        let dispatcher =
            Dispatcher::new(toc_arc.clone()).with_consensus(consensus_state.clone(), true);
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());
        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            consensus_state.clone(),
            None,
            Some("http://127.0.0.1:6335".parse().unwrap()),
            6335,
            ConsensusConfig::default(),
            None,
            ChannelService::new(settings.service.http_port, None),
            handle.clone(),
            false,
        )
        .unwrap();

        let is_leader_established = consensus_state.is_leader_established.clone();
        thread::spawn(move || consensus.start().unwrap());
        thread::spawn(move || {
            while let Ok(entry) = propose_receiver.recv() {
                if message_sender
                    .blocking_send(super::Message::FromClient(entry))
                    .is_err()
                {
                    log::error!("Can not forward new entry to consensus as it was stopped.");
                    break;
                }
            }
        });
        // Wait for Raft to establish the leader
        is_leader_established.await_ready();
        // Leader election produces a raft log entry, and then origin peer adds itself to consensus
        assert_eq!(consensus_state.hard_state().commit, 2);
        // Initially there are 0 collections
        assert_eq!(toc_arc.all_collections_sync().len(), 0);

        // When

        // New runtime is used as timers need to be enabled.
        handle
            .block_on(
                dispatcher.submit_collection_meta_op(
                    CollectionMetaOperations::CreateCollection(
                        CreateCollectionOperation::new(
                            "test".to_string(),
                            CreateCollection {
                                vectors: VectorParamsBuilder::new(10, Distance::Cosine)
                                    .build()
                                    .into(),
                                sparse_vectors: None,
                                hnsw_config: None,
                                wal_config: None,
                                optimizers_config: None,
                                shard_number: Some(2),
                                on_disk_payload: None,
                                replication_factor: None,
                                write_consistency_factor: None,
                                #[expect(deprecated)]
                                init_from: None,
                                quantization_config: None,
                                sharding_method: None,
                                strict_mode_config: None,
                                uuid: None,
                                metadata: None,
                            },
                        )
                        .unwrap(),
                    ),
                    Access::full("For test"),
                    None,
                ),
            )
            .unwrap();

        // Then
        assert_eq!(consensus_state.hard_state().commit, 5); // first peer self-election + add first peer + create collection + activate shard x2
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
