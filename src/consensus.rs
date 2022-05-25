use std::sync::Arc;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::Context;
use api::grpc::qdrant::{raft_client::RaftClient, PeerId, RaftMessage as GrpcRaftMessage};
use storage::content_manager::{
    consensus_ops::ConsensusOperations, errors::StorageError, toc::TableOfContentRef,
};

use crate::settings::ConsensusConfig;
use api::grpc::transport_channel_pool::TransportChannelPool;
use raft::{eraftpb::Message as RaftMessage, prelude::*};
use tokio::runtime::Runtime;
use tonic::transport::Uri;

type Node = RawNode<TableOfContentRef>;

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
}

impl Consensus {
    /// If `bootstrap_peer` peer is supplied, then either `uri` or `p2p_port` should be also supplied
    pub fn new(
        logger: &slog::Logger,
        toc_ref: TableOfContentRef,
        bootstrap_peer: Option<Uri>,
        uri: Option<String>,
        p2p_port: Option<u32>,
        config: ConsensusConfig,
    ) -> anyhow::Result<(Self, SyncSender<Message>)> {
        let raft_config = Config {
            id: toc_ref.this_peer_id(),
            ..Default::default()
        };
        raft_config.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("consensus-tokio-rt-{}", id)
            })
            .enable_all()
            .build()?;
        let (sender, receiver) = mpsc::sync_channel(config.max_message_queue_size);
        if let Some(bootstrap_peer) = bootstrap_peer.clone() {
            log::info!("Bootstrapping from peer with address: {bootstrap_peer}");
            if uri.is_none() && p2p_port.is_none() {
                return Err(anyhow::anyhow!("Failed to bootstrap peer as neither `internal rpc port` was configured nor `this peer uri` was supplied"));
            }
            runtime.block_on(Self::bootstrap(
                &toc_ref,
                bootstrap_peer,
                uri,
                p2p_port,
                &config,
            ))?;
        } else {
            log::info!("Bootstrapping is disabled. Assuming this peer is the first in the network");
            // First peer needs to add its own address
            let message = Message::FromClient(serde_cbor::to_vec(&ConsensusOperations::AddPeer(
                toc_ref.this_peer_id(),
                uri.ok_or_else(|| anyhow::anyhow!("First peer should specify its uri."))?,
            ))?);
            let sender_clone = sender.clone();
            thread::spawn(move || {
                // Wait for the leader to be established
                // TODO: Is it possible to do it deterministically?
                // TODO: Don't send message if this is a restart
                thread::sleep(Duration::from_secs(5));
                if let Err(err) = sender_clone.send(message) {
                    log::error!("Failed to send message to add this peer to known peers: {err}")
                }
            });
        }
        let mut node = Node::new(&raft_config, toc_ref.clone(), logger)?;
        // Before consensus has started apply any unapplied committed entries
        // They might have not been applied due to unplanned Qdrant shutdown
        toc_ref.apply_entries(&mut node)?;
        Ok((
            Self {
                node,
                receiver,
                runtime,
                bootstrap_uri: bootstrap_peer,
                config,
            },
            sender,
        ))
    }

    pub async fn bootstrap(
        toc_ref: &TableOfContentRef,
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
        let id = toc_ref.this_peer_id();
        let all_peers = client
            .add_peer_to_known(tonic::Request::new(
                api::grpc::qdrant::AddPeerToKnownMessage {
                    uri,
                    port: p2p_port,
                    id,
                },
            ))
            .await?
            .into_inner();
        // Although peer addresses are synchronized with consensus, addresses need to be pre-fetched in the case of a new peer
        // or it will not know how to answer the Raft leader
        for peer in all_peers.all_peers {
            toc_ref
                .add_peer(
                    peer.id,
                    peer.uri
                        .parse()
                        .context(format!("Failed to parse peer URI: {}", peer.uri))?,
                )
                .context("Failed to add peer")?;
        }
        client
            .add_peer_as_participant(tonic::Request::new(api::grpc::qdrant::PeerId { id }))
            .await?;
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(self.config.tick_period_ms);

        loop {
            match self.receiver.recv_timeout(timeout) {
                Ok(Message::FromPeer(message)) => self.node.step(*message)?,
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

            let d = t.elapsed();
            t = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(self.config.tick_period_ms);
                // We drive Raft every `tick_period_ms`.
                self.node.tick();
            } else {
                timeout -= d;
            }
            on_ready(
                &mut self.node,
                &self.runtime,
                &self.bootstrap_uri,
                &self.config,
            );
        }
    }
}

fn on_ready(
    raft_group: &mut Node,
    runtime: &Runtime,
    bootstrap_uri: &Option<Uri>,
    config: &ConsensusConfig,
) {
    if !raft_group.has_ready() {
        return;
    }

    let store = raft_group.raft.raft_log.store.clone();
    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();
    if !ready.messages().is_empty() {
        if let Err(err) = handle_messages(
            ready.take_messages(),
            &store,
            runtime,
            bootstrap_uri,
            config,
        ) {
            log::error!("Failed to send messages: {err}")
        }
    }
    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        log::info!("Applying snapshot");
        if let Err(err) = store.apply_snapshot(&ready.snapshot().clone()) {
            log::error!("Failed to apply snapshot: {err}")
        }
    }
    if let Err(err) = handle_committed_entries(ready.take_committed_entries(), &store, raft_group) {
        log::error!("Failed to apply committed entries: {err}")
    }
    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        log::info!("Appending {} entries to raft log", ready.entries().len());
        if let Err(err) = store.append_entries(ready.take_entries()) {
            log::error!("Failed to append entries: {err}")
        }
    }
    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        log::info!("Changing hard state. New hard state: {hs:?}");
        if let Err(err) = store.set_hard_state(hs.clone()) {
            log::error!("Failed to set hard state: {err}")
        }
    }
    if !ready.persisted_messages().is_empty() {
        if let Err(err) = handle_messages(
            ready.take_persisted_messages(),
            &store,
            runtime,
            bootstrap_uri,
            config,
        ) {
            log::error!("Failed to send messages: {err}")
        }
    }

    // Advance the Raft.
    let mut light_rd = raft_group.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        log::info!("Updating commit index to {commit}");
        if let Err(err) = store.set_commit_index(commit) {
            log::error!("Failed to set commit index: {err}")
        }
    }
    if let Err(err) = handle_messages(
        light_rd.take_messages(),
        &store,
        runtime,
        bootstrap_uri,
        config,
    ) {
        log::error!("Failed to send messages: {err}")
    }
    // Apply all committed entries.
    if let Err(err) =
        handle_committed_entries(light_rd.take_committed_entries(), &store, raft_group)
    {
        log::error!("Failed to apply committed entries: {err}")
    }
    // Advance the apply index.
    raft_group.advance_apply();
}

fn handle_committed_entries(
    entries: Vec<Entry>,
    toc: &TableOfContentRef,
    raw_node: &mut RawNode<TableOfContentRef>,
) -> raft::Result<()> {
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        toc.set_unapplied_entries(first.index, last.index)?;
        toc.apply_entries(raw_node)?;
    }
    Ok(())
}

fn handle_messages(
    messages: Vec<RaftMessage>,
    toc: &TableOfContentRef,
    runtime: &Runtime,
    bootstrap_uri: &Option<Uri>,
    config: &ConsensusConfig,
) -> Result<(), StorageError> {
    let peer_address_by_id = toc.peer_address_by_id()?;
    let messages_with_address: Vec<_> = messages
        .into_iter()
        .map(|message| {
            let address = peer_address_by_id.get(&message.to).cloned();
            (message, address)
        })
        .collect();
    let bootstrap_uri = bootstrap_uri.clone();
    let consensus_config_arc = Arc::new(config.clone());
    // TableOfContentRef wraps a Arc<TableOfContent>
    let toc_clone = toc.clone();
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
            send_futures.push(send_message(toc_clone.clone(), address, message));
        }
        for result in futures::future::join_all(send_futures).await {
            if let Err(err) = result {
                log::warn!("Failed to send message: {err:#}")
            }
        }
    };
    // Raft does not need the responses and should not wait for timeouts
    // so sending messages in parallel should be ok
    runtime.spawn(future);
    Ok(())
}

async fn who_is(
    peer_id: collection::PeerId,
    bootstrap_uri: Option<Uri>,
    config: Arc<ConsensusConfig>,
) -> anyhow::Result<Uri> {
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
    toc: TableOfContentRef,
    address: Uri,
    message: RaftMessage,
) -> anyhow::Result<()> {
    let channel = toc
        .get_or_create_pooled_channel(&address)
        .await
        .context("Failed to create timeout channel")?;
    let mut client = RaftClient::new(channel);
    let mut bytes = Vec::new();
    <RaftMessage as prost::Message>::encode(&message, &mut bytes)
        .context("Failed to serialize Raft message")?;
    let message = GrpcRaftMessage { message: bytes };
    client
        .send(tonic::Request::new(message))
        .await
        .context("gRPC call failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use crate::settings::ConsensusConfig;
    use api::grpc::transport_channel_pool::TransportChannelPool;
    use segment::types::Distance;
    use slog::Drain;
    use storage::content_manager::toc::ConsensusEnabled;
    use storage::content_manager::{
        collection_meta_ops::{
            CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
        },
        toc::TableOfContent,
    };
    use tempdir::TempDir;

    use super::Consensus;

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
        let consensus_enabled = ConsensusEnabled {
            propose_sender,
            first_peer: true,
            transport_channel_pool: TransportChannelPool::default(),
        };
        let toc = TableOfContent::new(&settings.storage, runtime, Some(consensus_enabled));
        let toc_arc = Arc::new(toc);
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());
        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            toc_arc.clone().into(),
            None,
            Some("http://127.0.0.1:6335".parse().unwrap()),
            None,
            ConsensusConfig::default(),
        )
        .unwrap();
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
        // Wait for Raft to establish the leader and commit its own address
        thread::sleep(Duration::from_secs(10));
        // Leader election produces a raft log entry
        // Address commit also produces a raft log entry
        assert_eq!(toc_arc.hard_state().unwrap().commit, 2);
        // Initially there are 0 collections
        assert_eq!(toc_arc.all_collections_sync().len(), 0);

        // When

        // New runtime is used as timers need to be enabled.
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(toc_arc.submit_collection_operation(
                CollectionMetaOperations::CreateCollection(CreateCollectionOperation {
                    collection_name: "test".to_string(),
                    create_collection: CreateCollection {
                        vector_size: 10,
                        distance: Distance::Cosine,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: 2,
                    },
                }),
                None,
            ))
            .unwrap();

        // Then
        assert_eq!(toc_arc.hard_state().unwrap().commit, 3);
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
