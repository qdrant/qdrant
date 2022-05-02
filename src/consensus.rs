use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use api::grpc::{
    qdrant::{raft_client::RaftClient, PeerId, RaftMessage as GrpcRaftMessage},
    timeout_channel,
};
use storage::content_manager::{errors::StorageError, toc::TableOfContentRef};

use raft::{eraftpb::Message as RaftMessage, prelude::*};
use tokio::runtime::Runtime;
use tonic::transport::Uri;

const CHANNEL_CAPACITY: usize = 100;
const TICK_PERIOD_MS: u64 = 100;
const MESSAGE_TIMEOUT: Duration = Duration::from_millis(1000);
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_millis(5000);

type Node = RawNode<TableOfContentRef>;

pub enum Message {
    FromClient(Vec<u8>),
    FromPeer(Box<RaftMessage>),
}

pub struct Consensus {
    node: Node,
    receiver: Receiver<Message>,
    runtime: Runtime,
    bootstrap_uri: Option<Uri>,
}

impl Consensus {
    pub fn new(
        logger: &slog::Logger,
        toc_ref: TableOfContentRef,
        bootstrap_peer: Option<Uri>,
        this_peer_uri: Uri,
    ) -> anyhow::Result<(Self, SyncSender<Message>)> {
        let config = Config {
            id: toc_ref.this_peer_id(),
            ..Default::default()
        };
        config.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("consensus-tokio-rt-{}", id)
            })
            .enable_all()
            .build()?;
        if let Some(bootstrap_peer) = bootstrap_peer.clone() {
            runtime.block_on(Self::bootstrap(&toc_ref, bootstrap_peer, this_peer_uri))?;
        }
        // Before consensus has started apply any unapplied committed entries
        // They might have not been applied due to unplanned Qdrant shutdown
        toc_ref.apply_entries()?;
        let node = Node::new(&config, toc_ref, logger)?;
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        Ok((
            Self {
                node,
                receiver,
                runtime,
                bootstrap_uri: bootstrap_peer,
            },
            sender,
        ))
    }

    pub async fn bootstrap(
        toc_ref: &TableOfContentRef,
        bootstrap_peer: Uri,
        this_peer_uri: Uri,
    ) -> anyhow::Result<()> {
        let channel = timeout_channel(BOOTSTRAP_TIMEOUT, bootstrap_peer)
            .await
            .context("Failed to create timeout channel")?;
        let mut client = RaftClient::new(channel);
        let all_peers = client
            .add_peer_to_known(tonic::Request::new(api::grpc::qdrant::Peer {
                uri: this_peer_uri.to_string(),
                id: toc_ref.this_peer_id(),
            }))
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
        Ok(())
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(TICK_PERIOD_MS);

        loop {
            match self.receiver.recv_timeout(timeout) {
                Ok(Message::FromPeer(message)) => self.node.step(*message)?,
                Ok(Message::FromClient(message)) => {
                    log::debug!("Proposing entry from client with length: {}", message.len());
                    self.node.propose(vec![], message)?
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
                timeout = Duration::from_millis(TICK_PERIOD_MS);
                // We drive Raft every 100ms.
                self.node.tick();
            } else {
                timeout -= d;
            }
            on_ready(&mut self.node, &self.runtime, &self.bootstrap_uri);
        }
    }
}

fn on_ready(raft_group: &mut Node, runtime: &Runtime, bootstrap_uri: &Option<Uri>) {
    if !raft_group.has_ready() {
        return;
    }

    let store = raft_group.raft.raft_log.store.clone();
    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();
    if !ready.messages().is_empty() {
        if let Err(err) = handle_messages(ready.take_messages(), &store, runtime, bootstrap_uri) {
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
    if let Err(err) = handle_committed_entries(ready.take_committed_entries(), &store) {
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
    if let Err(err) = handle_messages(light_rd.take_messages(), &store, runtime, bootstrap_uri) {
        log::error!("Failed to send messages: {err}")
    }
    // Apply all committed entries.
    if let Err(err) = handle_committed_entries(light_rd.take_committed_entries(), &store) {
        log::error!("Failed to apply committed entries: {err}")
    }
    // Advance the apply index.
    raft_group.advance_apply();
}

fn handle_committed_entries(entries: Vec<Entry>, toc: &TableOfContentRef) -> raft::Result<()> {
    if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
        toc.set_unapplied_entries(first.index, last.index)?;
        toc.apply_entries()?;
    }
    Ok(())
}

fn handle_messages(
    messages: Vec<RaftMessage>,
    toc: &TableOfContentRef,
    runtime: &Runtime,
    bootstrap_uri: &Option<Uri>,
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
    let future = async move {
        let mut send_futures = Vec::new();
        for (message, address) in messages_with_address {
            let address = match address {
                Some(address) => address,
                None => match who_is(message.to, bootstrap_uri.clone()).await {
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
            send_futures.push(send_message(address, message));
        }
        for result in futures::future::join_all(send_futures).await {
            if let Err(err) = result {
                log::warn!("Failed to send message: {err}")
            }
        }
    };
    // Raft does not need the responses and should not wait for timeouts
    // so sending messages in parallel should be ok
    runtime.spawn(future);
    Ok(())
}

async fn who_is(peer_id: collection::PeerId, bootstrap_uri: Option<Uri>) -> anyhow::Result<Uri> {
    let bootstrap_uri =
        bootstrap_uri.ok_or_else(|| anyhow::anyhow!("No bootstrap uri supplied"))?;
    let channel = timeout_channel(BOOTSTRAP_TIMEOUT, bootstrap_uri)
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

async fn send_message(address: Uri, message: RaftMessage) -> anyhow::Result<()> {
    let channel = timeout_channel(MESSAGE_TIMEOUT, address)
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

    use segment::types::Distance;
    use slog::Drain;
    use storage::content_manager::{
        collection_meta_ops::{
            CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
        },
        toc::TableOfContent,
    };
    use tempdir::TempDir;
    use tonic::transport::Uri;

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
        let mut toc = TableOfContent::new(&settings.storage, runtime);
        let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
        toc.with_propose_sender(propose_sender);
        let toc_arc = Arc::new(toc);
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());
        let (mut consensus, message_sender) = Consensus::new(
            &slog_logger,
            toc_arc.clone().into(),
            None,
            Uri::from_static("127.0.0.1:8080"),
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
        // Wait for Raft to establish the leader
        thread::sleep(Duration::from_secs(5));
        // Leader election produces a raft log entry
        assert_eq!(toc_arc.hard_state().unwrap().commit, 1);
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
                        shard_number: 1,
                    },
                }),
                None,
            ))
            .unwrap();

        // Then
        assert_eq!(toc_arc.hard_state().unwrap().commit, 2);
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
