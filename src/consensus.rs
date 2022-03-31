use std::{
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender},
    time::{Duration, Instant},
};

use storage::content_manager::toc::TableOfContentRef;

use raft::{eraftpb::Message as RaftMessage, prelude::*};

const TICK_PERIOD_MS: u64 = 100;

type Node = RawNode<TableOfContentRef>;

pub enum Message {
    FromClient(Vec<u8>),
    #[allow(dead_code)]
    FromPeer(Box<RaftMessage>),
}

pub struct Consensus {
    node: Node,
    receiver: Receiver<Message>,
}

impl Consensus {
    pub fn new(
        logger: &slog::Logger,
        toc_ref: TableOfContentRef,
    ) -> raft::Result<(Self, Sender<Message>)> {
        let config = Config {
            id: 1,
            ..Default::default()
        };
        config.validate()?;
        let node = Node::new(&config, toc_ref, logger)?;
        let (sender, receiver) = mpsc::channel();
        Ok((Self { node, receiver }, sender))
    }

    pub fn start(&mut self) -> raft::Result<()> {
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
            on_ready(&mut self.node);
        }
    }
}

fn on_ready(raft_group: &mut Node) {
    if !raft_group.has_ready() {
        return;
    }

    let store = raft_group.raft.raft_log.store.clone();
    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = raft_group.ready();
    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }
    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        log::info!("Applying snapshot");
        if let Err(err) = store.apply_snapshot(&ready.snapshot().clone()) {
            log::error!("Failed to apply snapshot: {err}")
        }
    }
    let mut _last_apply_index = 0;
    handle_committed_entries(ready.take_committed_entries(), &store);
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
        // Send out the persisted messages come from the node.
        handle_messages(ready.take_persisted_messages());
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
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(light_rd.take_committed_entries(), &store);
    // Advance the apply index.
    raft_group.advance_apply();
}

fn handle_committed_entries(entries: Vec<Entry>, toc: &TableOfContentRef) {
    for entry in entries {
        log::info!("Entry committed: {entry:?}");
        // Mostly, you need to save the last apply index to resume applying
        // TODO: handle it as part of persistent storage for raft state issue
        let _last_apply_index = entry.index;

        if entry.data.is_empty() {
            // Emtpy entry, when the peer becomes Leader it will send an empty entry.
            continue;
        }

        if entry.get_entry_type() == EntryType::EntryNormal {
            let operation_result = toc.apply_entry(&entry);
            match operation_result {
                Ok(result) => log::info!(
                    "Successfully applied collection meta operation entry. Index: {}. Result: {result}",
                    entry.index
                ),
                Err(err) => {
                    log::error!("Failed to apply collection meta operation entry with error: {err}")
                }
            }
        }

        // TODO: handle EntryConfChange
    }
}

fn handle_messages(messages: Vec<RaftMessage>) {
    for _message in messages {
        // TODO: send to other peers
    }
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
        let handle = runtime.handle().clone();
        let mut toc = TableOfContent::new(&settings.storage, runtime);
        let (propose_sender, propose_receiver) = std::sync::mpsc::channel();
        toc.with_propose_sender(propose_sender);
        let toc_arc = Arc::new(toc);
        let slog_logger = slog::Logger::root(slog_stdlog::StdLog.fuse(), slog::o!());
        let (mut consensus, message_sender) =
            Consensus::new(&slog_logger, toc_arc.clone().into()).unwrap();
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
        handle
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
            ))
            .unwrap();
        thread::sleep(Duration::from_secs(5));

        // Then
        assert_eq!(toc_arc.hard_state().unwrap().commit, 2);
        assert_eq!(toc_arc.all_collections_sync(), vec!["test"]);
    }
}
