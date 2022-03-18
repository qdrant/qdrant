use std::{
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender},
    time::{Duration, Instant},
};

use raft::{prelude::*, storage::MemStorage};

const TICK_PERIOD_MS: u64 = 100;

pub struct Consensus {
    node: RawNode<MemStorage>,
    receiver: Receiver<Message>,
}

impl Consensus {
    pub fn new(logger: &slog::Logger) -> (Self, Sender<Message>) {
        let config = Config {
            id: 1,
            ..Default::default()
        };
        config.validate().unwrap();
        let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
        let node = RawNode::new(&config, storage, logger).unwrap();
        let (sender, receiver) = mpsc::channel();
        (Self { node, receiver }, sender)
    }

    pub fn start(&mut self) {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(TICK_PERIOD_MS);

        loop {
            match self.receiver.recv_timeout(timeout) {
                Ok(message) => self.node.step(message).unwrap(),
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => {
                    log::warn!("Stopping Raft as message sender was dropped");
                    return;
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
            Consensus::on_ready(&mut self.node);
        }
    }

    fn on_ready(raft_group: &mut RawNode<MemStorage>) {
        if !raft_group.has_ready() {
            return;
        }
        let store = raft_group.raft.raft_log.store.clone();

        // Get the `Ready` with `RawNode::ready` interface.
        let mut ready = raft_group.ready();

        let handle_messages = |msgs: Vec<Message>| {
            for _msg in msgs {
                // Send messages to other peers.
            }
        };

        if !ready.messages().is_empty() {
            // Send out the messages come from the node.
            handle_messages(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            // This is a snapshot, we need to apply the snapshot at first.
            store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
        }

        let mut _last_apply_index = 0;
        let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
            for entry in committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                _last_apply_index = entry.index;

                if entry.data.is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                if entry.get_entry_type() == EntryType::EntryNormal {
                    // TODO: Manipulate data
                    // entry.data.get(0).unwrap());
                }

                // TODO: handle EntryConfChange
            }
        };
        handle_committed_entries(ready.take_committed_entries());

        if !ready.entries().is_empty() {
            // Append entries to the Raft log.
            store.wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            store.wl().set_hardstate(hs.clone());
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            handle_messages(ready.take_persisted_messages());
        }

        // Advance the Raft.
        let mut light_rd = raft_group.advance(ready);
        // Update commit index.
        if let Some(commit) = light_rd.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        // Send out the messages.
        handle_messages(light_rd.take_messages());
        // Apply all committed entries.
        handle_committed_entries(light_rd.take_committed_entries());
        // Advance the apply index.
        raft_group.advance_apply();
    }
}
