use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{Thread, ThreadId};

use parking_lot::Mutex;

use super::local_state::LocalState;
use crate::universal_io::{UioResult, UniversalIoError};

#[derive(Debug)]
enum PlaceholderState {
    Loading {
        leader: ThreadId,
        waiters: Vec<Thread>,
    },
    Completed,
    Abandoned,
}

#[derive(Debug)]
pub(super) struct Placeholder {
    pub(super) blocks_range: Range<u32>,
    pub(super) blocks_byte_range: Range<u64>,
    registry: Arc<PlaceholderRegistry>,
    state: Mutex<PlaceholderState>,
    completed: AtomicBool,
}

pub(super) enum WaitResult {
    Completed,
    Promoted(PlaceholderGuard),
}

impl Placeholder {
    fn new(
        blocks_range: Range<u32>,
        blocks_byte_range: Range<u64>,
        leader: ThreadId,
        registry: Arc<PlaceholderRegistry>,
    ) -> Self {
        Self {
            blocks_range,
            blocks_byte_range,
            registry,
            state: Mutex::new(PlaceholderState::Loading {
                leader,
                waiters: Vec::new(),
            }),
            completed: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(super) fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    #[inline]
    pub(super) fn covers(&self, range: &Range<u32>) -> bool {
        self.blocks_range.start <= range.start && range.end <= self.blocks_range.end
    }

    pub(super) fn leader_id(&self) -> ThreadId {
        let state = self.state.lock();
        match &*state {
            PlaceholderState::Loading { leader, .. } => *leader,
            PlaceholderState::Completed | PlaceholderState::Abandoned => std::thread::current().id(),
        }
    }

    pub(super) fn wait(self: &Arc<Self>) -> UioResult<WaitResult> {
        if self.is_completed() {
            return Ok(WaitResult::Completed);
        }

        let current = std::thread::current();
        loop {
            {
                let mut state = self.state.lock();
                match &mut *state {
                    PlaceholderState::Completed => return Ok(WaitResult::Completed),
                    PlaceholderState::Abandoned => {
                        return Err(UniversalIoError::Io(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "remote fetch abandoned",
                        )));
                    }
                    PlaceholderState::Loading { leader, waiters } => {
                        if *leader == current.id() {
                            // This thread has been promoted to be the new leader.
                            let guard = PlaceholderGuard {
                                placeholder: self.clone(),
                                registry: self.registry.clone(),
                                completed: false,
                            };
                            return Ok(WaitResult::Promoted(guard));
                        }
                        if !waiters.iter().any(|w| w.id() == current.id()) {
                            waiters.push(current.clone());
                        }
                    }
                }
            }
            std::thread::park();
        }
    }
}

#[derive(Debug)]
pub(super) struct PlaceholderGuard {
    placeholder: Arc<Placeholder>,
    registry: Arc<PlaceholderRegistry>,
    completed: bool,
}

impl PlaceholderGuard {
    pub(super) fn placeholder(&self) -> &Arc<Placeholder> {
        &self.placeholder
    }

    pub(super) fn complete(mut self) {
        self.completed = true;
        let waiters = {
            let mut state = self.placeholder.state.lock();
            self.placeholder.completed.store(true, Ordering::Release);
            match std::mem::replace(&mut *state, PlaceholderState::Completed) {
                PlaceholderState::Loading { waiters, .. } => waiters,
                PlaceholderState::Completed | PlaceholderState::Abandoned => Vec::new(),
            }
        };
        for thread in waiters {
            thread.unpark();
        }
        self.registry.remove(&self.placeholder);
    }
}

impl Drop for PlaceholderGuard {
    fn drop(&mut self) {
        if !self.completed {
            let next_to_unpark = {
                let mut state = self.placeholder.state.lock();
                match &mut *state {
                    PlaceholderState::Loading { leader, waiters } => {
                        if let Some(next_leader) = waiters.pop() {
                            // Promote the next waiting follower to be the leader
                            *leader = next_leader.id();
                            Some(next_leader)
                        } else {
                            *state = PlaceholderState::Abandoned;
                            None
                        }
                    }
                    PlaceholderState::Completed | PlaceholderState::Abandoned => None,
                }
            };

            if let Some(next_leader) = next_to_unpark {
                next_leader.unpark();
            } else {
                self.registry.remove(&self.placeholder);
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct PlaceholderRegistry {
    placeholders: Mutex<Vec<Arc<Placeholder>>>,
}

pub(super) enum PlaceholderResult {
    AlreadyLocal,
    Leader(PlaceholderGuard),
    Piggyback(Arc<Placeholder>),
}

impl PlaceholderRegistry {
    pub(super) fn new() -> Self {
        Self {
            placeholders: Mutex::new(Vec::new()),
        }
    }

    fn remove(&self, placeholder: &Arc<Placeholder>) {
        let mut list = self.placeholders.lock();
        list.retain(|p| !Arc::ptr_eq(p, placeholder));
    }

    pub(super) fn get_or_register(
        self: &Arc<Self>,
        local: &LocalState,
        blocks_range: Range<u32>,
        blocks_byte_range: Range<u64>,
    ) -> PlaceholderResult {
        let mut list = self.placeholders.lock();

        for p in list.iter() {
            if p.covers(&blocks_range) {
                // Different thread can safely piggyback; same thread avoids deadlock by fetching independently.
                if p.leader_id() != std::thread::current().id() {
                    return PlaceholderResult::Piggyback(p.clone());
                }
            }
        }

        // Avoid race where fetch committed and placeholder was removed right before we locked
        if local.contains(blocks_range.clone()) {
            return PlaceholderResult::AlreadyLocal;
        }

        let placeholder = Arc::new(Placeholder::new(
            blocks_range,
            blocks_byte_range,
            std::thread::current().id(),
            self.clone(),
        ));
        list.push(placeholder.clone());

        PlaceholderResult::Leader(PlaceholderGuard {
            placeholder,
            registry: self.clone(),
            completed: false,
        })
    }
}
