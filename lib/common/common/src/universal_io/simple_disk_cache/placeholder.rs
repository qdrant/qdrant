use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{Thread, ThreadId};

use parking_lot::Mutex;

use super::local_state::LocalState;

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
    registry: Weak<PlaceholderRegistry>,
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
        registry: Weak<PlaceholderRegistry>,
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
    fn new_guard(self: &Arc<Self>) -> PlaceholderGuard {
        PlaceholderGuard {
            placeholder: self.clone(),
            completed: false,
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

    pub(super) fn wait(self: &Arc<Self>) -> WaitResult {
        if self.is_completed() {
            return WaitResult::Completed;
        }

        let current = std::thread::current();
        loop {
            {
                let mut state = self.state.lock();
                match &mut *state {
                    PlaceholderState::Completed => return WaitResult::Completed,
                    PlaceholderState::Abandoned => {
                        *state = PlaceholderState::Loading {
                            leader: current.id(),
                            waiters: Vec::new(),
                        };
                        return WaitResult::Promoted(self.new_guard());
                    }
                    PlaceholderState::Loading { leader, waiters } => {
                        if *leader == current.id() {
                            return WaitResult::Promoted(self.new_guard());
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
        if let Some(registry) = self.placeholder.registry.upgrade() {
            registry.remove(&self.placeholder);
        }
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
            } else if Arc::strong_count(&self.placeholder) <= 2
                && let Some(registry) = self.placeholder.registry.upgrade()
            {
                registry.remove(&self.placeholder);
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

        list.retain(|p| !p.is_completed() && Arc::strong_count(p) > 1);

        let current_thread_id = std::thread::current().id();
        for p in list.iter() {
            if p.covers(&blocks_range) {
                let mut state = p.state.lock();
                match &mut *state {
                    PlaceholderState::Loading { leader, .. } => {
                        if *leader != current_thread_id {
                            return PlaceholderResult::Piggyback(p.clone());
                        }
                    }
                    PlaceholderState::Abandoned => {
                        *state = PlaceholderState::Loading {
                            leader: current_thread_id,
                            waiters: Vec::new(),
                        };
                        return PlaceholderResult::Leader(p.new_guard());
                    }
                    PlaceholderState::Completed => {}
                }
            }
        }

        if local.contains(blocks_range.clone()) {
            return PlaceholderResult::AlreadyLocal;
        }

        let placeholder = Arc::new(Placeholder::new(
            blocks_range,
            blocks_byte_range,
            current_thread_id,
            Arc::downgrade(self),
        ));
        list.push(placeholder.clone());

        PlaceholderResult::Leader(placeholder.new_guard())
    }
}
