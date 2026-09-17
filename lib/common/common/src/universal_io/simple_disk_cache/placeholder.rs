use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::Thread;

use parking_lot::Mutex;

use super::local_state::LocalState;
use crate::universal_io::{UioResult, UniversalIoError};

#[derive(Debug)]
enum PlaceholderState {
    Loading(Vec<Thread>),
    Completed,
    Abandoned,
}

#[derive(Debug)]
pub(super) struct Placeholder {
    pub(super) blocks_range: Range<u32>,
    pub(super) leader: Thread,
    state: Mutex<PlaceholderState>,
    completed: AtomicBool,
}

impl Placeholder {
    fn new(blocks_range: Range<u32>, leader: Thread) -> Self {
        Self {
            blocks_range,
            leader,
            state: Mutex::new(PlaceholderState::Loading(Vec::new())),
            completed: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(super) fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    pub(super) fn wait(&self) -> UioResult<()> {
        if self.is_completed() {
            return Ok(());
        }

        let current = std::thread::current();
        loop {
            {
                let mut state = self.state.lock();
                match &mut *state {
                    PlaceholderState::Completed => return Ok(()),
                    PlaceholderState::Abandoned => {
                        return Err(UniversalIoError::Io(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "remote fetch abandoned",
                        )));
                    }
                    PlaceholderState::Loading(waiters) => {
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
                PlaceholderState::Loading(waiters) => waiters,
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
            let waiters = {
                let mut state = self.placeholder.state.lock();
                match std::mem::replace(&mut *state, PlaceholderState::Abandoned) {
                    PlaceholderState::Loading(waiters) => waiters,
                    PlaceholderState::Completed | PlaceholderState::Abandoned => Vec::new(),
                }
            };
            for thread in waiters {
                thread.unpark();
            }
            self.registry.remove(&self.placeholder);
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
    ) -> PlaceholderResult {
        let mut list = self.placeholders.lock();

        for p in list.iter() {
            if p.blocks_range.start <= blocks_range.start && blocks_range.end <= p.blocks_range.end {
                // Different thread can safely piggyback; same thread avoids deadlock by fetching independently.
                if p.leader.id() != std::thread::current().id() {
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
            std::thread::current(),
        ));
        list.push(placeholder.clone());

        PlaceholderResult::Leader(PlaceholderGuard {
            placeholder,
            registry: self.clone(),
            completed: false,
        })
    }
}
