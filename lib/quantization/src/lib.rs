pub mod encoded_storage;
pub mod encoded_vectors;
pub mod encoded_vectors_binary;
pub mod encoded_vectors_pq;
pub mod encoded_vectors_tq;
pub mod encoded_vectors_u8;
pub mod kmeans;
pub mod p_square;
pub mod quantile;
pub mod turboquant;
pub mod vector_stats;

#[cfg(test)]
mod conditional_variable_tests;

use std::fmt::Display;
use std::sync::{Arc, Condvar, Mutex};

pub use encoded_storage::{EncodedStorage, EncodedStorageBuilder, EncodedStorageWrite};
pub use encoded_vectors::{DistanceType, EncodedVectors, EncodedVectorsWrite, VectorParameters};
pub use encoded_vectors_pq::{EncodedQueryPQ, EncodedVectorsPQ};
pub use encoded_vectors_u8::{EncodedQueryU8, EncodedVectorsU8};

#[derive(Debug, PartialEq, Eq)]
pub enum EncodingError {
    IOError(String),
    EncodingError(String),
    ArgumentsError(String),
    Stopped,
}

impl Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::IOError(description) => write!(f, "IOError: {description}"),
            EncodingError::EncodingError(description) => {
                write!(f, "EncodingError: {description}")
            }
            EncodingError::ArgumentsError(description) => {
                write!(f, "ArgumentsError: {description}")
            }
            EncodingError::Stopped => write!(f, "Stopped"),
        }
    }
}

struct ConditionalVariableState {
    notified: bool,
    /// Live clones of this variable. Kept under the mutex (not read from `Arc::strong_count`), so
    /// a waiter can't observe a clone that is already being dropped as still alive: `Drop` runs
    /// before the clone's `Arc` is released, so a waiter woken by its notification would otherwise
    /// see the old count, go back to sleep, and never be woken again.
    handles: usize,
}

/// A wrapper around a mutex and a condvar.
///
/// `wait` returns when another clone calls `notify`, or when every other clone is dropped.
pub struct ConditionalVariable {
    shared: Arc<(Mutex<ConditionalVariableState>, Condvar)>,
}

impl Default for ConditionalVariable {
    fn default() -> Self {
        Self {
            shared: Arc::new((
                Mutex::new(ConditionalVariableState {
                    notified: false,
                    handles: 1,
                }),
                Condvar::new(),
            )),
        }
    }
}

impl Clone for ConditionalVariable {
    fn clone(&self) -> Self {
        self.shared.0.lock().unwrap().handles += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl ConditionalVariable {
    /// Wait until notified or until all other clones are dropped.
    ///
    /// Consumes the notification. Returns `true` if this is the last clone left.
    pub fn wait(&self) -> bool {
        let (mutex, condvar) = &*self.shared;
        let mut guard = mutex.lock().unwrap();
        while !guard.notified && guard.handles > 1 {
            guard = condvar.wait(guard).unwrap();
        }
        guard.notified = false;
        guard.handles == 1
    }

    pub fn notify(&self) {
        let (mutex, condvar) = &*self.shared;
        mutex.lock().unwrap().notified = true;
        condvar.notify_all();
    }
}

impl Drop for ConditionalVariable {
    fn drop(&mut self) {
        let (mutex, condvar) = &*self.shared;
        // Don't panic in drop if a panicking thread poisoned the mutex
        let mut guard = mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.handles -= 1;
        drop(guard);
        condvar.notify_all();
    }
}
