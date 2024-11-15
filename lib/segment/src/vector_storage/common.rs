use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

static ASYNC_SCORER: AtomicBool = AtomicBool::new(false);

pub fn set_async_scorer(async_scorer: bool) {
    ASYNC_SCORER.store(async_scorer, Ordering::Relaxed);
}

pub fn get_async_scorer() -> bool {
    ASYNC_SCORER.load(Ordering::Relaxed)
}

/// Storage type for RocksDB based storage
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StoredRecord<T> {
    pub deleted: bool,
    pub vector: T,
}

/// Minimal number of bytes we read from disk in one go
/// WARN: this might be system dependent, so we assume 4Kb, which might be wrong
/// ToDo: read this from system
pub const PAGE_SIZE_BYTES: usize = 4096;

/// Number of vectors we read from storage in one batch
/// in case we need to score an iterator of vector ids
pub const VECTOR_READ_BATCH_SIZE: usize = 64;

#[cfg(debug_assertions)]
pub const CHUNK_SIZE: usize = 512 * 1024;

/// Vector storage chunk size in bytes
#[cfg(not(debug_assertions))]
pub const CHUNK_SIZE: usize = 32 * 1024 * 1024;
