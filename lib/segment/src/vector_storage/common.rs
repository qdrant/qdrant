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
