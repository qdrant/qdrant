pub mod condition_checker;
#[cfg(feature = "testing")]
pub mod in_memory_payload_storage;
#[cfg(feature = "testing")]
pub mod in_memory_payload_storage_impl;
mod memory_reporter;
mod payload_storage_base;
pub mod payload_storage_enum;
pub mod payload_storage_impl;
pub mod query_checker;
pub mod read_only;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, Ordering};

pub use payload_storage_base::*;

static ASYNC_PAYLOAD_STORAGE: AtomicBool = AtomicBool::new(false);

pub fn set_async_payload_storage(async_payload_storage: bool) {
    ASYNC_PAYLOAD_STORAGE.store(async_payload_storage, Ordering::Relaxed);
}

pub fn get_async_payload_storage() -> bool {
    ASYNC_PAYLOAD_STORAGE.load(Ordering::Relaxed)
}
