use std::sync::atomic;

use super::TableOfContent;
use crate::content_manager::errors::StorageError;

pub const DEFAULT_WRITE_LOCK_ERROR_MESSAGE: &str = "Write operations are forbidden";

impl TableOfContent {
    pub fn is_write_locked(&self) -> bool {
        self.is_write_locked.load(atomic::Ordering::Relaxed)
    }

    pub fn get_lock_error_message(&self) -> Option<String> {
        self.lock_error_message.lock().clone()
    }

    /// Returns an error if the write lock is set
    pub fn check_write_lock(&self) -> Result<(), StorageError> {
        if self.is_write_locked.load(atomic::Ordering::Relaxed) {
            return Err(StorageError::Locked {
                description: self
                    .lock_error_message
                    .lock()
                    .clone()
                    .unwrap_or_else(|| DEFAULT_WRITE_LOCK_ERROR_MESSAGE.to_string()),
            });
        }
        Ok(())
    }

    pub fn set_locks(&self, is_write_locked: bool, error_message: Option<String>) {
        self.is_write_locked
            .store(is_write_locked, atomic::Ordering::Relaxed);
        *self.lock_error_message.lock() = error_message;
    }
}
