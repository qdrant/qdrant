use std::collections::{HashMap, HashSet};
use std::mem;

use parking_lot::Mutex;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, LockedDatabaseColumnWrapper};

/// Wrapper around `DatabaseColumnWrapper` that ensures,
///     that all changes are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
#[derive(Debug)]
pub struct DatabaseColumnScheduledUpdateWrapper {
    db: DatabaseColumnWrapper,
    pending_operations: Mutex<PendingOperations>, // in-flight operations persisted on flush
}

#[derive(Debug, Default)]
struct PendingOperations {
    deleted: HashSet<Vec<u8>>,
    inserted: HashMap<Vec<u8>, Vec<u8>>,
}

impl DatabaseColumnScheduledUpdateWrapper {
    pub fn new(db: DatabaseColumnWrapper) -> Self {
        Self {
            db,
            pending_operations: Mutex::new(PendingOperations::default()),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut pending_guard = self.pending_operations.lock();
        pending_guard
            .inserted
            .insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        pending_guard.deleted.remove(key.as_ref());
        Ok(())
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let mut pending_guard = self.pending_operations.lock();
        pending_guard.inserted.remove(key);
        pending_guard.deleted.insert(key.to_vec());
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let PendingOperations { deleted, inserted } =
            mem::take(&mut *self.pending_operations.lock());
        debug_assert!(
            inserted.keys().all(|key| !deleted.contains(key)),
            "Key to marked for insertion is also marked for deletion!"
        );
        let wrapper = self.db.clone();
        Box::new(move || {
            for id in deleted {
                wrapper.remove(id)?;
            }
            for (id, value) in inserted {
                wrapper.put(id, value)?;
            }
            wrapper.flusher()()
        })
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnWrapper {
        self.db.lock_db()
    }

    pub fn get<K>(&self, key: K) -> OperationResult<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let pending_guard = self.pending_operations.lock();
        if let Some(value) = pending_guard.inserted.get(key.as_ref()) {
            return Ok(value.clone());
        }
        if pending_guard.deleted.contains(key.as_ref()) {
            return Err(OperationError::service_error(
                "RocksDB get_cf error: key not found",
            ));
        }
        self.db.get(key)
    }

    pub fn get_opt<K>(&self, key: K) -> OperationResult<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        let pending_guard = self.pending_operations.lock();
        if let Some(value) = pending_guard.inserted.get(key.as_ref()) {
            return Ok(Some(value.clone()));
        }
        if pending_guard.deleted.contains(key.as_ref()) {
            return Ok(None);
        }
        self.db.get_opt(key)
    }
}
