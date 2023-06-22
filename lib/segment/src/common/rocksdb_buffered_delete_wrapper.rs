use std::collections::HashSet;
use std::mem;

use parking_lot::Mutex;

use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, LockedDatabaseColumnWrapper};
use crate::common::Flusher;
use crate::entry::entry_point::OperationResult;

/// Wrapper around `DatabaseColumnWrapper` that ensures, that keys that were removed from the
/// database are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
pub struct DatabaseColumnScheduledDeleteWrapper {
    db: DatabaseColumnWrapper,
    deleted_pending_persistence: Mutex<HashSet<Vec<u8>>>,
}

impl DatabaseColumnScheduledDeleteWrapper {
    pub fn new(db: DatabaseColumnWrapper) -> Self {
        Self {
            db,
            deleted_pending_persistence: Mutex::new(HashSet::new()),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.deleted_pending_persistence.lock().remove(key.as_ref());
        self.db.put(key, value)
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .lock()
            .insert(key.as_ref().to_vec());
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let ids_to_delete = mem::take(&mut *self.deleted_pending_persistence.lock());
        let wrapper = self.db.clone();
        Box::new(move || {
            for id in ids_to_delete {
                wrapper.remove(id)?;
            }
            wrapper.flusher()()
        })
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnWrapper {
        self.db.lock_db()
    }
}
