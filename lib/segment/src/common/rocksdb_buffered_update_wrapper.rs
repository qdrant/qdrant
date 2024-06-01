use std::collections::{HashMap, HashSet};
use std::mem;

use parking_lot::Mutex;

use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, LockedDatabaseColumnWrapper};
use crate::common::Flusher;

/// Wrapper around `DatabaseColumnWrapper` that ensures,
///     that all changes are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
pub struct DatabaseColumnScheduledUpdateWrapper {
    db: DatabaseColumnWrapper,
    deleted_pending_persistence: Mutex<HashSet<Vec<u8>>>,
    insert_pending_persistence: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DatabaseColumnScheduledUpdateWrapper {
    pub fn new(db: DatabaseColumnWrapper) -> Self {
        Self {
            db,
            deleted_pending_persistence: Mutex::new(HashSet::new()),
            insert_pending_persistence: Mutex::new(HashMap::new()),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        // keep `insert_pending_persistence` lock for atomicity
        let mut insert_guard = self.insert_pending_persistence.lock();
        insert_guard.insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        self.deleted_pending_persistence.lock().remove(key.as_ref());
        drop(insert_guard);
        Ok(())
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        // keep `insert_pending_persistence` lock for atomicity
        let mut insert_guard = self.insert_pending_persistence.lock();
        insert_guard.remove(key);
        self.deleted_pending_persistence.lock().insert(key.to_vec());
        drop(insert_guard);
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let ids_to_insert = mem::take(&mut *self.insert_pending_persistence.lock());
        let ids_to_delete = mem::take(&mut *self.deleted_pending_persistence.lock());
        debug_assert!(
            ids_to_insert.keys().all(|key| !ids_to_delete.contains(key)),
            "Key to marked for insertion is also marked for deletion!"
        );
        let wrapper = self.db.clone();
        Box::new(move || {
            for id in ids_to_delete {
                wrapper.remove(id)?;
            }
            for (id, value) in ids_to_insert {
                wrapper.put(id, value)?;
            }
            wrapper.flusher()()
        })
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnWrapper {
        self.db.lock_db()
    }
}
