use std::collections::HashSet;
use std::mem;

use parking_lot::Mutex;

use super::rocksdb_wrapper::DatabaseColumn;
use crate::common::rocksdb_wrapper::LockedDatabaseColumnWrapper;
use crate::common::Flusher;
use crate::entry::entry_point::OperationResult;

/// Wrapper around `DatabaseColumnWrapper` that ensures, that keys that were removed from the
/// database are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
pub struct ScheduledDeleteDecorator<T: DatabaseColumn> {
    db: T,
    deleted_pending_persistence: Mutex<HashSet<Vec<u8>>>,
}

impl<T: DatabaseColumn> ScheduledDeleteDecorator<T> {
    pub fn new(db: T) -> Self {
        Self {
            db,
            deleted_pending_persistence: Mutex::new(HashSet::new()),
        }
    }
}

impl<W: DatabaseColumn + Clone + Send + 'static> DatabaseColumn for ScheduledDeleteDecorator<W> {
    fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.deleted_pending_persistence.lock().remove(key.as_ref());
        self.db.put(key, value)
    }

    fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .lock()
            .insert(key.as_ref().to_vec());
        Ok(())
    }

    fn flusher(&self) -> Flusher {
        let ids_to_delete = mem::take(&mut *self.deleted_pending_persistence.lock());
        let wrapper = self.db.clone();
        Box::new(move || {
            for id in ids_to_delete {
                wrapper.remove(id)?;
            }
            wrapper.flusher()()
        })
    }

    fn lock_db(&self) -> LockedDatabaseColumnWrapper {
        self.db.lock_db()
    }

    fn get_pinned<T, F>(&self, key: &[u8], f: F) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        self.db.get_pinned(key, f)
    }

    fn create_column_family_if_not_exists(&self) -> OperationResult<()> {
        self.db.create_column_family_if_not_exists()
    }

    fn recreate_column_family(&self) -> OperationResult<()> {
        self.db.recreate_column_family()
    }

    fn remove_column_family(&self) -> OperationResult<()> {
        self.db.remove_column_family()
    }

    fn has_column_family(&self) -> OperationResult<bool> {
        self.db.has_column_family()
    }
}
