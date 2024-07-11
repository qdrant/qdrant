use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rocksdb::{ColumnFamily, DB};

use super::operation_error::OperationError;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, LockedDatabaseColumnWrapper};
use crate::common::Flusher;

/// Wrapper around `DatabaseColumnWrapper` that ensures, that keys that were removed from the
/// database are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
#[derive(Debug)]
pub struct DatabaseColumnScheduledDeleteWrapper {
    db: DatabaseColumnWrapper,
    deleted_pending_persistence: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl Clone for DatabaseColumnScheduledDeleteWrapper {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            deleted_pending_persistence: self.pending_deletes(),
        }
    }
}

impl DatabaseColumnScheduledDeleteWrapper {
    pub fn new(db: DatabaseColumnWrapper) -> Self {
        Self {
            db,
            deleted_pending_persistence: Arc::new(Mutex::new(HashSet::new())),
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

    fn is_pending_removal<K>(&self, key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .lock()
            .contains(key.as_ref())
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

    pub fn pending_deletes(&self) -> Arc<Mutex<HashSet<Vec<u8>>>> {
        self.deleted_pending_persistence.clone()
    }

    pub fn get_pinned<T, F>(&self, key: &[u8], f: F) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        if self.is_pending_removal(key) {
            return Ok(None);
        }
        self.db.get_pinned(key, f)
    }

    pub fn recreate_column_family(&self) -> OperationResult<()> {
        self.db.recreate_column_family()
    }

    pub fn get_database(&self) -> Arc<RwLock<DB>> {
        self.db.get_database()
    }

    pub fn get_column_name(&self) -> &str {
        self.db.get_column_name()
    }

    pub fn has_column_family(&self) -> OperationResult<bool> {
        self.db.has_column_family()
    }

    pub fn remove_column_family(&self) -> OperationResult<()> {
        self.db.remove_column_family()
    }
}

/// RocksDB column iterator like `DatabaseColumnIterator`, but excluding pending deletes
pub struct DatabaseColumnScheduledDeleteWrapperIterator<'a> {
    pub handle: &'a ColumnFamily,
    pub iter: rocksdb::DBRawIterator<'a>,
    pub pending_deletes: Arc<Mutex<HashSet<Vec<u8>>>>,
}

impl<'a> DatabaseColumnScheduledDeleteWrapperIterator<'a> {
    pub fn new(
        db: &'a DB,
        column_name: &str,
        pending_deletes: Arc<Mutex<HashSet<Vec<u8>>>>,
    ) -> OperationResult<Self> {
        let handle = db.cf_handle(column_name).ok_or_else(|| {
            OperationError::service_error(format!(
                "RocksDB cf_handle error: Cannot find column family {column_name}"
            ))
        })?;
        let mut iter = db.raw_iterator_cf(&handle);
        iter.seek_to_first();
        Ok(Self {
            handle,
            iter,
            pending_deletes,
        })
    }
}

impl<'a> Iterator for DatabaseColumnScheduledDeleteWrapperIterator<'a> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut key;

        // Loop until we find a key that is not deleted
        loop {
            // Stop if iterator has ended or errored
            if !self.iter.valid() {
                return None;
            }

            key = self.iter.key().unwrap();
            if !self.pending_deletes.lock().contains(key) {
                break;
            }

            self.iter.next();
        }

        let item = (Box::from(key), Box::from(self.iter.value().unwrap()));

        // Search to next item for next iteration
        self.iter.next();

        Some(item)
    }
}
