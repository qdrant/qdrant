use std::sync::Arc;

use ahash::AHashSet;
use parking_lot::RwLock;
use rocksdb::DB;

use super::rocksdb_wrapper::DatabaseColumnIterator;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_wrapper::{DatabaseColumnWrapper, LockedDatabaseColumnWrapper};

/// Wrapper around `DatabaseColumnWrapper` that ensures, that keys that were removed from the
/// database are only persisted on flush explicitly.
///
/// This might be required to guarantee consistency of the database component.
/// E.g. copy-on-write implementation should guarantee that data in the `write` component is
/// persisted before it is removed from the `copy` component.
///
/// WARN: this structure is expected to be write-only.
#[derive(Debug, Clone)]
pub struct DatabaseColumnScheduledDeleteWrapper {
    db: DatabaseColumnWrapper,
    deleted_pending_persistence: Arc<RwLock<AHashSet<Vec<u8>>>>,
}

impl DatabaseColumnScheduledDeleteWrapper {
    pub fn new(db: DatabaseColumnWrapper) -> Self {
        Self {
            db,
            deleted_pending_persistence: Arc::new(RwLock::new(AHashSet::new())),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .write()
            .remove(key.as_ref());
        self.db.put(key, value)
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .write()
            .insert(key.as_ref().to_vec());
        Ok(())
    }

    fn is_pending_removal<K>(&self, key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        self.deleted_pending_persistence
            .read()
            .contains(key.as_ref())
    }

    pub fn flusher(&self) -> Flusher {
        let ids_to_delete = self.deleted_pending_persistence.read().clone();
        let wrapper = self.db.clone();

        let deleted_pending_persistence = Arc::downgrade(&self.deleted_pending_persistence);

        Box::new(move || {
            let Some(deleted_pending_persistence_arc) = deleted_pending_persistence.upgrade()
            else {
                return Ok(());
            };

            for id in &ids_to_delete {
                wrapper.remove(id)?;
            }
            wrapper.flusher()()?;

            Self::reconcile_persisted_deletes(ids_to_delete, &deleted_pending_persistence_arc);

            Ok(())
        })
    }

    /// Removes from `deleted_pending_persistence` all results that are flushed.
    fn reconcile_persisted_deletes(
        persisted: AHashSet<Vec<u8>>,
        pending_operations: &RwLock<AHashSet<Vec<u8>>>,
    ) {
        pending_operations
            .write()
            .retain(|pending| !persisted.contains(pending));
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnScheduledDeleteWrapper<'_> {
        LockedDatabaseColumnScheduledDeleteWrapper {
            base: self.db.lock_db(),
            deleted_pending_persistence: &self.deleted_pending_persistence,
        }
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

    pub fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        self.db.get_storage_size_bytes()
    }
}

pub struct LockedDatabaseColumnScheduledDeleteWrapper<'a> {
    base: LockedDatabaseColumnWrapper<'a>,
    deleted_pending_persistence: &'a RwLock<AHashSet<Vec<u8>>>,
}

impl LockedDatabaseColumnScheduledDeleteWrapper<'_> {
    pub fn iter(&self) -> OperationResult<DatabaseColumnScheduledDeleteIterator<'_>> {
        Ok(DatabaseColumnScheduledDeleteIterator {
            base: self.base.iter()?,
            deleted_pending_persistence: self.deleted_pending_persistence,
        })
    }
}

pub struct DatabaseColumnScheduledDeleteIterator<'a> {
    base: DatabaseColumnIterator<'a>,
    deleted_pending_persistence: &'a RwLock<AHashSet<Vec<u8>>>,
}

impl Iterator for DatabaseColumnScheduledDeleteIterator<'_> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (key, value) = self.base.next()?;
            if !self
                .deleted_pending_persistence
                .read()
                .contains(key.as_ref())
            {
                return Some((key, value));
            }
        }
    }
}
