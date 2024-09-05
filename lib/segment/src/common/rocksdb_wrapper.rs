use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::statistics::StatsLevel;
//use atomic_refcell::{AtomicRef, AtomicRefCell};
use rocksdb::{ColumnFamily, DBRecoveryMode, LogLevel, Options, WriteOptions, DB};

//use crate::common::arc_rwlock_iterator::ArcRwLockIterator;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;
const DB_DELETE_OBSOLETE_FILES_PERIOD: u64 = 3 * 60 * 1_000_000; // 3 minutes in microseconds

pub const DB_VECTOR_CF: &str = "vector";
pub const DB_PAYLOAD_CF: &str = "payload";
pub const DB_MAPPING_CF: &str = "mapping";
pub const DB_VERSIONS_CF: &str = "version";
/// If there is no Column Family specified, key-value pair is associated with Column Family "default".
pub const DB_DEFAULT_CF: &str = "default";

#[derive(Clone, Debug)]
pub struct DatabaseColumnWrapper {
    database: Arc<RwLock<DB>>,
    column_name: String,
}

pub struct DatabaseColumnIterator<'a> {
    pub handle: &'a ColumnFamily,
    pub iter: rocksdb::DBRawIterator<'a>,
}

pub struct LockedDatabaseColumnWrapper<'a> {
    guard: parking_lot::RwLockReadGuard<'a, DB>,
    column_name: &'a str,
}

/// RocksDB options (both global and for column families)
pub fn db_options() -> Options {
    let mut options: Options = Options::default();
    options.set_write_buffer_size(DB_CACHE_SIZE); // write_buffer_size is enforced per column family.
    options.create_if_missing(true);
    options.set_log_level(LogLevel::Info);
    options.set_recycle_log_file_num(1);
    options.set_keep_log_file_num(1); // must be greater than zero
    options.set_max_log_file_size(DB_MAX_LOG_SIZE);
    options.set_delete_obsolete_files_period_micros(DB_DELETE_OBSOLETE_FILES_PERIOD);
    options.create_missing_column_families(true);
    options.set_max_open_files(DB_MAX_OPEN_FILES as i32);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_dump_malloc_stats(true);
    options.enable_statistics();
    options.set_statistics_level(StatsLevel::All);

    // Qdrant relies on it's own WAL for durability
    options.set_wal_recovery_mode(DBRecoveryMode::TolerateCorruptedTailRecords);
    #[cfg(debug_assertions)]
    {
        options.set_paranoid_checks(true);
    }
    options
}

pub fn open_db<T: AsRef<str>>(
    path: &Path,
    vector_paths: &[T],
) -> Result<Arc<RwLock<DB>>, rocksdb::Error> {
    let mut column_families = vec![DB_PAYLOAD_CF, DB_MAPPING_CF, DB_VERSIONS_CF, DB_DEFAULT_CF];
    for vector_path in vector_paths {
        column_families.push(vector_path.as_ref());
    }
    let options = db_options();
    // Make sure that all column families have the same options
    let column_with_options = column_families
        .into_iter()
        .map(|cf| (cf, options.clone()))
        .collect::<Vec<_>>();
    let db = DB::open_cf_with_opts(&options, path, column_with_options)?;
    Ok(Arc::new(RwLock::new(db)))
}

pub fn check_db_exists(path: &Path) -> bool {
    let db_file = path.join("CURRENT");
    db_file.exists()
}

pub fn open_db_with_existing_cf(path: &Path) -> Result<Arc<RwLock<DB>>, rocksdb::Error> {
    let existing_column_families = if check_db_exists(path) {
        DB::list_cf(&db_options(), path)?
    } else {
        vec![]
    };
    let db = DB::open_cf(&db_options(), path, existing_column_families)?;
    Ok(Arc::new(RwLock::new(db)))
}

impl DatabaseColumnWrapper {
    pub fn new(database: Arc<RwLock<DB>>, column_name: &str) -> Self {
        Self {
            database,
            column_name: column_name.to_string(),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let db = self.database.read();
        let cf_handle = self.get_column_family(&db)?;
        db.put_cf_opt(cf_handle, key, value, &Self::get_write_options())
            .map_err(|err| OperationError::service_error(format!("RocksDB put_cf error: {err}")))?;
        Ok(())
    }

    pub fn get<K>(&self, key: K) -> OperationResult<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        let db = self.database.read();
        let cf_handle = self.get_column_family(&db)?;
        db.get_cf(cf_handle, key)
            .map_err(|err| OperationError::service_error(format!("RocksDB get_cf error: {err}")))?
            .ok_or_else(|| OperationError::service_error("RocksDB get_cf error: key not found"))
    }

    pub fn get_pinned<T, F>(&self, key: &[u8], f: F) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let db = self.database.read();
        let cf_handle = self.get_column_family(&db)?;
        let result = db
            .get_pinned_cf(cf_handle, key)
            .map_err(|err| {
                OperationError::service_error(format!("RocksDB get_pinned_cf error: {err}"))
            })?
            .map(|value| f(&value));
        Ok(result)
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let db = self.database.read();
        let cf_handle = self.get_column_family(&db)?;
        db.delete_cf_opt(cf_handle, key, &Self::get_write_options())
            .map_err(|err| {
                OperationError::service_error(format!("RocksDB delete_cf error: {err}"))
            })?;
        Ok(())
    }

    pub fn lock_db(&self) -> LockedDatabaseColumnWrapper {
        LockedDatabaseColumnWrapper {
            guard: self.database.read(),
            column_name: &self.column_name,
        }
    }

    pub fn flusher(&self) -> Flusher {
        let database = self.database.clone();
        let column_name = self.column_name.clone();
        Box::new(move || {
            let db = database.read();
            let Some(column_family) = db.cf_handle(&column_name) else {
                // It is possible, that the index was removed during the flush by user or another thread.
                // In this case, non-existing column family is not an error, but an expected behavior.

                // Still we want to log this event, for potential debugging.
                log::warn!(
                    "Flush: RocksDB cf_handle error: Cannot find column family {}. Ignoring",
                    &column_name
                );
                return Ok(()); // ignore error
            };

            db.flush_cf(column_family).map_err(|err| {
                OperationError::service_error(format!("RocksDB flush_cf error: {err}"))
            })?;
            Ok(())
        })
    }

    pub fn create_column_family_if_not_exists(&self) -> OperationResult<()> {
        let mut db = self.database.write();
        if db.cf_handle(&self.column_name).is_none() {
            db.create_cf(&self.column_name, &db_options())
                .map_err(|err| {
                    OperationError::service_error(format!("RocksDB create_cf error: {err}"))
                })?;
        }
        Ok(())
    }

    pub fn recreate_column_family(&self) -> OperationResult<()> {
        self.remove_column_family()?;
        self.create_column_family_if_not_exists()
    }

    pub fn remove_column_family(&self) -> OperationResult<()> {
        let mut db = self.database.write();
        if db.cf_handle(&self.column_name).is_some() {
            db.drop_cf(&self.column_name).map_err(|err| {
                OperationError::service_error(format!("RocksDB drop_cf error: {err}"))
            })?;
        }
        Ok(())
    }

    pub fn has_column_family(&self) -> OperationResult<bool> {
        let db = self.database.read();
        Ok(db.cf_handle(&self.column_name).is_some())
    }

    fn get_write_options() -> WriteOptions {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        // RocksDB WAL is required for durability even if data is flushed
        write_options.disable_wal(false);
        write_options
    }

    fn get_column_family<'a>(
        &self,
        db: &'a parking_lot::RwLockReadGuard<'_, DB>,
    ) -> OperationResult<&'a ColumnFamily> {
        db.cf_handle(&self.column_name).ok_or_else(|| {
            OperationError::service_error(format!(
                "RocksDB cf_handle error: Cannot find column family {}",
                &self.column_name
            ))
        })
    }

    pub fn get_database(&self) -> Arc<RwLock<DB>> {
        self.database.clone()
    }

    pub fn get_column_name(&self) -> &str {
        &self.column_name
    }
}

impl<'a> LockedDatabaseColumnWrapper<'a> {
    pub fn iter(&self) -> OperationResult<DatabaseColumnIterator> {
        DatabaseColumnIterator::new(&self.guard, self.column_name)
    }
}

impl<'a> DatabaseColumnIterator<'a> {
    pub fn new(db: &'a DB, column_name: &str) -> OperationResult<DatabaseColumnIterator<'a>> {
        let handle = db.cf_handle(column_name).ok_or_else(|| {
            OperationError::service_error(format!(
                "RocksDB cf_handle error: Cannot find column family {column_name}"
            ))
        })?;
        let mut iter = db.raw_iterator_cf(&handle);
        iter.seek_to_first();
        Ok(DatabaseColumnIterator { handle, iter })
    }
}

impl<'a> Iterator for DatabaseColumnIterator<'a> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        // Stop if iterator has ended or errored
        if !self.iter.valid() {
            return None;
        }

        let item = (
            Box::from(self.iter.key().unwrap()),
            Box::from(self.iter.value().unwrap()),
        );

        // Search to next item for next iteration
        self.iter.next();

        Some(item)
    }
}
