use crate::common::arc_atomic_ref_cell_iterator::ArcAtomicRefCellIterator;
use crate::entry::entry_point::{OperationError, OperationResult};
use atomic_refcell::{AtomicRef, AtomicRefCell};
use rocksdb::{ColumnFamily, LogLevel, Options, WriteOptions, DB};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;

pub const DB_VECTOR_CF: &str = "vector";
pub const DB_PAYLOAD_CF: &str = "payload";
pub const DB_MAPPING_CF: &str = "mapping";
pub const DB_VERSIONS_CF: &str = "version";

/// Each rocksdb column contains this key with constant value
/// This is needed to avoid trivial move compaction:
/// https://github.com/facebook/rocksdb/wiki/Compaction-Trivial-Move
/// Trivial move compaction makes a bug when user writes data slowly.
/// When user makes 1 write between flush calls, trivial move compaction makes sst file for each user write
/// and it appears a ton of sst files.
/// Manual compaction works slowly that's why we use this fixed key-value pair to avoid using trivial move compaction
pub const FIXED_KEY: &[u8] = &[0xff, 0x07, 0xaf, 0x90, 0x7b, 0x07, 0x55, 0xd0];
pub const FIXED_VALUE: &[u8] = &[0];

pub struct Database {
    db: DB,
    allow_add_new_keys: bool,
}

pub struct DatabaseColumnWrapper {
    pub database: Arc<AtomicRefCell<Database>>,
    pub column_name: String,
    pub put_fixed_key: AtomicBool,
}

pub struct DatabaseColumnIterator<'a: 'b, 'b> {
    pub handle: &'a ColumnFamily,
    pub iter: rocksdb::DBRawIterator<'b>,
    pub just_seeked: bool,
}

impl Database {
    pub fn new(
        path: &Path,
        default_columns: bool,
        allow_add_new_keys: bool,
    ) -> OperationResult<Arc<AtomicRefCell<Self>>> {
        let column_families: Vec<String> = if default_columns {
            vec![
                DB_VECTOR_CF.to_string(),
                DB_PAYLOAD_CF.to_string(),
                DB_MAPPING_CF.to_string(),
                DB_VERSIONS_CF.to_string(),
            ]
        } else {
            let db_file = path.join("CURRENT");
            if db_file.exists() {
                DB::list_cf(&Self::get_options(), path).map_err(|err| {
                    OperationError::service_error(&format!("RocksDB list_cf error: {}", err))
                })?
            } else {
                vec![]
            }
        };
        let db = DB::open_cf(&Self::get_options(), path, &column_families).map_err(|err| {
            OperationError::service_error(&format!("RocksDB open_cf error: {}", err))
        })?;
        Ok(Arc::new(AtomicRefCell::new(Self {
            db,
            allow_add_new_keys,
        })))
    }

    fn get_options() -> Options {
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        options.set_log_level(LogLevel::Error);
        options.set_recycle_log_file_num(2);
        options.set_max_log_file_size(DB_MAX_LOG_SIZE);
        options.create_missing_column_families(true);
        options.set_max_open_files(DB_MAX_OPEN_FILES as i32);
        #[cfg(debug_assertions)]
        {
            options.set_paranoid_checks(true);
        }
        options
    }
}

impl DatabaseColumnWrapper {
    pub fn new(database: Arc<AtomicRefCell<Database>>, column_name: &str) -> Self {
        Self {
            database,
            column_name: column_name.to_string(),
            put_fixed_key: AtomicBool::new(false),
        }
    }

    pub fn put<K, V>(&self, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        if key.as_ref() == FIXED_KEY {
            return Err(OperationError::service_error(
                "Rocksdb: cannot use FIXED_KEY value",
            ));
        }
        let db = self.database.borrow();
        let cf_handle = self.get_column_family(&db)?;
        db.db
            .put_cf_opt(cf_handle, key, value, &Self::get_write_options())
            .map_err(|err| {
                OperationError::service_error(&format!("RocksDB put_cf error: {}", err))
            })?;
        if self.put_fixed_key.load(Ordering::Relaxed) {
            db.db
                .put_cf_opt(
                    cf_handle,
                    FIXED_KEY,
                    FIXED_VALUE,
                    &Self::get_write_options(),
                )
                .map_err(|err| {
                    OperationError::service_error(&format!(
                        "RocksDB put_cf (fixed key) error: {}",
                        err
                    ))
                })?;
            self.put_fixed_key.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn get_pinned<T, F>(&self, key: &[u8], f: F) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let db = self.database.borrow();
        let cf_handle = self.get_column_family(&db)?;
        let result = db
            .db
            .get_pinned_cf(cf_handle, key)
            .map_err(|err| {
                OperationError::service_error(&format!("RocksDB get_pinned_cf error: {}", err))
            })?
            .map(|value| f(&value));
        Ok(result)
    }

    pub fn remove<K>(&self, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let db = self.database.borrow();
        let cf_handle = self.get_column_family(&db)?;
        db.db.delete_cf(cf_handle, key).map_err(|err| {
            OperationError::service_error(&format!("RocksDB delete_cf error: {}", err))
        })?;
        Ok(())
    }

    pub fn iter<'a: 'b, 'b>(
        &self,
    ) -> OperationResult<ArcAtomicRefCellIterator<Database, DatabaseColumnIterator<'a, 'b>>> {
        let db = self.database.borrow();
        // check column existing
        self.get_column_family(&db)?;
        let column_name = self.column_name.clone();
        Ok(ArcAtomicRefCellIterator::new(
            self.database.clone(),
            move |db| {
                // column existsing is already checked
                let handle = db.db.cf_handle(&column_name).unwrap();
                let mut iter = db.db.raw_iterator_cf(&handle);
                iter.seek_to_first();
                DatabaseColumnIterator {
                    handle,
                    iter,
                    just_seeked: true,
                }
            },
        ))
    }

    pub fn flush(&self) -> OperationResult<()> {
        let db = self.database.borrow();
        let column_family = self.get_column_family(&db)?;

        db.db.flush_cf(column_family).map_err(|err| {
            OperationError::service_error(&format!("RocksDB flush_cf error: {}", err))
        })?;
        if db.allow_add_new_keys {
            self.put_fixed_key.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn create_column_family_if_not_exists(&self) -> OperationResult<()> {
        let mut db = self.database.borrow_mut();
        if db.db.cf_handle(&self.column_name).is_none() {
            db.db
                .create_cf(&self.column_name, &Database::get_options())
                .map_err(|err| {
                    OperationError::service_error(&format!("RocksDB create_cf error: {}", err))
                })?;
        }
        Ok(())
    }

    pub fn recreate_column_family(&self) -> OperationResult<()> {
        self.remove_column_family()?;
        self.create_column_family_if_not_exists()
    }

    pub fn remove_column_family(&self) -> OperationResult<()> {
        let mut db = self.database.borrow_mut();
        if db.db.cf_handle(&self.column_name).is_some() {
            db.db.drop_cf(&self.column_name).map_err(|err| {
                OperationError::service_error(&format!("RocksDB drop_cf error: {}", err))
            })?;
        }
        Ok(())
    }

    pub fn has_column_family(&self) -> OperationResult<bool> {
        let db = self.database.borrow();
        Ok(db.db.cf_handle(&self.column_name).is_some())
    }

    fn get_write_options() -> WriteOptions {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);
        write_options
    }

    fn get_column_family<'a, 'b>(
        &self,
        db: &'a AtomicRef<'b, Database>,
    ) -> OperationResult<&'a ColumnFamily> {
        db.db.cf_handle(&self.column_name).ok_or_else(|| {
            OperationError::service_error(&format!(
                "RocksDB cf_handle error: Cannot find column family {}",
                &self.column_name
            ))
        })
    }
}

impl<'a: 'b, 'b> Iterator for DatabaseColumnIterator<'a, 'b> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.iter.valid() {
            return None;
        }

        // Initial call to next() after seeking should not move the iterator
        // or the first item will not be returned
        if self.just_seeked {
            self.just_seeked = false;
        } else {
            self.iter.next();
        }

        if self.iter.valid() {
            // .key() only ever return None if valid == false, which we've just checked
            if self.iter.key().unwrap() == FIXED_KEY {
                self.iter.next();
            }
        }

        if self.iter.valid() {
            // .key() and .value() only ever return None if valid == false, which we've just checked
            Some((
                Box::from(self.iter.key().unwrap()),
                Box::from(self.iter.value().unwrap()),
            ))
        } else {
            None
        }
    }
}
