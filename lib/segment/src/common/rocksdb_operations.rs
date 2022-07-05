use crate::entry::entry_point::{OperationError, OperationResult};
use rocksdb::{LogLevel, Options, WriteOptions, DB};
use std::path::Path;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;

pub const DB_VECTOR_CF: &str = "vector";
pub const DB_PAYLOAD_CF: &str = "payload";
pub const DB_MAPPING_CF: &str = "mapping";
pub const DB_VERSIONS_CF: &str = "version";

pub struct Database {
    db: DB,
}

pub enum DatabaseIterationResult<T> {
    Break(OperationResult<T>),
    Continue,
}

impl Database {
    pub fn new_with_default_column_families(path: &Path, is_appendable: bool) -> OperationResult<Self> {
        Self::new_with_column_families(
            path,
            &[DB_VECTOR_CF, DB_PAYLOAD_CF, DB_MAPPING_CF, DB_VERSIONS_CF],
            is_appendable,
        )
    }

    pub fn new_with_existing_column_families(path: &Path, is_appendable: bool) -> OperationResult<Self> {
        let db_file = path.join("CURRENT");
        let existing_column_families = if db_file.exists() {
            DB::list_cf(&Self::get_options(), path)?
        } else {
            vec![]
        };
        Self::new_with_column_families(path, &existing_column_families, is_appendable)
    }

    pub fn new_with_column_families<I, N>(path: &Path, column_families: I, is_appendable: bool) -> OperationResult<Self>
    where
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let db = DB::open_cf(&Self::get_options(), path, column_families)?;
        Ok(Self { db })
    }

    pub fn put<K, V>(&self, column_family: &str, key: K, value: V) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf_handle = self
            .db
            .cf_handle(column_family)
            .ok_or_else(|| OperationError::service_error(""))?;
        self.db
            .put_cf_opt(cf_handle, key, value, &Self::get_write_options())
            .map_err(|_| OperationError::service_error(""))?;
        Ok(())
    }

    pub fn get_pinned<T, F>(
        &self,
        column_family: &str,
        key: &[u8],
        f: F,
    ) -> OperationResult<Option<T>>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let cf_handle = self
            .db
            .cf_handle(column_family)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;
        Ok(self
            .db
            .get_pinned_cf(cf_handle, key)?
            .map(|value| f(&value)))
    }

    pub fn remove<K>(&self, column_family: &str, key: K) -> OperationResult<()>
    where
        K: AsRef<[u8]>,
    {
        let cf_handle = self
            .db
            .cf_handle(column_family)
            .ok_or_else(|| OperationError::service_error(""))?;
        self.db
            .delete_cf(cf_handle, key)
            .map_err(|_| OperationError::service_error(""))?;
        Ok(())
    }

    pub fn iterate_over_column_family<T, F>(
        &self,
        column_family: &str,
        mut f: F,
    ) -> OperationResult<T>
    where
        F: FnMut((&[u8], &[u8])) -> DatabaseIterationResult<T>,
        T: Default,
    {
        let cf_handle = self
            .db
            .cf_handle(column_family)
            .ok_or_else(|| OperationError::service_error("Payload storage column not found"))?;

        let mut iter = self.db.raw_iterator_cf(&cf_handle);
        iter.seek_to_first();

        while iter.valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            match f((key, value)) {
                DatabaseIterationResult::Break(result) => return result,
                DatabaseIterationResult::Continue => {}
            }
            iter.next();
        }
        Ok(T::default())
    }

    pub fn flush(&self, column_family: &str) -> OperationResult<()> {
        let column_family = self.db.cf_handle(column_family).ok_or_else(|| {
            OperationError::service_error(&format!(
                "RocksDB flush error: column family {} not found",
                column_family
            ))
        })?;
        self.db.flush_cf(column_family)?;
        Ok(())
    }

    pub fn create_column_family_if_not_exists(
        &mut self,
        column_family: &str,
    ) -> OperationResult<()> {
        if self.db.cf_handle(column_family).is_none() {
            self.db.create_cf(column_family, &Self::get_options())?;
        }
        Ok(())
    }

    pub fn recreate_column_family(&mut self, column_family: &str) -> OperationResult<()> {
        self.remove_column_family(column_family)?;
        self.create_column_family_if_not_exists(column_family)
    }

    pub fn remove_column_family(&mut self, column_family: &str) -> OperationResult<()> {
        if self.db.cf_handle(column_family).is_some() {
            self.db.drop_cf(column_family)?;
        }
        Ok(())
    }

    pub fn has_column_family(&self, column_family: &str) -> OperationResult<bool> {
        Ok(self.db.cf_handle(column_family).is_some())
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

    fn get_write_options() -> WriteOptions {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);
        write_options
    }
}
