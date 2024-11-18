use std::sync::Arc;

use blob_store::fixtures::Payload;
use blob_store::Blob;
use bustle::Collection;
use parking_lot::RwLock;
use rocksdb::{DBRecoveryMode, LogLevel, Options, WriteOptions, DB};

use crate::fixture::{ArcStorage, SequentialCollectionHandle, StorageProxy};

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;
const DB_DELETE_OBSOLETE_FILES_PERIOD: u64 = 3 * 60 * 1_000_000; // 3 minutes in microseconds

pub const DB_PAYLOAD_CF: &str = "payload";

/// RocksDB options (both global and for column families)
pub fn db_options() -> Options {
    let mut options: Options = Options::default();
    options.set_write_buffer_size(DB_CACHE_SIZE); // write_buffer_size is enforced per column family.
    options.create_if_missing(true);
    options.set_log_level(LogLevel::Error);
    options.set_recycle_log_file_num(1);
    options.set_keep_log_file_num(1); // must be greater than zero
    options.set_max_log_file_size(DB_MAX_LOG_SIZE);
    options.set_delete_obsolete_files_period_micros(DB_DELETE_OBSOLETE_FILES_PERIOD);
    options.create_missing_column_families(true);
    options.set_max_open_files(DB_MAX_OPEN_FILES as i32);
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);

    // Qdrant relies on it's own WAL for durability
    options.set_wal_recovery_mode(DBRecoveryMode::TolerateCorruptedTailRecords);
    #[cfg(debug_assertions)]
    {
        options.set_paranoid_checks(true);
    }
    options
}

fn get_write_options() -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    // RocksDB WAL is required for durability even if data is flushed
    write_options.disable_wal(false);
    write_options
}

impl Collection for ArcStorage<rocksdb::DB> {
    type Handle = Self;

    fn with_capacity(_capacity: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocksdb");
        let column_families = vec![DB_PAYLOAD_CF];
        let options = db_options();
        // Make sure that all column families have the same options
        let column_with_options = column_families
            .into_iter()
            .map(|cf| (cf, options.clone()))
            .collect::<Vec<_>>();
        let db = DB::open_cf_with_opts(&options, path, column_with_options).unwrap();

        let proxy = StorageProxy::new(db);
        ArcStorage {
            proxy: Arc::new(RwLock::new(proxy)),
            dir: Arc::new(dir),
        }
    }

    fn pin(&self) -> Self::Handle {
        Self {
            proxy: self.proxy.clone(),
            dir: self.dir.clone(),
        }
    }
}

impl SequentialCollectionHandle for DB {
    fn get(&self, key: &u32) -> bool {
        let cf_handle = self.cf_handle(DB_PAYLOAD_CF).unwrap();
        self.get_cf(cf_handle, key.to_be_bytes()).unwrap().is_some()
    }

    fn insert(&mut self, key: u32, payload: &Payload) -> bool {
        let cf_handle = self.cf_handle(DB_PAYLOAD_CF).unwrap();
        let value = payload.to_bytes();
        self.put_cf_opt(cf_handle, key.to_be_bytes(), value, &get_write_options())
            .unwrap();
        true
    }

    fn remove(&mut self, key: &u32) -> bool {
        let cf_handle = self.cf_handle(DB_PAYLOAD_CF).unwrap();
        self.delete_cf_opt(cf_handle, key.to_be_bytes(), &get_write_options())
            .unwrap();
        true
    }

    fn update(&mut self, key: &u32, payload: &Payload) -> bool {
        let cf_handle = self.cf_handle(DB_PAYLOAD_CF).unwrap();
        let value = payload.to_bytes();
        self.put_cf_opt(cf_handle, key.to_be_bytes(), value, &get_write_options())
            .unwrap();
        true
    }

    fn flush(&self) -> bool {
        self.flush().is_ok()
    }
}
