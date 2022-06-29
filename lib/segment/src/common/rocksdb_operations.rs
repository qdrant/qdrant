use crate::entry::entry_point::{OperationError, OperationResult};
use atomic_refcell::AtomicRefCell;
use rocksdb::{Error, LogLevel, Options, WriteOptions, DB};
use std::path::Path;
use std::sync::Arc;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;
const MAX_SST_FILES_PER_COLUMN_FAMILY: usize = 8;

pub const DB_VECTOR_CF: &str = "vector";
pub const DB_PAYLOAD_CF: &str = "payload";
pub const DB_MAPPING_CF: &str = "mapping";
pub const DB_VERSIONS_CF: &str = "version";

pub fn db_options() -> Options {
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

pub fn open_db(path: &Path) -> Result<Arc<AtomicRefCell<DB>>, Error> {
    let db = DB::open_cf(
        &db_options(),
        path,
        &[DB_VECTOR_CF, DB_PAYLOAD_CF, DB_MAPPING_CF, DB_VERSIONS_CF],
    )?;
    Ok(Arc::new(AtomicRefCell::new(db)))
}

pub fn check_db_exists(path: &Path) -> bool {
    let db_file = path.join("CURRENT");
    db_file.exists()
}

pub fn open_db_with_existing_cf(path: &Path) -> Result<Arc<AtomicRefCell<DB>>, Error> {
    let existing_column_families = if check_db_exists(path) {
        DB::list_cf(&db_options(), path)?
    } else {
        vec![]
    };
    let db = DB::open_cf(&db_options(), path, &existing_column_families)?;
    Ok(Arc::new(AtomicRefCell::new(db)))
}

pub fn db_write_options() -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(true);
    write_options
}

pub fn create_db_cf_if_not_exists(
    db: Arc<AtomicRefCell<DB>>,
    store_cf_name: &str,
) -> Result<(), Error> {
    if db.borrow().cf_handle(store_cf_name).is_none() {
        db.borrow_mut().create_cf(store_cf_name, &db_options())?;
    }
    Ok(())
}

pub fn recreate_cf(db: Arc<AtomicRefCell<DB>>, store_cf_name: &str) -> Result<(), Error> {
    let mut db_mut = db.borrow_mut();

    if db_mut.cf_handle(store_cf_name).is_some() {
        db_mut.drop_cf(store_cf_name)?;
    }

    db_mut.create_cf(store_cf_name, &db_options())?;
    Ok(())
}

pub fn flush_db(db: &Arc<AtomicRefCell<DB>>, cf_name: &str) -> OperationResult<()> {
    let db_ref = db.borrow();
    let cf = db_ref.cf_handle(cf_name).ok_or_else(|| {
        OperationError::service_error(&format!(
            "RocksDB flush error: column family {} not found",
            cf_name
        ))
    })?;

    db_ref.flush_cf(cf)?;

    let live_files = db_ref
        .live_files()?
        .iter()
        .filter(|live_file| live_file.column_family_name == cf_name)
        .count();

    if live_files > MAX_SST_FILES_PER_COLUMN_FAMILY {
        let mut compact_options = rocksdb::CompactOptions::default();
        compact_options.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
        // merge all SST files to one
        let start_key: Option<&[u8]> = None;
        let end_key: Option<&[u8]> = None;
        db_ref.compact_range_cf_opt(cf, start_key, end_key, &compact_options);
    }

    Ok(())
}
