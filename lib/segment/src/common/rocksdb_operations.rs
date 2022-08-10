use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::{Error, LogLevel, Options, WriteOptions, DB};

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb
const DB_MAX_OPEN_FILES: usize = 256;

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

pub fn open_db(path: &Path) -> Result<Arc<RwLock<DB>>, Error> {
    let db = DB::open_cf(
        &db_options(),
        path,
        &[DB_VECTOR_CF, DB_PAYLOAD_CF, DB_MAPPING_CF, DB_VERSIONS_CF],
    )?;
    Ok(Arc::new(RwLock::new(db)))
}

pub fn check_db_exists(path: &Path) -> bool {
    let db_file = path.join("CURRENT");
    db_file.exists()
}

pub fn open_db_with_existing_cf(path: &Path) -> Result<Arc<RwLock<DB>>, Error> {
    let existing_column_families = if check_db_exists(path) {
        DB::list_cf(&db_options(), path)?
    } else {
        vec![]
    };
    let db = DB::open_cf(&db_options(), path, &existing_column_families)?;
    Ok(Arc::new(RwLock::new(db)))
}

pub fn db_write_options() -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(true);
    write_options
}

pub fn create_db_cf_if_not_exists(db: Arc<RwLock<DB>>, store_cf_name: &str) -> Result<(), Error> {
    let mut db_mut = db.write();
    if db_mut.cf_handle(store_cf_name).is_none() {
        db_mut.create_cf(store_cf_name, &db_options())?;
    }
    Ok(())
}

pub fn recreate_cf(db: Arc<RwLock<DB>>, store_cf_name: &str) -> Result<(), Error> {
    let mut db_mut = db.write();

    if db_mut.cf_handle(store_cf_name).is_some() {
        db_mut.drop_cf(store_cf_name)?;
    }

    db_mut.create_cf(store_cf_name, &db_options())?;
    Ok(())
}
