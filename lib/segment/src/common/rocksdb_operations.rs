use rocksdb::{Error, LogLevel, Options, WriteOptions, DB};
use std::path::Path;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_MAX_LOG_SIZE: usize = 1024 * 1024; // 1 mb

fn db_options() -> Options {
    let mut options: Options = Options::default();
    options.set_write_buffer_size(DB_CACHE_SIZE);
    options.create_if_missing(true);
    options.set_log_level(LogLevel::Error);
    options.set_recycle_log_file_num(2);
    options.set_max_log_file_size(DB_MAX_LOG_SIZE);
    #[cfg(debug_assertions)]
    {
        options.set_paranoid_checks(true);
    }
    options
}

pub fn open_db(path: &Path) -> Result<DB, Error> {
    let options = db_options();
    DB::open(&options, path)
}

pub fn open_db_with_cf(path: &Path, cf: &[&str]) -> Result<DB, Error> {
    let mut options = db_options();
    options.create_missing_column_families(true);
    DB::open_cf(&options, path, cf)
}

pub fn db_write_options() -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);
    write_options.disable_wal(true);
    write_options
}
