use segment::segment_constructor::open_segment_db;
use segment::segment_constructor::simple_segment_constructor::simple_segment_config;
use segment::types::Distance;
use tempfile::Builder;

use crate::fixtures::segment::{build_segment_1, empty_segment};

#[test]
fn test_create_empty_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let _segment = empty_segment(dir.path());
}

#[test]
fn test_create_rocksdb() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let config = simple_segment_config(4, Distance::Dot);
    let _database = open_segment_db(dir.path(), &config).unwrap();
}

#[test]
fn test_create_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let _segment = build_segment_1(dir.path());
}

#[test]
fn empty_test() {
    println!("Empty test");
}
