use tempfile::Builder;

use crate::fixtures::segment::build_segment_1;

#[test]
fn test_create_segment() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let _segment = build_segment_1(dir.path());
}

#[test]
fn empty_test() {
    println!("Empty test");
}
