use tempfile::Builder;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::operations::snapshot_ops::calculate_checksum;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_calculate_checksum() {
    init_logger();
    let test_file_path = Builder::new().prefix("test_checksum").tempdir().unwrap();
    let test_file_path = test_file_path.path().join("test_file.txt");
    let mut test_file = File::create(&test_file_path).await.expect("Failed to create test file");
    test_file.write_all(b"Hello, world!").await.expect("Failed to write to test file");

    // Calculate checksum
    let checksum = calculate_checksum(&test_file_path).await.expect("Failed to calculate checksum");

    let expected_checksum = "315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3";

    assert_eq!(checksum, expected_checksum);

    // Cleanup: Remove test file
    tokio::fs::remove_file(test_file_path).await.expect("Failed to remove test file");
}
