use std::fs::File;

use tempfile::Builder;

#[test]
fn test_check_valid_collection_file() {
    let valid_collection_location = Builder::new().prefix("storage").tempdir().unwrap();
    File::create(
        valid_collection_location
            .path()
            .join(COLLECTION_CONFIG_FILE),
    )
    .unwrap();

    assert!(CollectionConfig::check_config_file_exists(
        valid_collection_location.path()
    ));

    let invalid_path = valid_collection_location.as_ref().join("invalid");
    File::create(invalid_path.clone()).unwrap();

    assert_eq!(
        CollectionConfig::check_config_file_exists(invalid_path.as_path()),
        false
    );
}
