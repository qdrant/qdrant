use edge::flags::{FeatureFlags, feature_flags, init_feature_flags};
use edge::mmap::{MULTI_MMAP_IS_SUPPORTED, MULTI_MMAP_SUPPORT_CHECK_RESULT, check_mmap_functionality};

#[test]
fn test_reexported_flags_and_mmap() {
    // Verify feature_flags returns valid flags
    let flags = feature_flags();
    assert!(flags.is_default() || flags.incremental_hnsw_building);

    // Verify init_feature_flags can be called with custom or default FeatureFlags
    let mut custom_flags = FeatureFlags::default();
    custom_flags.write_segment_manifest = true;
    init_feature_flags(custom_flags);

    // Verify check_mmap_functionality can be executed on a directory
    let temp_dir = tempfile::tempdir().unwrap();
    let result = check_mmap_functionality(temp_dir.path()).expect("check_mmap_functionality works");
    assert!(result);

    // Verify MULTI_MMAP_SUPPORT_CHECK_RESULT and MULTI_MMAP_IS_SUPPORTED are accessible
    let _ = MULTI_MMAP_SUPPORT_CHECK_RESULT.set(result);
    assert!(*MULTI_MMAP_IS_SUPPORTED);
}
