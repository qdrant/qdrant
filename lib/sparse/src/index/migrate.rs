use io::storage_version::StorageVersion;

pub struct SparseVectorIndexVersion;

impl StorageVersion for SparseVectorIndexVersion {
    fn current_raw() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
