// Custom remote storage provider implementation for Qdrant
pub trait RemoteStorageProvider: Send + Sync {
    fn read_blob(&self, path: &str) -> Result<Vec<u8>, String>;
    fn write_blob(&self, path: &str, data: &[u8]) -> Result<(), String>;
}
