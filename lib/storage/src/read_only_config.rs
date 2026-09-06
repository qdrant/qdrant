// Read-Only Storage Engine Hook
#[derive(Debug, Clone, Default)]
pub struct ReadOnlyStorageOptions {
    pub is_read_only: bool,
}

impl ReadOnlyStorageOptions {
    pub fn should_bypass_wal(&self) -> bool {
        self.is_read_only
    }
    
    pub fn bypass_disk_locks(&self) -> bool {
        self.is_read_only
    }
}
