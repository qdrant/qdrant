use std::path::PathBuf;

/// Describes how a component expects its files to be used.
///
/// Used to compute `expected_cache_bytes` and label components for users.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStorageIntent {
    /// Data may or may not be in RAM, residency managed by OS page cache.
    Cached,
    /// Data is on disk, not expected to be in RAM.
    OnDisk,
}

/// A single file with its storage intent.
#[derive(Debug, Clone)]
pub struct ComponentFileEntry {
    pub path: PathBuf,
    pub intent: FileStorageIntent,
}

/// Memory usage reported by a single component.
///
/// Components return file entries (with intent metadata) and optionally
/// an estimate of additional heap RAM not backed by files.
#[derive(Debug, Clone)]
pub struct ComponentMemoryUsage {
    /// Files owned by this component with their storage intent.
    pub files: Vec<ComponentFileEntry>,
    /// Additional RAM not backed by files (e.g., in-memory data structures).
    pub extra_ram_bytes: Option<u64>,
}

impl ComponentMemoryUsage {
    /// Convenience: empty report (no files, no extra RAM).
    pub fn empty() -> Self {
        Self {
            files: Vec::new(),
            extra_ram_bytes: None,
        }
    }

    /// Convenience: report only non-file RAM usage.
    pub fn ram_only(bytes: u64) -> Self {
        Self {
            files: Vec::new(),
            extra_ram_bytes: Some(bytes),
        }
    }

    /// Convenience: report files with a uniform intent and no extra RAM.
    pub fn from_files(paths: Vec<PathBuf>, intent: FileStorageIntent) -> Self {
        Self {
            files: paths
                .into_iter()
                .map(|path| ComponentFileEntry { path, intent })
                .collect(),
            extra_ram_bytes: None,
        }
    }

    /// Report files with a uniform intent plus additional non-file RAM.
    pub fn from_files_and_ram(
        paths: Vec<PathBuf>,
        intent: FileStorageIntent,
        extra_ram_bytes: u64,
    ) -> Self {
        Self {
            files: paths
                .into_iter()
                .map(|path| ComponentFileEntry { path, intent })
                .collect(),
            extra_ram_bytes: Some(extra_ram_bytes),
        }
    }

    /// Merge another report into this one (concatenate files, sum RAM).
    pub fn merge(&mut self, other: &ComponentMemoryUsage) {
        self.files.extend(other.files.iter().cloned());
        match (self.extra_ram_bytes, other.extra_ram_bytes) {
            (Some(a), Some(b)) => self.extra_ram_bytes = Some(a + b),
            (None, Some(b)) => self.extra_ram_bytes = Some(b),
            (_, None) => {}
        }
    }
}

/// Trait for components to report their memory footprint.
///
/// Implementations should be cheap (no I/O). The actual `mincore` measurement
/// happens in the aggregation layer after file entries are collected.
pub trait MemoryReporter {
    fn memory_usage(&self) -> ComponentMemoryUsage;
}
