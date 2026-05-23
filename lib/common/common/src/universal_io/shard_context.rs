/// Per-shard context bundling all backend-instance state needed to open files
/// through any [`UniversalRead`](super::UniversalRead) implementation.
///
/// Constructed once at shard load. Each backend implements
/// [`UniversalRead::extras_from_context`](super::UniversalRead::extras_from_context)
/// to pull out the slice it needs, so a single shard can mix backends across
/// its components.
///
/// Today this is an empty placeholder. As backends migrate off of process-wide
/// `OnceLock` singletons, their state (cache controller, mirror dirs, io_uring
/// runtime, default mmap advice) lands here.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ShardStorageContext {}

impl ShardStorageContext {
    pub fn new() -> Self {
        Self::default()
    }
}
