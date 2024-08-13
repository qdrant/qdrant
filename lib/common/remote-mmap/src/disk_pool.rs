use crate::file_reader::FileReader;

struct DiskPoolConfig {
    /// Total amount of disk space, allowed for the pool
    max_pool_size_bytes: usize,
    /// Size of a minimal downloadable block
    block_size_bytes: usize,
    /// Size of the chunk, stored on local disk
    chunk_size_bytes: usize,
}

struct DiskPool<FR: FileReader> {
    file_reader: FR,
    config: DiskPoolConfig,
    // pool: Vec<RwLock<>>
}
