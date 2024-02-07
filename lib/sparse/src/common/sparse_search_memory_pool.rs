use lazy_static::lazy_static;
use parking_lot::RwLock;

type SparseSearchMemory = Vec<f32>;

lazy_static! {
    /// Max number of SparseSearchMemory to preserve in memory
    ///
    /// If there are more concurrent requests, a new temporary list is created dynamically.
    /// This limit is implemented to prevent memory leakage.
    /// It matches the number of logical CPU cores, to best represent the expected number of
    /// concurrent requests. Clamped between 16 and 128 to prevent extreme values.
    static ref POOL_KEEP_LIMIT: usize = common::cpu::get_num_cpus().clamp(16, 128);
}

#[derive(Debug)]
pub struct SparseSearchMemoryHandle<'a> {
    pool: &'a SparseSearchMemoryPool,
    pub memory: SparseSearchMemory,
}

impl<'a> SparseSearchMemoryHandle<'a> {
    fn new(pool: &'a SparseSearchMemoryPool, memory: SparseSearchMemory) -> Self {
        SparseSearchMemoryHandle { pool, memory }
    }
}

impl<'a> Drop for SparseSearchMemoryHandle<'a> {
    fn drop(&mut self) {
        self.pool.return_back(std::mem::take(&mut self.memory));
    }
}

#[derive(Debug)]
pub struct SparseSearchMemoryPool {
    pool: RwLock<Vec<SparseSearchMemory>>,
}

impl SparseSearchMemoryPool {
    pub fn new() -> Self {
        SparseSearchMemoryPool {
            pool: RwLock::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self) -> SparseSearchMemoryHandle {
        match self.pool.write().pop() {
            None => SparseSearchMemoryHandle::new(self, vec![]),
            Some(data) => SparseSearchMemoryHandle::new(self, data),
        }
    }

    fn return_back(&self, data: SparseSearchMemory) {
        let mut pool = self.pool.write();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for SparseSearchMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}
