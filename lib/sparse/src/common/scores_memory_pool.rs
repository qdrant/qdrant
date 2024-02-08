use common::types::ScoreType;
use lazy_static::lazy_static;
use parking_lot::RwLock;

type PooledScores = Vec<ScoreType>;

lazy_static! {
    /// Max number of `PooledScores` to preserve in memory
    ///
    /// If there are more concurrent requests, a new temporary vec is created dynamically.
    /// This limit is implemented to prevent memory leakage.
    /// It matches the number of logical CPU cores, to best represent the expected number of
    /// concurrent requests. Clamped between 16 and 128 to prevent extreme values.
    static ref POOL_KEEP_LIMIT: usize = common::cpu::get_num_cpus().clamp(16, 128);
}

#[derive(Debug)]
pub struct PooledScoresHandle<'a> {
    pool: &'a ScoresMemoryPool,
    pub scores: PooledScores,
}

impl<'a> PooledScoresHandle<'a> {
    fn new(pool: &'a ScoresMemoryPool, scores: PooledScores) -> Self {
        PooledScoresHandle { pool, scores }
    }
}

impl<'a> Drop for PooledScoresHandle<'a> {
    fn drop(&mut self) {
        self.pool.return_back(std::mem::take(&mut self.scores));
    }
}

#[derive(Debug)]
pub struct ScoresMemoryPool {
    pool: RwLock<Vec<PooledScores>>,
}

impl ScoresMemoryPool {
    pub fn new() -> Self {
        ScoresMemoryPool {
            pool: RwLock::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self) -> PooledScoresHandle {
        match self.pool.write().pop() {
            None => PooledScoresHandle::new(self, vec![]),
            Some(data) => PooledScoresHandle::new(self, data),
        }
    }

    fn return_back(&self, data: PooledScores) {
        let mut pool = self.pool.write();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for ScoresMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}
