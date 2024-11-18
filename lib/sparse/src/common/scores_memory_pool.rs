use common::defaults::POOL_KEEP_LIMIT;
use common::types::ScoreType;
use parking_lot::Mutex;

type PooledScores = Vec<ScoreType>;

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

impl Drop for PooledScoresHandle<'_> {
    fn drop(&mut self) {
        self.pool.return_back(std::mem::take(&mut self.scores));
    }
}

#[derive(Debug)]
pub struct ScoresMemoryPool {
    pool: Mutex<Vec<PooledScores>>,
}

impl ScoresMemoryPool {
    pub fn new() -> Self {
        ScoresMemoryPool {
            pool: Mutex::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self) -> PooledScoresHandle {
        match self.pool.lock().pop() {
            None => PooledScoresHandle::new(self, vec![]),
            Some(data) => PooledScoresHandle::new(self, data),
        }
    }

    fn return_back(&self, data: PooledScores) {
        let mut pool = self.pool.lock();
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
