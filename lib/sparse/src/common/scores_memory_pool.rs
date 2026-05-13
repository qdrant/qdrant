// TODO: We added `Bump` to the pool, so it's no longer "scores only" pool.
//       So, worth renaming it.

use bumpalo::Bump;
use common::defaults::POOL_KEEP_LIMIT;
use common::types::ScoreType;
use parking_lot::Mutex;

type PooledScores = Vec<ScoreType>;

#[derive(Debug)]
pub struct PooledScoresHandle<'a> {
    pool: &'a ScoresMemoryPool,
    pub entry: PoolEntry,
}

impl<'a> PooledScoresHandle<'a> {
    fn new(pool: &'a ScoresMemoryPool, entry: PoolEntry) -> Self {
        PooledScoresHandle { pool, entry }
    }
}

impl Drop for PooledScoresHandle<'_> {
    fn drop(&mut self) {
        self.pool.return_back(std::mem::take(&mut self.entry));
    }
}

#[derive(Debug, Default)]
pub struct PoolEntry {
    pub scores: PooledScores,
    pub bump: Bump,
}

#[derive(Debug)]
pub struct ScoresMemoryPool {
    pool: Mutex<Vec<PoolEntry>>,
}

impl ScoresMemoryPool {
    pub fn new() -> Self {
        ScoresMemoryPool {
            pool: Mutex::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self) -> PooledScoresHandle<'_> {
        let entry = self.pool.lock().pop().unwrap_or_default();
        PooledScoresHandle::new(self, entry)
    }

    fn return_back(&self, mut entry: PoolEntry) {
        entry.bump.reset();
        let mut pool = self.pool.lock();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(entry);
        }
    }
}

impl Default for ScoresMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}
