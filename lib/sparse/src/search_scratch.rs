use blink_alloc::Blink;
use common::defaults::POOL_KEEP_LIMIT;
use common::types::ScoreType;
use parking_lot::Mutex;

#[derive(Debug)]
pub struct SearchScratchPool {
    pool: Mutex<Vec<SearchScratchScores>>,
}

impl SearchScratchPool {
    #[expect(clippy::new_without_default)]
    pub fn new() -> SearchScratchPool {
        SearchScratchPool {
            pool: Mutex::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    /// Take a single [`SearchScratch`] from the pool.
    pub fn get(&self) -> SearchScratch<'_> {
        let scores = self.pool.lock().pop().unwrap_or_default();
        SearchScratch {
            pool: self,
            scores,
            arena: Blink::new(),
        }
    }
}

/// Scratch space for use by [`crate::index::search_context::SearchContext`].
pub struct SearchScratch<'a> {
    /// The pool to return this scratch on drop.
    pool: &'a SearchScratchPool,
    /// Used for batched scoring.
    pub(crate) scores: SearchScratchScores,
    /// Used to own/store posting list bytes while reading them from the file.
    pub(crate) arena: Blink,
}

impl SearchScratch<'_> {
    #[cfg(test)]
    pub fn new_for_test() -> SearchScratch<'static> {
        use std::sync::OnceLock;
        static POOL: OnceLock<SearchScratchPool> = OnceLock::new();
        POOL.get_or_init(SearchScratchPool::new).get()
    }
}

type SearchScratchScores = Vec<ScoreType>;

impl Drop for SearchScratch<'_> {
    fn drop(&mut self) {
        let SearchScratch {
            pool: SearchScratchPool { pool },
            scores,
            arena: _,
        } = self;
        let mut pool = pool.lock();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(std::mem::take(scores));
        }
    }
}
