use common::defaults::POOL_KEEP_LIMIT;
use common::ext::aligned_vec::{AVec, RuntimeAlign};
use common::types::ScoreType;
use parking_lot::Mutex;
use typed_arena::Arena;

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
            arena: SearchScratchArena::new_slow(),
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
    pub(crate) arena: SearchScratchArena,
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

/// Hacky wrapper around [`Arena`] with [`Self::gc`] method.
///
/// TODO: get rid of this struct once [`Arena`] gets `clear()` method, and use
/// type alias instead.
/// https://github.com/thomcc/rust-typed-arena/issues/64
// pub type SearchScratchArena = Arena<AVec<u8, RuntimeAlign>>;
pub struct SearchScratchArena(Arena<AVec<u8, RuntimeAlign>>);

impl SearchScratchArena {
    /// Same as [`Arena::new`].
    /// Renamed to `new_slow` to point out that [`Arena::new`] allocates.
    pub fn new_slow() -> SearchScratchArena {
        SearchScratchArena(Arena::new())
    }

    /// Free memory if the arena got too big.
    /// Poor's man version of `Arena::clear()`.
    pub fn gc(&mut self) {
        // Too small => frequent reallocations
        // Too big => memory bloat
        const ARBITRARY_LIMIT: usize = 256;
        if self.0.len() > ARBITRARY_LIMIT {
            self.0 = Arena::new();
        }

        for vec in self.0.iter_mut() {
            *vec = AVec::new(1);
        }
    }
}

impl std::ops::Deref for SearchScratchArena {
    type Target = Arena<AVec<u8, RuntimeAlign>>;
    fn deref(&self) -> &Arena<AVec<u8, RuntimeAlign>> {
        &self.0
    }
}
