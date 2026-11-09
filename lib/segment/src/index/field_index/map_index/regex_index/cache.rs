//! Compiled patterns shared across the segments and threads serving a query.

use std::sync::{Arc, LazyLock};

use ahash::AHashMap;
use parking_lot::Mutex;

use super::matcher::{RegexError, RegexMatcher};

/// Distinct patterns kept compiled. Small: a query compiles its patterns once
/// for all its segments, and a compiled pattern holds lazy DFA caches of up to
/// `DFA_CACHE_LIMIT` (8 MiB) per thread that ran it, so the worst case is
/// `CACHE_CAPACITY` × threads × 8 MiB; typical patterns take KiBs.
const CACHE_CAPACITY: usize = 16;

static CACHE: LazyLock<Mutex<AHashMap<String, Arc<RegexMatcher>>>> =
    LazyLock::new(|| Mutex::new(AHashMap::new()));

/// `pattern` compiled, reusing a recent compilation of the same pattern.
pub fn compiled(pattern: &str) -> Result<Arc<RegexMatcher>, RegexError> {
    if let Some(matcher) = CACHE.lock().get(pattern) {
        return Ok(matcher.clone());
    }

    // Compile outside the lock; a concurrent compilation of the same pattern
    // just wins or loses the insert.
    let matcher = Arc::new(RegexMatcher::new(pattern)?);

    let mut cache = CACHE.lock();
    if cache.len() >= CACHE_CAPACITY {
        cache.clear();
    }
    cache.insert(pattern.to_owned(), matcher.clone());
    Ok(matcher)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_compilations() {
        let a = compiled("cache_test_[a-z]+").unwrap();
        let b = compiled("cache_test_[a-z]+").unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert!(compiled("(cache_test").is_err());

        for i in 0..CACHE_CAPACITY * 2 {
            compiled(&format!("cache_test_{i}")).unwrap();
        }
        assert!(CACHE.lock().len() <= CACHE_CAPACITY);
    }
}
