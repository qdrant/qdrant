//! Counts the posting list lookups of per-point token checks, so tests can see the order
//! the tokens are checked in: it decides how many lookups reject or accept a point.
//! Compiles to nothing outside tests.

#[cfg(test)]
thread_local! {
    static POSTING_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Record one posting list lookup on this thread.
#[inline(always)]
pub(super) fn count_posting_lookup() {
    #[cfg(test)]
    POSTING_LOOKUPS.set(POSTING_LOOKUPS.get() + 1);
}

/// Posting list lookups made on this thread by `f`.
#[cfg(test)]
pub(crate) fn posting_lookups(f: impl FnOnce()) -> usize {
    let before = POSTING_LOOKUPS.get();
    f();
    POSTING_LOOKUPS.get() - before
}
