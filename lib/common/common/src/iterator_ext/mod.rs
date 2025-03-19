#[cfg(any(test, feature = "testing"))]
use std::fmt::Debug;

use check_stopped::CheckStopped;
use on_final_count::OnFinalCount;

pub(super) mod on_final_count;

mod check_stopped;

pub trait IteratorExt: Iterator {
    /// Periodically check if the iteration should be stopped.
    /// The closure `f` is called every `every` iterations, and should return `true` if the iteration should be stopped.
    fn check_stop_every<F>(self, every: usize, f: F) -> CheckStopped<Self, F>
    where
        F: FnMut() -> bool,
        Self: Sized,
    {
        CheckStopped::new(self, every, f)
    }

    /// Periodically check if the iteration should be stopped.
    /// The closure `f` is called every 500 iterations, and should return `true` if the iteration should be stopped.
    #[inline]
    fn check_stop<F>(self, f: F) -> CheckStopped<Self, F>
    where
        F: Fn() -> bool,
        Self: Sized,
    {
        self.check_stop_every(500, f)
    }

    /// Will execute the callback when the iterator is dropped.
    ///
    /// The callback receives the total number of times `.next()` was called on the iterator,
    /// including the final one where it usually returns `None`.
    ///
    /// Consider subtracting 1 if the final `None` is not needed.
    fn on_final_count<F>(self, f: F) -> OnFinalCount<Self, F>
    where
        F: FnMut(usize),
        Self: Sized,
    {
        OnFinalCount::new(self, f)
    }
}

impl<I: Iterator> IteratorExt for I {}

/// Checks that [`Iterator::fold()`] yields same values as [`Iterator::next()`].
/// Panics if it is not.
#[cfg(any(test, feature = "testing"))]
pub fn check_iterator_fold<I: Iterator, F: Fn() -> I>(mk_iter: F)
where
    I::Item: PartialEq + Debug,
{
    const EXTRA_COUNT: usize = 3;

    // Treat values returned by `next()` as reference.
    let mut reference_values = Vec::new();
    let mut iter = mk_iter();
    #[expect(
        clippy::while_let_on_iterator,
        reason = "Reference implementation: call bare-bones `next()` explicitly"
    )]
    while let Some(value) = iter.next() {
        reference_values.push(value);
    }

    // Check that `next()` after exhaustion returns None.
    for _ in 0..EXTRA_COUNT {
        assert!(
            iter.next().is_none(),
            "Iterator returns values after it's exhausted",
        );
    }
    drop(iter);

    // Check `fold()` yields same values as `next()`.
    let mut values_for_fold = Vec::new();
    for split_at in 0..reference_values.len() + EXTRA_COUNT {
        let mut iter = mk_iter();
        values_for_fold.clear();

        for _ in 0..split_at.min(reference_values.len()) {
            values_for_fold.push(iter.next().expect("not enough values"));
        }
        // Call `next()` a few times to check that these extra calls won't break
        // `fold()`.
        for _ in 0..split_at.saturating_sub(reference_values.len()) {
            assert!(iter.next().is_none());
        }

        let acc = iter.fold(values_for_fold.len(), |acc, value| {
            assert_eq!(acc, values_for_fold.len());
            values_for_fold.push(value);
            acc + 1
        });
        assert_eq!(reference_values, values_for_fold);
        assert_eq!(acc, values_for_fold.len());
    }
}

/// Checks that [`ExactSizeIterator::len()`] returns correct length.
/// Panics if it is not.
#[cfg(any(test, feature = "testing"))]
pub fn check_exact_size_iterator_len<I: ExactSizeIterator>(mut iter: I) {
    for expected_len in (0..iter.len()).rev() {
        iter.next();
        assert_eq!(iter.len(), expected_len);
    }
    assert!(iter.next().is_none());
    assert_eq!(iter.len(), 0);
}
