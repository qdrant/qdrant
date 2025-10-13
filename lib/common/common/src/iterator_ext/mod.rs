#[cfg(any(test, feature = "testing"))]
use std::fmt::Debug;
use std::ops::ControlFlow;

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

    /// Consume the iterator and call `black_box` on each item, for benchmarking purposes.
    fn black_box(self)
    where
        Self: Sized,
    {
        self.for_each(|p| {
            std::hint::black_box(p);
        });
    }
}

pub trait IteratorTryFold: Iterator {
    /// Similar to [`Iterator::try_fold()`], but works on [`ControlFlow`]
    /// rather than [`std::ops::Try`], making it implementable on stable Rust.
    #[inline]
    fn try_fold_simple<Acc, F, R>(&mut self, acc: Acc, f: F) -> ControlFlow<R, Acc>
    where
        F: FnMut(Acc, Self::Item) -> ControlFlow<R, Acc>,
        Self: Sized,
    {
        self.try_fold(acc, f)
    }

    /// Similar to [`Iterator::try_for_each()`].
    #[inline]
    fn try_for_each_simple<F, R>(&mut self, f: F) -> ControlFlow<R>
    where
        F: FnMut(Self::Item) -> ControlFlow<R>,
        Self: Sized,
    {
        #[inline]
        fn call<T, R>(mut f: impl FnMut(T) -> R) -> impl FnMut((), T) -> R {
            move |(), x| f(x)
        }

        self.try_fold_simple((), call(f))
    }
}

impl<I: Iterator> IteratorExt for I {}

/// Checks that the following methods are consistent with each other:
///
/// - [`Iterator::next()`]
/// - [`Iterator::fold()`]
/// - [`IteratorTryFold::try_fold_simple()`]
///
/// Panics if they are not.
#[cfg(any(test, feature = "testing"))]
pub fn check_iterator_fold<I: IteratorTryFold + Clone>(iter: I)
where
    I::Item: PartialEq + Debug,
{
    const EXTRA_COUNT: usize = 3;

    // Treat values returned by `next()` as reference.
    let mut reference_values = Vec::new();
    {
        let mut iter = iter.clone();
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
    }

    let mut values = Vec::new();
    for split_at in 0..reference_values.len() + EXTRA_COUNT {
        let mut iter = iter.clone();
        values.clear();

        for _ in 0..split_at.min(reference_values.len()) {
            values.push(iter.next().expect("not enough values"));
        }
        // Call `next()` a few times to check that these extra calls won't break
        // `fold()`.
        for _ in 0..split_at.saturating_sub(reference_values.len()) {
            assert!(iter.next().is_none());
        }

        // Check `fold()`. The `values` vec consists of two parts:
        //
        // │ from `.next()`     │ from `.fold()`     │
        // ├────────────────────┼────────────────────┤
        // 0                 split_at      reference_values.len()
        let acc = iter.clone().fold(values.len(), |acc, value| {
            assert_eq!(acc, values.len());
            values.push(value);
            acc + 1
        });
        assert_eq!(reference_values, values);
        assert_eq!(acc, values.len());

        // Check `try_fold_simple()`. The `values` vec consists of three parts:
        //
        // │ from `.next()`   │ from `.try_fold_simple()`   │ from `.next()`   │
        // ├──────────────────┼─────────────────────────────┼──────────────────┤
        // 0               split_at                     split_at2    reference_values.len()
        for split_at2 in (split_at + 1..=reference_values.len() + 1).take(EXTRA_COUNT) {
            // Reuse the first part by cloning the iterator.
            let mut iter = iter.clone();
            values.truncate(split_at);

            // Second part: `try_fold_simple()`.
            let acc = iter.try_fold_simple(split_at, |acc, value| {
                assert_eq!(acc, values.len());
                values.push(value);
                if values.len() >= split_at2 {
                    return ControlFlow::<(), usize>::Break(());
                }
                ControlFlow::<(), usize>::Continue(acc + 1)
            });
            if split_at2 > reference_values.len() {
                assert_eq!(&reference_values, &values);
                assert_eq!(acc, ControlFlow::Continue(reference_values.len()));
            } else {
                assert_eq!(&reference_values[..split_at2], values);
                assert_eq!(acc, ControlFlow::Break(()));
            }

            // Third part: continue with `next()`.
            for i in split_at2..reference_values.len() {
                let value = iter.next().expect("not enough values");
                assert_eq!(&reference_values[i], &value);
            }
            assert!(iter.next().is_none());
        }
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
