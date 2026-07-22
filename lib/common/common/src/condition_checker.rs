use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;

#[cfg(feature = "testing")]
use rand::RngExt;
#[cfg(feature = "testing")]
use rand::rngs::StdRng;

use crate::types::{PointOffsetType, ScoredPointOffset};

/// A check that tests whether points satisfy a condition.
pub trait ConditionChecker {
    type Error;

    fn check(&self, point_id: PointOffsetType) -> Result<bool, Self::Error>;

    /// Same as [`Self::check`] but ignoring errors.
    fn check_infallible(&self, point_id: PointOffsetType) -> bool {
        // This method is a workaround to keep the performance on-par.
        // It's faster to do `.unwrap_or(false)` *inside* the trait method
        // because the compiler can't inline `&dyn Trait` methods.
        //
        // TODO(uio): remove this method and handle errors properly.
        self.check(point_id).unwrap_or(false)
    }

    /// Rearranges items in-place, separating those that satisfy the condition
    /// from those that don't.
    ///
    /// Returns the partition point (aka the length of the left side).
    ///
    /// ```text
    /// Input:   ○ ○ ● ○ ○ ○ ● ○ ● ○ ● ● ○
    /// Output:  ● ● ● ● ● ○ ○ ○ ○ ○ ○ ○ ○
    ///         └─────────┴───────────────┘
    ///                   ↑ partition point
    /// ```
    fn check_batched<K: CheckItem>(
        &self,
        items: &mut [K],
        select: Select,
        rest: Rest,
    ) -> Result<usize, Self::Error>
    where
        Self: Sized,
    {
        panic!()
    }
}

/// See [`ConditionChecker::check_batched`].
pub trait CheckItem: Copy + fmt::Debug {
    fn point_id(self) -> PointOffsetType;
}

impl CheckItem for PointOffsetType {
    fn point_id(self) -> PointOffsetType {
        self
    }
}

impl CheckItem for ScoredPointOffset {
    fn point_id(self) -> PointOffsetType {
        self.idx
    }
}

/// Parameter for [`ConditionChecker::check_batched`].
///
/// Controls whether the left side should contain the matching or non-matching
/// items.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Select {
    /// `● ● ● ● ● ○ ○ ○ ○ ○ ○ ○ ○` - Left are matches.
    Matches,
    /// `○ ○ ○ ○ ○ ○ ○ ○ ● ● ● ● ●` - Left are non-matches.
    NonMatches,
}

/// Parameter for [`ConditionChecker::check_batched`].
///
/// An optimization hint: whether the caller needs the right part.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Rest {
    /// `● ● ● ● ● ○ ○ ○ ○ ○ ○ ○ ○` - Right side should be written.
    Keep,
    /// `● ● ● ● ● # # # # # # # #` - Right side might contain garbage.
    Discard,
}

impl Select {
    #[inline(always)]
    pub const fn is_match(self) -> bool {
        match self {
            Select::Matches => true,
            Select::NonMatches => false,
        }
    }
}

impl Rest {
    /// Helper for sequencing checks.
    #[inline(always)]
    pub const fn keep_if(self, more_checks_follow: bool) -> Rest {
        if more_checks_follow { Rest::Keep } else { self }
    }
}

/// The default implementation of [`ConditionChecker::check_batched`].
pub fn default_check_batched<K: CheckItem, E>(
    items: &mut [K],
    select: Select,
    rest: Rest,
    mut pred: impl FnMut(PointOffsetType) -> Result<bool, E>,
) -> Result<usize, E> {
    match rest {
        Rest::Keep => {
            let mut lo = 0;
            let mut hi = items.len();
            'outer: while lo < hi {
                if pred(items[lo].point_id())? == select.is_match() {
                    lo += 1;
                    continue;
                }
                // items[lo] doesn't belong on the left: scan down for one that does.
                loop {
                    hi -= 1;
                    if lo == hi {
                        break 'outer;
                    }
                    if pred(items[hi].point_id())? == select.is_match() {
                        break;
                    }
                }
                items.swap(lo, hi);
                lo += 1;
            }
            Ok(lo)
        }
        Rest::Discard => {
            let mut w = 0;
            for i in 0..items.len() {
                let id = items[i];
                if pred(id.point_id())? == select.is_match() {
                    if w != i {
                        items[w] = id;
                    }
                    w += 1;
                }
            }
            Ok(w)
        }
    }
}

/// A checker that ignores the point and always returns the same value.
pub struct ConstantConditionChecker<E>(bool, PhantomData<E>);

impl<E> ConstantConditionChecker<E> {
    pub const MATCH_NONE: Self = Self(false, PhantomData);

    pub const MATCH_ALL: Self = Self(true, PhantomData);

    pub const fn new(value: bool) -> Self {
        ConstantConditionChecker(value, PhantomData)
    }
}

impl<E> ConditionChecker for ConstantConditionChecker<E> {
    type Error = E;

    fn check(&self, _point_id: PointOffsetType) -> Result<bool, E> {
        Ok(self.0)
    }

    fn check_batched<K: CheckItem>(
        &self,
        ids: &mut [K],
        select: Select,
        _rest: Rest,
    ) -> Result<usize, E> {
        // Every id is on the same side, so no rearrangement is needed.
        Ok(match self.0 == select.is_match() {
            true => ids.len(),
            false => 0,
        })
    }
}

/// A helper struct to use in [`ConditionChecker::check_batched`] impls.
///
/// Lets you partition a slice in-place.
///
/// # Implementation
///
/// Using three pointers (`left_end`, `unread_start`, `right_start`),
/// we split the slice into four parts:
/// - left, right: already written values
/// - vacant: empty slots (free to be written to)
/// - unread: values that haven't been read yet
///
/// Pointer invariant:
/// 0 ≤ `left_end` ≤ `unread_start` ≤ `right_start` ≤ `data.len()`.
///
/// ```text
///  left    vacant  unread  right
/// │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│       
/// ├───────┼───────┼───────┼───────┤
/// ↑       ↑       ↑       ↑       ↑
/// 0       │  unread_start │   data.len()
///      left_end       right_start
/// ```
pub struct Partitioner<'a, T> {
    data: &'a [Cell<T>],
    /// How many values have been written to the left side.
    left_end: Cell<usize>,
    /// Index of the first unread value.
    unread_start: Cell<usize>,
    /// Index of the first value on the right side.
    right_start: Cell<usize>,
}

impl<'a, T: Copy> Partitioner<'a, T> {
    pub fn new(data: &'a mut [T]) -> Self {
        // The initial state is all items are unread, and left/right/vacant
        // parts are empty.
        //
        // │? ? ? ? ? ? ? ? ? ? ? ? ? ? ? ?│
        // ├───────────────────────────────┤
        // ↑                               ↑
        // 0 = left_end = unread_start     right_start = data.len()
        let len = data.len();
        Self {
            data: Cell::from_mut(data).as_slice_of_cells(),
            left_end: Cell::new(0),
            unread_start: Cell::new(0),
            right_start: Cell::new(len),
        }
    }

    /// Reads a single element by moving `unread_start` forward.
    pub fn read(&self) -> Option<T> {
        // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before reading)
        //
        // │● ● ● ●│# # # # #│? ? ?│○ ○ ○ ○│    (after reading)
        //                  ↑ this value is returned
        if self.unread_start.get() < self.right_start.get() {
            let value = self.data[self.unread_start.get()].get();
            self.unread_start.set(self.unread_start.get() + 1);
            Some(value)
        } else {
            None
        }
    }

    /// Writes a single element to either the left or right side.
    ///
    /// Panics if you try to write more than was read.
    pub fn write(&self, value: T, is_left: bool) {
        assert!(self.left_end.get() < self.unread_start.get());
        if is_left {
            // Writing to the left side moves `left_end` forward.
            //
            // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before)
            //
            // │● ● ● ● ●│# # #│? ? ? ?│○ ○ ○ ○│    (after write)
            //          ↑ we just wrote that value
            self.data[self.left_end.get()].set(value);
            self.left_end.set(self.left_end.get() + 1);
        } else {
            if self.unread_start.get() < self.right_start.get() {
                // Writing to the right side if there are unread values left:
                //                swap these
                //                ↓       ↓
                // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before swap)
                //
                // │● ● ● ●│# # #│? ? ? ? #│○ ○ ○ ○│    (after swap)
                //
                // │● ● ● ●│# # #│? ? ? ?│○ ○ ○ ○ ○│    (after write)
                //                        ↑ we just wrote that value
                // ```
                let last_unread = self.data[self.right_start.get() - 1].get();
                self.data[self.unread_start.get() - 1].set(last_unread);
                self.unread_start.set(self.unread_start.get() - 1);
                self.data[self.right_start.get() - 1].set(value);
                self.right_start.set(self.right_start.get() - 1);
            } else {
                // Writing to the right side if there are no unread values:
                // │● ● ● ●│# # # # # # # #│○ ○ ○ ○│    (before)
                //
                // │● ● ● ●│# # # # # # #│○ ○ ○ ○ ○│    (after write)
                //                        ↑ we just wrote that value
                // ```
                self.data[self.unread_start.get() - 1].set(value);
                self.unread_start.set(self.unread_start.get() - 1);
                self.right_start.set(self.right_start.get() - 1);
            }
        }
    }

    /// A convenience adapter - returns an iterator that calls [`Self::read()`]
    /// on each iteration.
    pub fn iter(&self) -> PartitionerIter<'_, T> {
        PartitionerIter(self)
    }

    /// "I'm done reading. Where is the partition point?"
    ///
    /// Panics if you haven't read/written all the elements yet.
    pub fn finish(&self) -> usize {
        assert_eq!(self.left_end.get(), self.unread_start.get());
        assert_eq!(self.unread_start.get(), self.right_start.get());
        self.left_end.get()
    }
}

pub struct PartitionerIter<'a, T>(&'a Partitioner<'a, T>);

impl<T: Copy> Iterator for PartitionerIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.read()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let unread = self.0.right_start.get() - self.0.unread_start.get();
        (unread, Some(unread))
    }
}

/// Assert that [`ConditionChecker::check_batched`] returns the same values as
/// [`ConditionChecker::check`].
#[cfg(feature = "testing")]
pub fn assert_congruence<C>(checker: &C, num_points: usize, rng: &mut StdRng)
where
    C: ConditionChecker<Error: std::fmt::Debug>,
{
    let num_points = num_points as u32;
    let far_ids = [num_points, num_points + 100, u32::MAX / 2, u32::MAX - 1];
    let rand_id = |rng: &mut StdRng| match rng.random_bool(0.9) {
        true => rng.random_range(0..num_points.max(1)),
        false => far_ids[rng.random_range(0..far_ids.len())],
    };

    let check = |mut input: Vec<PointOffsetType>| {
        input.sort_unstable();

        for select in [Select::Matches, Select::NonMatches] {
            let want = input
                .iter()
                .copied()
                .filter(|&id| checker.check(id).unwrap() == select.is_match())
                .collect::<Vec<_>>();

            for rest in [Rest::Keep, Rest::Discard] {
                let mut buf = input.clone();
                let split = checker.check_batched(&mut buf, select, rest).unwrap();
                buf[..split].sort_unstable();
                assert_eq!(&buf[..split], want, "left, {select:?} {rest:?}");
                if rest == Rest::Keep {
                    buf.sort_unstable();
                    assert_eq!(&buf, &input, "kept, {select:?}");
                }
            }
        }
    };

    check(vec![]);
    check(vec![rng.random_range(0..num_points.max(1))]);
    check((0..num_points).chain(far_ids).collect());
    check((0..2048).map(|_| rand_id(rng)).collect());
    for _ in 0..5 {
        let len = rng.random_range(0..300);
        check((0..len).map(|_| rand_id(rng)).collect());
    }
}
