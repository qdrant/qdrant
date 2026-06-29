use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;

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
        &mut self,
        items: &mut [K],
        select: Select,
        rest: Rest,
    ) -> Result<usize, Self::Error>
    where
        Self: Sized,
    {
        default_check_batched(items, select, rest, |id| self.check(id))
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
        matches!(self, Select::Matches)
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
            loop {
                while lo < hi && pred(items[lo].point_id())? == select.is_match() {
                    lo += 1;
                }
                while lo < hi && pred(items[hi - 1].point_id())? != select.is_match() {
                    hi -= 1;
                }
                if lo >= hi {
                    break;
                }
                items.swap(lo, hi - 1);
                lo += 1;
                hi -= 1;
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
        &mut self,
        ids: &mut [K],
        select: Select,
        _rest: Rest,
    ) -> Result<usize, E> {
        // Every id is on the same side, so no rearrangement is needed.
        Ok(if self.0 == select.is_match() {
            ids.len()
        } else {
            0
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
        if self.left_end.get() >= self.unread_start.get() {
            debug_assert!(false);
            return;
        }
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
        debug_assert_eq!(self.left_end.get(), self.unread_start.get());
        debug_assert_eq!(self.unread_start.get(), self.right_start.get());
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
