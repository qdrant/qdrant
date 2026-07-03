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
        partition_batched(items, select, rest, |id| self.check(id))
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
    /// `● ● ● ● ● ○ ○ ○ ○ ○ ○ ○ ○` - Left is match.
    Match,
    /// `○ ○ ○ ○ ○ ○ ○ ○ ● ● ● ● ●` - Left is non-match.
    NonMatch,
}

/// Parameter for [`ConditionChecker::check_batched`].
///
/// An optimization hint: whether the caller needs the right part.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Rest {
    /// `● ● ● ● ● ○ ○ ○ ○ ○ ○ ○ ○` - Right side should be written.
    Keep,
    /// `● ● ● ● ● # # # # # # # #` - Right side might contain garbage.
    Drop,
}

impl Select {
    #[inline(always)]
    pub const fn is_match(self) -> bool {
        matches!(self, Select::Match)
    }
}

/// The default implementation of [`ConditionChecker::check_batched`].
pub fn partition_batched<K: CheckItem, E>(
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
        Rest::Drop => {
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
/// Using three pointers (`left`, `read`, `right`),
/// we split the slice into four parts:
/// - left, right: already written values
/// - vacant: empty slots (free to be written to)
/// - remaining (rmng): values that haven't been read yet
///
/// Pointer invariant: 0 ≤ `left` ≤ `read` ≤ `right` ≤ `data.len()`.
///
/// ```text
///  left    vacant  rmng    right
/// │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│       
/// ├───────┼───────┼───────┼───────┤
/// ↑       ↑       ↑       ↑       ↑
/// 0       left    read    right   data.len()
/// ```
pub struct Partitioner<'a, T> {
    data: &'a [Cell<T>],
    /// How many values have been written to the left side.
    left: Cell<usize>,
    /// How many values have been read from the input.
    read: Cell<usize>,
    /// Index of the first value on the right side.
    right: Cell<usize>,
}

impl<'a, T: Copy> Partitioner<'a, T> {
    pub fn new(data: &'a mut [T]) -> Self {
        // The initial state is all items are remaining, and left/right/vacant
        // parts are empty.
        //
        // │? ? ? ? ? ? ? ? ? ? ? ? ? ? ? ?│
        // ├───────────────────────────────┤
        // ↑                               ↑
        // 0 = left = read         right = data.len()
        let len = data.len();
        Self {
            data: Cell::from_mut(data).as_slice_of_cells(),
            left: Cell::new(0),
            read: Cell::new(0),
            right: Cell::new(len),
        }
    }

    /// Reads a single element by moving the read pointer forward.
    pub fn read(&self) -> Option<T> {
        // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before reading)
        //
        // │● ● ● ●│# # # # #│? ? ?│○ ○ ○ ○│    (after reading)
        //                  ↑ this value is returned
        if self.read.get() < self.right.get() {
            let value = self.data[self.read.get()].get();
            self.read.set(self.read.get() + 1);
            Some(value)
        } else {
            None
        }
    }

    /// Writes a single element to either the left or right side.
    ///
    /// Panics if you try to write past the read pointer.
    pub fn write(&self, value: T, is_left: bool) {
        if self.left.get() >= self.read.get() {
            debug_assert!(false);
            return;
        }
        if is_left {
            // Writing to the left side moves the `left` pointer forward.
            //
            // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before)
            //
            // │● ● ● ● ●│# # #│? ? ? ?│○ ○ ○ ○│    (after write)
            //          ↑ we just wrote that value
            self.data[self.left.get()].set(value);
            self.left.set(self.left.get() + 1);
        } else {
            if self.read.get() < self.right.get() {
                // Writing to the right side if there are remaining values left:
                //                swap these
                //                ↓       ↓
                // │● ● ● ●│# # # #│? ? ? ?│○ ○ ○ ○│    (before swap)
                //
                // │● ● ● ●│# # #|? ? ? ? #│○ ○ ○ ○│    (after swap)
                //
                // │● ● ● ●│# # #|? ? ? ?│○ ○ ○ ○ ○│    (after write)
                //                        ↑ we just wrote that value
                // ```
                let last_remaining = self.data[self.right.get() - 1].get();
                self.data[self.read.get() - 1].set(last_remaining);
                self.read.set(self.read.get() - 1);
                self.data[self.right.get() - 1].set(value);
                self.right.set(self.right.get() - 1);
            } else {
                // Writing to the right side if there are no remaining values:
                // │● ● ● ●│# # # # # # # #│○ ○ ○ ○│    (before)
                //
                // │● ● ● ●│# # # # # # #│○ ○ ○ ○ ○│    (after write)
                //                        ↑ we just wrote that value
                // ```
                self.data[self.read.get() - 1].set(value);
                self.read.set(self.read.get() - 1);
                self.right.set(self.right.get() - 1);
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
    /// Panics if you haven't read all the elements yet.
    pub fn finish(&self) -> usize {
        debug_assert_eq!(self.left.get(), self.read.get());
        debug_assert_eq!(self.read.get(), self.right.get());
        self.left.get()
    }
}

pub struct PartitionerIter<'a, T>(&'a Partitioner<'a, T>);

impl<T: Copy> Iterator for PartitionerIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.read()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.0.right.get() - self.0.read.get();
        (remaining, Some(remaining))
    }
}
