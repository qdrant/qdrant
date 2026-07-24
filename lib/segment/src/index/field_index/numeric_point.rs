use std::fmt::Debug;

use common::types::PointOffsetType;
use num_traits::Num;
use ordered_float::OrderedFloat;
use serde::Serialize;

pub use self::point::Point;
use crate::types::{FloatPayloadType, IntPayloadType, Range};

// bytemuck macros expand to code that triggers this clippy lint
// The only reason this is its own module is so that we scope the lint suppression
#[expect(clippy::multiple_bound_locations)]
mod point {
    use common::types::PointOffsetType;

    use super::Numericable;

    #[expect(clippy::derive_ord_xor_partial_ord)]
    #[derive(
        PartialEq,
        PartialOrd,
        Debug,
        Clone,
        Copy,
        serde::Serialize,
        serde::Deserialize,
        bytemuck::Pod,
        bytemuck::Zeroable,
    )]
    #[repr(C, packed)]
    pub struct Point<T: Numericable> {
        pub val: T,
        pub idx: PointOffsetType,
        #[serde(skip)]
        _padding: T::PointPadding,
    }

    impl<T: Numericable> Point<T> {
        pub fn new(val: T, idx: PointOffsetType) -> Self {
            Self {
                val,
                idx,
                _padding: bytemuck::Zeroable::zeroed(),
            }
        }
    }

    impl<T: PartialEq + Numericable> Eq for Point<T> {}

    impl<T: PartialOrd + Copy + Numericable> Ord for Point<T> {
        fn cmp(&self, other: &Point<T>) -> std::cmp::Ordering {
            (self.val, self.idx)
                .partial_cmp(&(other.val, other.idx))
                .unwrap()
        }
    }
}

/// Calculate the exact padding so that the [`Point<T>`] type would have explicit alignment, so that
/// it is able to derive [`bytemuck::Pod`] and use #[repr(packed)] safely
const fn derive_point_padding<T: bytemuck::Pod>() -> usize {
    struct Point<T> {
        _t: T,
        _idx: PointOffsetType,
    }
    let align = std::mem::align_of::<Point<T>>();

    // Since we are adding padding at the end, we need to ensure that the align of T is larger the one
    // of PointOffsetType, so that there is no inter-field padding
    assert!(std::mem::align_of::<T>() >= std::mem::align_of::<PointOffsetType>());

    align - (std::mem::size_of::<T>() + std::mem::size_of::<PointOffsetType>()) % align
}

/// A trait that should represent common properties of integer and floating point types.
/// In particular, i64 and f64.
pub trait Numericable: Num + PartialEq + PartialOrd + Copy + bytemuck::Pod + Send {
    /// This is to be able to derive [`bytemuck::Pod`] for [`Point<T>`], which is required for safe de/serialization.
    ///
    /// Since we need ['Point<T>`] to be repr(packed), this padding must be picked to be the next multiple of the largest
    /// field in the struct, which fits the entire struct.
    type PointPadding: bytemuck::Pod
        + Debug
        + Default
        + PartialEq
        + PartialOrd
        + Serialize
        + for<'de> serde::Deserialize<'de>
        + Send;

    fn min_value() -> Self;
    fn max_value() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(x: f64) -> Self;
    fn from_i64(x: IntPayloadType) -> Self;
    fn from_u128(x: u128) -> Self;
    fn min(self, b: Self) -> Self {
        if self < b { self } else { b }
    }
    fn max(self, b: Self) -> Self {
        if self > b { self } else { b }
    }
    fn abs_diff(self, b: Self) -> Self {
        if self > b { self - b } else { b - self }
    }

    /// Convert a fractional `f64` range into an equivalent `Self`-typed range
    /// that preserves the original predicate for every value of `Self`.
    ///
    /// The default impl uses [`Self::from_f64`] per bound and is correct
    /// for floating-point `Self`. Integer types must override to round
    /// each bound *away* from the matching set — ceil for `lt`/`gte`,
    /// floor for `gt`/`lte` — so that fractional predicates like
    /// `gte: 1.5` do not slip the integer `1` into the result set (#9049).
    fn from_f64_range(range: Range<OrderedFloat<FloatPayloadType>>) -> Range<Self> {
        range.map(|x| Self::from_f64(x.0))
    }

    fn from_i64_range(range: Range<IntPayloadType>) -> Range<Self> {
        range.map(Self::from_i64)
    }
}

impl Numericable for i64 {
    type PointPadding = [u8; derive_point_padding::<Self>()];

    fn min_value() -> Self {
        i64::MIN
    }
    fn max_value() -> Self {
        i64::MAX
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn from_f64(x: f64) -> Self {
        x as Self
    }
    fn from_i64(x: IntPayloadType) -> Self {
        x
    }
    fn from_u128(x: u128) -> Self {
        x as i64
    }
    fn abs_diff(self, b: Self) -> Self {
        i64::abs_diff(self, b) as i64
    }

    fn from_f64_range(range: Range<OrderedFloat<FloatPayloadType>>) -> Range<Self> {
        Range {
            lt: range.lt.map(|f| f.0.ceil() as Self),
            gt: range.gt.map(|f| f.0.floor() as Self),
            gte: range.gte.map(|f| f.0.ceil() as Self),
            lte: range.lte.map(|f| f.0.floor() as Self),
        }
    }
}

impl Numericable for f64 {
    type PointPadding = [u8; derive_point_padding::<Self>()];

    fn min_value() -> Self {
        f64::MIN
    }
    fn max_value() -> Self {
        f64::MAX
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn from_f64(x: f64) -> Self {
        x
    }
    fn from_i64(x: IntPayloadType) -> Self {
        x as Self
    }
    fn from_u128(x: u128) -> Self {
        x as Self
    }
}

impl Numericable for u128 {
    type PointPadding = [u8; derive_point_padding::<Self>()];

    fn min_value() -> Self {
        u128::MIN
    }

    fn max_value() -> Self {
        u128::MAX
    }

    fn to_f64(self) -> f64 {
        self as f64
    }

    fn from_f64(x: f64) -> Self {
        x as u128
    }

    fn from_i64(x: IntPayloadType) -> Self {
        u128::try_from(x).unwrap_or_default()
    }

    fn from_u128(x: u128) -> Self {
        x
    }

    fn abs_diff(self, b: Self) -> Self {
        u128::abs_diff(self, b)
    }

    fn from_f64_range(range: Range<OrderedFloat<FloatPayloadType>>) -> Range<Self> {
        Range {
            lt: range.lt.map(|f| f.0.ceil() as Self),
            gt: range.gt.map(|f| f.0.floor() as Self),
            gte: range.gte.map(|f| f.0.ceil() as Self),
            lte: range.lte.map(|f| f.0.floor() as Self),
        }
    }
}
