use std::fmt::Debug;

use common::types::PointOffsetType;
use num_traits::Num;
use serde::Serialize;

pub use self::point::Point;

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
pub trait Numericable: Num + PartialEq + PartialOrd + Copy + bytemuck::Pod {
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
        + for<'de> serde::Deserialize<'de>;

    fn min_value() -> Self;
    fn max_value() -> Self;
    fn to_f64(self) -> f64;
    fn from_f64(x: f64) -> Self;
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
    fn from_u128(x: u128) -> Self {
        x as i64
    }
    fn abs_diff(self, b: Self) -> Self {
        i64::abs_diff(self, b) as i64
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

    fn from_u128(x: u128) -> Self {
        x
    }

    fn abs_diff(self, b: Self) -> Self {
        u128::abs_diff(self, b)
    }
}
