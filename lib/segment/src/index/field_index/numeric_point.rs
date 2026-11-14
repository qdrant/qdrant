use std::fmt::Debug;

use common::types::PointOffsetType;
use num_traits::Num;
use ordered_float::OrderedFloat;
use serde::Serialize;

pub use self::point::Point;
use crate::types::{FloatPayloadType, Range};

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

    fn from_f64_range(range: Range<OrderedFloat<FloatPayloadType>>) -> Range<Self> {
        integer_range_from_f64(range)
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

    fn from_f64_range(range: Range<OrderedFloat<FloatPayloadType>>) -> Range<Self> {
        integer_range_from_f64(range)
    }
}

/// A fractional bound mapped onto an integer type.
enum IntegerBound<T> {
    /// Every value of `T` satisfies the bound.
    Unbounded,
    /// No value of `T` satisfies the bound.
    Empty,
    Value(T),
}

/// Convert a fractional range into an integer range with the same meaning.
///
/// Each bound is rounded away from the matching set, ceil for `lt`/`gte` and
/// floor for `gt`/`lte` (#9049). A bound past the range of `T` is not
/// saturated, which would move the predicate: `lt: 1e20` admits every `i64`
/// and `gte: 1e20` admits none (#11379). `as` saturates, so the bound is
/// compared against `T`'s limits first.
fn integer_range_from_f64<T: Numericable>(
    range: Range<OrderedFloat<FloatPayloadType>>,
) -> Range<T> {
    let min = T::min_value().to_f64();
    let max = T::max_value().to_f64();

    // `x < f`: true for every x once f reaches max (f64 rounds max up), false for every x
    // once f is at or below min.
    let lt = |f: f64| match f {
        f if f >= max => IntegerBound::Unbounded,
        f if f <= min => IntegerBound::Empty,
        f => IntegerBound::Value(T::from_f64(f.ceil())),
    };
    // `x > f`: false for every x once f reaches max, true for every x below min.
    let gt = |f: f64| match f {
        f if f >= max => IntegerBound::Empty,
        f if f < min => IntegerBound::Unbounded,
        f => IntegerBound::Value(T::from_f64(f.floor())),
    };
    // `x >= f`: false for every x once f reaches max, true for every x at or below min.
    let gte = |f: f64| match f {
        f if f >= max => IntegerBound::Empty,
        f if f <= min => IntegerBound::Unbounded,
        f => IntegerBound::Value(T::from_f64(f.ceil())),
    };
    // `x <= f`: true for every x once f reaches max, false for every x below min.
    let lte = |f: f64| match f {
        f if f >= max => IntegerBound::Unbounded,
        f if f < min => IntegerBound::Empty,
        f => IntegerBound::Value(T::from_f64(f.floor())),
    };

    let bounds = [
        range.lt.map(|f| lt(f.0)),
        range.gt.map(|f| gt(f.0)),
        range.gte.map(|f| gte(f.0)),
        range.lte.map(|f| lte(f.0)),
    ];
    if bounds
        .iter()
        .any(|bound| matches!(bound, Some(IntegerBound::Empty)))
    {
        // Nothing can match: `x < min` keeps the range empty whatever else it holds.
        return Range {
            lt: Some(T::min_value()),
            gt: None,
            gte: None,
            lte: None,
        };
    }
    let value = |bound: Option<IntegerBound<T>>| match bound {
        Some(IntegerBound::Value(value)) => Some(value),
        Some(IntegerBound::Unbounded) | Some(IntegerBound::Empty) | None => None,
    };
    let [lt, gt, gte, lte] = bounds;
    Range {
        lt: value(lt),
        gt: value(gt),
        gte: value(gte),
        lte: value(lte),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn float_range(
        lt: Option<f64>,
        gt: Option<f64>,
        gte: Option<f64>,
        lte: Option<f64>,
    ) -> Range<OrderedFloat<FloatPayloadType>> {
        Range { lt, gt, gte, lte }.map(OrderedFloat::from)
    }

    fn matches_i64(range: Range<OrderedFloat<FloatPayloadType>>, value: i64) -> bool {
        i64::from_f64_range(range).check_range(value)
    }

    #[test]
    fn i64_range_bounds_past_the_type_are_not_saturated() {
        let values = [i64::MIN, 1, i64::MAX];

        // Every value is above -1e20 and below 1e20.
        for value in values {
            assert!(matches_i64(
                float_range(None, Some(-1e20), None, None),
                value
            ));
            assert!(matches_i64(
                float_range(Some(1e20), None, None, None),
                value
            ));
            assert!(matches_i64(
                float_range(None, None, Some(-1e20), None),
                value
            ));
            assert!(matches_i64(
                float_range(None, None, None, Some(1e20)),
                value
            ));
        }
        // No value reaches 1e20 or goes down to -1e20.
        for value in values {
            assert!(!matches_i64(
                float_range(None, None, Some(1e20), None),
                value
            ));
            assert!(!matches_i64(
                float_range(None, None, None, Some(-1e20)),
                value
            ));
            assert!(!matches_i64(
                float_range(None, Some(1e20), None, None),
                value
            ));
            assert!(!matches_i64(
                float_range(Some(-1e20), None, None, None),
                value
            ));
        }
        // An empty bound empties the whole range, whatever the other bounds say.
        assert!(!matches_i64(
            float_range(Some(1e20), Some(-1e20), Some(1e20), None),
            1
        ));
    }

    #[test]
    fn i64_range_bounds_at_the_type_limits_keep_their_meaning() {
        let min = i64::MIN as f64;
        // `> MIN` excludes MIN, `>= MIN` and `<= MIN` include it, `< MIN` matches nothing.
        assert!(!matches_i64(
            float_range(None, Some(min), None, None),
            i64::MIN
        ));
        assert!(matches_i64(
            float_range(None, Some(min), None, None),
            i64::MIN + 1
        ));
        assert!(matches_i64(
            float_range(None, None, Some(min), None),
            i64::MIN
        ));
        assert!(matches_i64(
            float_range(None, None, None, Some(min)),
            i64::MIN
        ));
        assert!(!matches_i64(
            float_range(Some(min), None, None, None),
            i64::MIN
        ));
        // `i64::MAX as f64` rounds up to 2^63, which no i64 reaches: `< 2^63` and
        // `<= 2^63` admit MAX, `> 2^63` and `>= 2^63` admit nothing.
        let max = i64::MAX as f64;
        assert!(matches_i64(
            float_range(Some(max), None, None, None),
            i64::MAX
        ));
        assert!(matches_i64(
            float_range(None, None, None, Some(max)),
            i64::MAX
        ));
        assert!(!matches_i64(
            float_range(None, Some(max), None, None),
            i64::MAX
        ));
        assert!(!matches_i64(
            float_range(None, None, Some(max), None),
            i64::MAX
        ));
    }

    #[test]
    fn i64_fractional_bounds_still_round_away_from_the_matching_set() {
        // #9049: `gte: 1.5` must not admit 1, `lt: 1.5` must admit 1 and not 2.
        assert_eq!(
            i64::from_f64_range(float_range(Some(1.5), Some(1.5), Some(1.5), Some(1.5))),
            Range {
                lt: Some(2),
                gt: Some(1),
                gte: Some(2),
                lte: Some(1),
            }
        );
    }

    #[test]
    fn u128_negative_bounds_are_not_saturated_to_zero() {
        // `> -1.0` admits 0; saturating `-1.0 as u128` to 0 would have excluded it.
        assert!(u128::from_f64_range(float_range(None, Some(-1.0), None, None)).check_range(0));
        // `<= -1.0` admits nothing.
        assert!(!u128::from_f64_range(float_range(None, None, None, Some(-1.0))).check_range(0));
    }
}
