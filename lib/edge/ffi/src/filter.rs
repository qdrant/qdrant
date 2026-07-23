use std::num::NonZeroU32;

use ahash::AHashSet;
use segment::types::{
    AnyVariants, Condition as SegmentCondition, DateTimePayloadType,
    FieldCondition as SegmentFieldCondition, Filter as SegmentFilter,
    GeoBoundingBox as SegmentGeoBoundingBox, GeoLineString as SegmentGeoLineString,
    GeoPoint as SegmentGeoPoint, GeoPolygon as SegmentGeoPolygon, GeoRadius as SegmentGeoRadius,
    HasIdCondition, HasVectorCondition, IsEmptyCondition, IsNullCondition, Match as SegmentMatch,
    MatchAny, MatchExcept, MatchPhrase, MatchPrefix, MatchText, MatchTextAny, MatchValue,
    MinShould as SegmentMinShould, Nested as SegmentNested, NestedCondition, PayloadField,
    PointIdType, Range, RangeInterface, Slice as SegmentSlice, SliceCondition,
    ValueVariants as SegmentValueVariants, ValuesCount as SegmentValuesCount,
};
use segment::utils::maybe_arc::MaybeArc;

use crate::types::PointId;

// ── GeoPoint ────────────────────────────────────────────────────────────────

/// A latitude/longitude pair in decimal degrees.
///
/// Latitude must be in `[-90, 90]` and longitude in `[-180, 180]`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoPoint {
    /// Longitude in decimal degrees.
    pub lon: f64,
    /// Latitude in decimal degrees.
    pub lat: f64,
}

impl TryFrom<GeoPoint> for SegmentGeoPoint {
    type Error = crate::error::EdgeError;

    fn try_from(p: GeoPoint) -> Result<Self, Self::Error> {
        let GeoPoint { lon, lat } = p;
        SegmentGeoPoint::new(lon, lat).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!(
                "invalid geo point (lon={lon}, lat={lat}): {e}"
            ))
        })
    }
}

impl From<SegmentGeoPoint> for GeoPoint {
    fn from(p: SegmentGeoPoint) -> Self {
        let SegmentGeoPoint { lon, lat } = p;
        GeoPoint {
            lon: lon.into_inner(),
            lat: lat.into_inner(),
        }
    }
}

// ── GeoBoundingBox ──────────────────────────────────────────────────────────

/// An axis-aligned geographic bounding box, used by geo filters.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoBoundingBox {
    /// Top-left corner (north-west).
    pub top_left: GeoPoint,
    /// Bottom-right corner (south-east).
    pub bottom_right: GeoPoint,
}

impl TryFrom<GeoBoundingBox> for SegmentGeoBoundingBox {
    type Error = crate::error::EdgeError;

    fn try_from(b: GeoBoundingBox) -> Result<Self, Self::Error> {
        let GeoBoundingBox {
            top_left,
            bottom_right,
        } = b;
        Ok(SegmentGeoBoundingBox {
            top_left: SegmentGeoPoint::try_from(top_left)?,
            bottom_right: SegmentGeoPoint::try_from(bottom_right)?,
        })
    }
}

// ── GeoRadius ───────────────────────────────────────────────────────────────

/// A circular geographic region defined by a center and a radius.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoRadius {
    /// Center of the circle.
    pub center: GeoPoint,
    /// Radius in meters.
    pub radius: f64,
}

impl TryFrom<GeoRadius> for SegmentGeoRadius {
    type Error = crate::error::EdgeError;

    fn try_from(r: GeoRadius) -> Result<Self, Self::Error> {
        let GeoRadius { center, radius } = r;
        // The radius reaches the geo index unvalidated otherwise: a negative or
        // non-finite value yields nonsensical distance comparisons (and feeds
        // NaN into the geohash math). Reject it at the boundary.
        if !radius.is_finite() || radius < 0.0 {
            return Err(crate::error::EdgeError::invalid_argument(format!(
                "invalid geo radius: must be a finite, non-negative number of meters, got {radius}"
            )));
        }
        Ok(SegmentGeoRadius {
            center: SegmentGeoPoint::try_from(center)?,
            radius: ordered_float::OrderedFloat(radius),
        })
    }
}

// ── GeoLineString ────────────────────────────────────────────────────────────

/// A closed ring of geographic points. The first and last point must be equal
/// and a ring needs at least 4 points; this is validated at the FFI boundary
/// (and again by the engine), so a malformed ring is rejected with
/// `InvalidArgument`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoLineString {
    pub points: Vec<GeoPoint>,
}

impl TryFrom<GeoLineString> for SegmentGeoLineString {
    type Error = crate::error::EdgeError;

    fn try_from(ls: GeoLineString) -> Result<Self, Self::Error> {
        let GeoLineString { points } = ls;
        let points = points
            .into_iter()
            .map(SegmentGeoPoint::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        // Mirror the engine's `GeoPolygon::validate_line_string`, which only runs
        // on the serde-deserialization path. Building the segment type directly
        // here would otherwise let a malformed ring (too few points or unclosed)
        // through to the geo index, where it can panic on indexed payloads.
        if points.len() <= 3 {
            return Err(crate::error::EdgeError::invalid_argument(format!(
                "invalid geo ring: a closed ring needs at least 4 points, got {}",
                points.len()
            )));
        }
        // `points` is non-empty here (len > 3), so first/last always exist.
        let (first, last) = (&points[0], &points[points.len() - 1]);
        if (first.lat - last.lat).abs() > f64::EPSILON
            || (first.lon - last.lon).abs() > f64::EPSILON
        {
            return Err(crate::error::EdgeError::invalid_argument(
                "invalid geo ring: the first and last points must be equal to close the ring",
            ));
        }

        Ok(SegmentGeoLineString { points })
    }
}

// ── GeoPolygon ───────────────────────────────────────────────────────────────

/// A polygon geo filter: points inside `exterior` and outside every `interior`
/// hole match.
///
/// Each line string must form a closed ring (first == last point) with at
/// least 4 points; this is validated at the FFI boundary (and again by the
/// engine), so a malformed ring is rejected with `InvalidArgument`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoPolygon {
    /// The exterior ring that bounds the surface.
    pub exterior: GeoLineString,
    /// Optional interior rings (holes). Points inside a hole are excluded.
    #[uniffi(default = None)]
    pub interiors: Option<Vec<GeoLineString>>,
}

impl TryFrom<GeoPolygon> for SegmentGeoPolygon {
    type Error = crate::error::EdgeError;

    fn try_from(p: GeoPolygon) -> Result<Self, Self::Error> {
        let GeoPolygon {
            exterior,
            interiors,
        } = p;
        let exterior = SegmentGeoLineString::try_from(exterior)?;
        let interiors = interiors
            .map(|rings| {
                rings
                    .into_iter()
                    .map(SegmentGeoLineString::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(SegmentGeoPolygon {
            exterior,
            interiors,
        })
    }
}

// ── RangeFloat ──────────────────────────────────────────────────────────────

/// A numeric range filter with optional inclusive/exclusive bounds.
///
/// Any combination of bounds can be set; unset bounds are treated as
/// unbounded. `gte` (≥) and `gt` (>) should not both be set simultaneously;
/// likewise for `lte` and `lt`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RangeFloat {
    /// Inclusive lower bound (≥).
    #[uniffi(default = None)]
    pub gte: Option<f64>,
    /// Exclusive lower bound (>).
    #[uniffi(default = None)]
    pub gt: Option<f64>,
    /// Inclusive upper bound (≤).
    #[uniffi(default = None)]
    pub lte: Option<f64>,
    /// Exclusive upper bound (<).
    #[uniffi(default = None)]
    pub lt: Option<f64>,
}

// ── RangeDatetime ───────────────────────────────────────────────────────────

/// A datetime range filter with optional inclusive/exclusive bounds, each an
/// RFC 3339 timestamp string (e.g. `"2024-01-01T00:00:00Z"`).
///
/// Requires a datetime index on the payload key. Unset bounds are treated as
/// unbounded.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RangeDatetime {
    /// Inclusive lower bound (≥).
    #[uniffi(default = None)]
    pub gte: Option<String>,
    /// Exclusive lower bound (>).
    #[uniffi(default = None)]
    pub gt: Option<String>,
    /// Inclusive upper bound (≤).
    #[uniffi(default = None)]
    pub lte: Option<String>,
    /// Exclusive upper bound (<).
    #[uniffi(default = None)]
    pub lt: Option<String>,
}

/// Parse one datetime range bound, naming the bound in the error.
fn parse_datetime_bound(
    bound: &str,
    value: Option<String>,
) -> Result<Option<DateTimePayloadType>, crate::error::EdgeError> {
    value
        .map(|v| {
            v.parse::<DateTimePayloadType>().map_err(|e| {
                crate::error::EdgeError::invalid_argument(format!(
                    "invalid datetime range bound {bound} ({v:?}): {e}"
                ))
            })
        })
        .transpose()
}

impl TryFrom<RangeDatetime> for Range<DateTimePayloadType> {
    type Error = crate::error::EdgeError;

    fn try_from(r: RangeDatetime) -> Result<Self, Self::Error> {
        let RangeDatetime { gte, gt, lte, lt } = r;
        Ok(Range {
            gte: parse_datetime_bound("gte", gte)?,
            gt: parse_datetime_bound("gt", gt)?,
            lte: parse_datetime_bound("lte", lte)?,
            lt: parse_datetime_bound("lt", lt)?,
        })
    }
}

// ── MatchValue ──────────────────────────────────────────────────────────────

/// A scalar payload value used by [`Match::Value`].
#[derive(Clone, Debug, uniffi::Enum)]
pub enum ValueVariants {
    /// A string value.
    String { value: String },
    /// A signed 64-bit integer.
    Integer { value: i64 },
    /// A boolean value.
    Bool { value: bool },
}

impl From<ValueVariants> for SegmentValueVariants {
    fn from(v: ValueVariants) -> Self {
        match v {
            ValueVariants::String { value } => SegmentValueVariants::String(value),
            ValueVariants::Integer { value } => SegmentValueVariants::Integer(value),
            ValueVariants::Bool { value } => SegmentValueVariants::Bool(value),
        }
    }
}

// ── Match ───────────────────────────────────────────────────────────────────

/// How a [`FieldCondition`] compares a payload field.
///
/// For each variant, exactly one of its alternative payloads should be set
/// (e.g. for `Any`, pass either `strings` or `integers`, not both).
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Match {
    /// Exact scalar match.
    Value { value: ValueVariants },
    /// Full-text match: every word of `text` must occur in the field
    /// (requires a full-text index on the payload key).
    Text { text: String },
    /// Full-text match: at least one word of `text_any` must occur in the
    /// field (requires a full-text index on the payload key).
    TextAny { text_any: String },
    /// Full-text phrase match: the words must occur adjacently and in order
    /// (requires a full-text index with phrase matching enabled).
    Phrase { phrase: String },
    /// Prefix match: some word of the field must start with `prefix`
    /// (requires a full-text index on the payload key).
    Prefix { prefix: String },
    /// The field value must equal any of the given strings or integers.
    Any {
        strings: Option<Vec<String>>,
        integers: Option<Vec<i64>>,
    },
    /// The field value must equal none of the given strings or integers.
    Except {
        strings: Option<Vec<String>>,
        integers: Option<Vec<i64>>,
    },
}

impl TryFrom<Match> for SegmentMatch {
    type Error = crate::error::EdgeError;

    fn try_from(m: Match) -> Result<Self, Self::Error> {
        match m {
            Match::Value { value } => Ok(SegmentMatch::Value(MatchValue {
                value: SegmentValueVariants::from(value),
            })),
            Match::Text { text } => Ok(SegmentMatch::Text(MatchText { text })),
            Match::TextAny { text_any } => Ok(SegmentMatch::TextAny(MatchTextAny { text_any })),
            Match::Phrase { phrase } => Ok(SegmentMatch::Phrase(MatchPhrase { phrase })),
            Match::Prefix { prefix } => Ok(SegmentMatch::Prefix(MatchPrefix { prefix })),
            Match::Any { strings, integers } => {
                let any = match (strings, integers) {
                    (None, None) => {
                        return Err(crate::error::EdgeError::invalid_argument(
                            "Match::Any requires either `strings` or `integers`",
                        ));
                    }
                    (Some(_), Some(_)) => {
                        return Err(crate::error::EdgeError::invalid_argument(
                            "Match::Any: set either `strings` or `integers`, not both",
                        ));
                    }
                    (Some(strings), None) => AnyVariants::Strings(strings.into_iter().collect()),
                    (None, Some(integers)) => AnyVariants::Integers(integers.into_iter().collect()),
                };
                Ok(SegmentMatch::Any(MatchAny { any }))
            }
            Match::Except { strings, integers } => {
                let except = match (strings, integers) {
                    (None, None) => {
                        return Err(crate::error::EdgeError::invalid_argument(
                            "Match::Except requires either `strings` or `integers`",
                        ));
                    }
                    (Some(_), Some(_)) => {
                        return Err(crate::error::EdgeError::invalid_argument(
                            "Match::Except: set either `strings` or `integers`, not both",
                        ));
                    }
                    (Some(strings), None) => AnyVariants::Strings(strings.into_iter().collect()),
                    (None, Some(integers)) => AnyVariants::Integers(integers.into_iter().collect()),
                };
                Ok(SegmentMatch::Except(MatchExcept { except }))
            }
        }
    }
}

// ── ValuesCount ─────────────────────────────────────────────────────────────

/// Matches points where the field value count falls in the given range.
///
/// Useful for filtering by the cardinality of an array-valued payload
/// field — e.g. "points with at least 3 tags".
#[derive(Clone, Debug, uniffi::Record)]
pub struct ValuesCount {
    /// Inclusive lower bound (≥).
    #[uniffi(default = None)]
    pub gte: Option<u64>,
    /// Exclusive lower bound (>).
    #[uniffi(default = None)]
    pub gt: Option<u64>,
    /// Inclusive upper bound (≤).
    #[uniffi(default = None)]
    pub lte: Option<u64>,
    /// Exclusive upper bound (<).
    #[uniffi(default = None)]
    pub lt: Option<u64>,
}

impl From<ValuesCount> for SegmentValuesCount {
    fn from(v: ValuesCount) -> Self {
        let ValuesCount { gte, gt, lte, lt } = v;
        SegmentValuesCount {
            lt: lt.map(crate::error::clamp_usize),
            gt: gt.map(crate::error::clamp_usize),
            gte: gte.map(crate::error::clamp_usize),
            lte: lte.map(crate::error::clamp_usize),
        }
    }
}

// ── FieldCondition ──────────────────────────────────────────────────────────

/// A filter condition applied to a single payload field.
///
/// Exactly one predicate must be set — `match`, `range`/`datetime_range`,
/// `geo_bounding_box`, `geo_radius`, `geo_polygon`, or `values_count`. A
/// condition with none set is rejected (it would silently match every point),
/// and so is one with more than one: the engine has no well-defined semantics
/// for multiple predicates in a single field condition — it evaluates only one,
/// and *which* one depends on the field's indexes — so results would silently
/// diverge from a Qdrant server. To AND several predicates on the same key, add
/// one `FieldCondition` per predicate to the filter's `must` clause. The payload
/// `key` uses JSON-path syntax for nested fields (e.g. `"meta.location"`).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FieldCondition {
    /// Payload key to test (JSON-path syntax supported).
    pub key: String,
    /// Scalar / text / list match.
    #[uniffi(default = None)]
    pub r#match: Option<Match>,
    /// Numeric range comparison. Mutually exclusive with `datetime_range`.
    #[uniffi(default = None)]
    pub range: Option<RangeFloat>,
    /// Datetime range comparison (RFC 3339 bounds). Mutually exclusive with
    /// `range`; requires a datetime index on the key.
    #[uniffi(default = None)]
    pub datetime_range: Option<RangeDatetime>,
    /// Geographic bounding-box containment.
    #[uniffi(default = None)]
    pub geo_bounding_box: Option<GeoBoundingBox>,
    /// Geographic radius containment.
    #[uniffi(default = None)]
    pub geo_radius: Option<GeoRadius>,
    /// Geographic polygon containment.
    #[uniffi(default = None)]
    pub geo_polygon: Option<GeoPolygon>,
    /// Cardinality filter over array-valued payloads.
    #[uniffi(default = None)]
    pub values_count: Option<ValuesCount>,
}

impl TryFrom<FieldCondition> for SegmentFieldCondition {
    type Error = crate::error::EdgeError;

    fn try_from(c: FieldCondition) -> Result<Self, Self::Error> {
        let FieldCondition {
            key,
            r#match,
            range,
            datetime_range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count,
        } = c;
        let key = crate::error::parse_json_path(&key)?;

        // Exactly one predicate per field condition. The engine has no
        // well-defined semantics for multiple predicates in one condition — it
        // evaluates only one, and which one depends on the field's indexes — so
        // passing several through would silently diverge from a Qdrant server.
        // None set matches every point (a silent no-op); >1 is ambiguous. Reject
        // both; callers AND predicates via separate `must` conditions. (`range`
        // and `datetime_range` share the engine's single range slot, so they
        // count as one predicate here; setting both is caught below.)
        let predicate_count = [
            r#match.is_some(),
            range.is_some() || datetime_range.is_some(),
            geo_bounding_box.is_some(),
            geo_radius.is_some(),
            geo_polygon.is_some(),
            values_count.is_some(),
        ]
        .into_iter()
        .filter(|&set| set)
        .count();
        if predicate_count == 0 {
            return Err(crate::error::EdgeError::invalid_argument(
                "field condition has no predicate set: specify exactly one of \
                 match, range, datetime_range, geo_bounding_box, geo_radius, geo_polygon, \
                 or values_count",
            ));
        }
        if predicate_count > 1 {
            return Err(crate::error::EdgeError::invalid_argument(
                "field condition has more than one predicate set: a field condition tests \
                 exactly one predicate. To AND several predicates on the same key, add one \
                 field condition per predicate to the filter's `must` clause",
            ));
        }

        // The engine's `range` slot holds one interface, float or datetime.
        let range = match (range, datetime_range) {
            (Some(_), Some(_)) => {
                return Err(crate::error::EdgeError::invalid_argument(
                    "field condition: set either `range` or `datetime_range`, not both",
                ));
            }
            (Some(RangeFloat { gte, gt, lte, lt }), None) => Some(RangeInterface::Float(Range {
                gte: gte.map(ordered_float::OrderedFloat),
                gt: gt.map(ordered_float::OrderedFloat),
                lte: lte.map(ordered_float::OrderedFloat),
                lt: lt.map(ordered_float::OrderedFloat),
            })),
            (None, Some(datetime_range)) => {
                Some(RangeInterface::DateTime(Range::try_from(datetime_range)?))
            }
            (None, None) => None,
        };

        Ok(SegmentFieldCondition {
            key,
            r#match: r#match.map(SegmentMatch::try_from).transpose()?,
            range,
            geo_bounding_box: geo_bounding_box
                .map(SegmentGeoBoundingBox::try_from)
                .transpose()?,
            geo_radius: geo_radius.map(SegmentGeoRadius::try_from).transpose()?,
            geo_polygon: geo_polygon.map(SegmentGeoPolygon::try_from).transpose()?,
            values_count: values_count.map(SegmentValuesCount::from),
            is_empty: None,
            is_null: None,
        })
    }
}

// ── Condition ───────────────────────────────────────────────────────────────

/// A single filter clause, composed into larger filters by [`Filter`].
// `FieldCondition` is the largest variant, but boxing variants is not supported
// by `uniffi::Enum`, so we accept the size difference.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Condition {
    /// Match against a specific payload field.
    Field { condition: FieldCondition },
    /// The given payload key must be absent or empty.
    IsEmpty { key: String },
    /// The given payload key must hold the JSON `null` value.
    IsNull { key: String },
    /// Point ID must match any of the listed IDs.
    HasId { ids: Vec<PointId> },
    /// The point must have a vector in the named field.
    HasVector { vector_name: String },
    /// The point ID must fall into slice `index` of the ID space split into
    /// `total` deterministic, disjoint slices. Slices with different `total`
    /// values are correlated: slice `0` of `total: 4` is a strict subset of
    /// slice `0` of `total: 2`, so a smaller sample is contained in a larger
    /// one. `index` must be in `0..total`.
    Slice { total: u32, index: u32 },
    /// Apply `filter` to the objects of the nested-object array stored under
    /// payload `key`: a point matches if at least one array element satisfies
    /// the whole filter. Inside, keys are relative to `key` and ID-based
    /// conditions are not allowed.
    Nested { key: String, filter: Filter },
    /// Nested filter — useful for grouping `should` clauses inside a
    /// `must` / `must_not`.
    Filter { filter: Filter },
}

impl TryFrom<Condition> for SegmentCondition {
    type Error = crate::error::EdgeError;

    fn try_from(c: Condition) -> Result<Self, Self::Error> {
        condition_to_segment(c, 0)
    }
}

/// Convert a `Condition`, tracking nesting `depth` so a self-recursive
/// `Condition::Filter` chain cannot overflow the stack (see
/// [`crate::error::check_nesting_depth`]).
fn condition_to_segment(
    c: Condition,
    depth: u32,
) -> Result<SegmentCondition, crate::error::EdgeError> {
    match c {
        Condition::Field { condition } => Ok(SegmentCondition::Field(
            SegmentFieldCondition::try_from(condition)?,
        )),
        Condition::IsEmpty { key } => {
            let parsed_key = crate::error::parse_json_path(&key)?;
            Ok(SegmentCondition::IsEmpty(IsEmptyCondition {
                is_empty: PayloadField { key: parsed_key },
            }))
        }
        Condition::IsNull { key } => {
            let parsed_key = crate::error::parse_json_path(&key)?;
            Ok(SegmentCondition::IsNull(IsNullCondition {
                is_null: PayloadField { key: parsed_key },
            }))
        }
        Condition::HasId { ids } => {
            let id_set: Result<AHashSet<PointIdType>, crate::error::EdgeError> =
                ids.into_iter().map(PointIdType::try_from).collect();
            Ok(SegmentCondition::HasId(HasIdCondition {
                has_id: MaybeArc::NoArc(id_set?),
            }))
        }
        Condition::Slice { total, index } => {
            // Mirror the engine's `validate_slice_condition`, which only runs
            // on the serde path: `total` must be non-zero and `index < total`.
            let total = NonZeroU32::new(total).ok_or_else(|| {
                crate::error::EdgeError::invalid_argument("slice condition: total must be non-zero")
            })?;
            if index >= total.get() {
                return Err(crate::error::EdgeError::invalid_argument(format!(
                    "slice condition: index ({index}) must be less than total ({total})"
                )));
            }
            Ok(SegmentCondition::Slice(SliceCondition {
                slice: SegmentSlice { total, index },
            }))
        }
        Condition::Nested { key, filter } => Ok(SegmentCondition::Nested(NestedCondition::new(
            SegmentNested {
                key: crate::error::parse_json_path(&key)?,
                filter: filter_to_segment(filter, depth + 1)?,
            },
        ))),
        Condition::HasVector { vector_name } => {
            Ok(SegmentCondition::HasVector(HasVectorCondition {
                has_vector: vector_name,
            }))
        }
        Condition::Filter { filter } => Ok(SegmentCondition::Filter(filter_to_segment(
            filter,
            depth + 1,
        )?)),
    }
}

// ── Filter ──────────────────────────────────────────────────────────────────

/// A boolean combination of [`Condition`]s.
///
/// Semantics:
/// - `must`: all clauses must match (logical AND).
/// - `should`: at least one clause must match (logical OR).
/// - `must_not`: no clause may match (logical NOT AND).
///
/// All three fields are optional; an empty filter matches every point.
///
/// ## Example
///
/// ```swift
/// let filter = Filter(
///     must: [.field(condition:
///         FieldCondition(key: "category",
///                        match: .value(value: .string(value: "news"))))]
/// )
/// ```
///
/// ```kotlin
/// val filter = Filter(
///     must = listOf(Condition.Field(FieldCondition(
///         key = "category",
///         `match` = Match.Value(ValueVariants.String("news")),
///     ))),
/// )
/// ```
#[derive(Clone, Debug, uniffi::Record)]
pub struct Filter {
    /// Clauses that must all match.
    #[uniffi(default = None)]
    pub must: Option<Vec<Condition>>,
    /// Clauses of which at least one must match.
    #[uniffi(default = None)]
    pub should: Option<Vec<Condition>>,
    /// Clauses that must all fail to match.
    #[uniffi(default = None)]
    pub must_not: Option<Vec<Condition>>,
    /// At least `min_count` of these clauses must match.
    #[uniffi(default = None)]
    pub min_should: Option<MinShould>,
}

/// The `min_should` clause of a [`Filter`]: at least `min_count` of
/// `conditions` must match.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MinShould {
    /// Candidate clauses.
    pub conditions: Vec<Condition>,
    /// How many of `conditions` must match, at minimum.
    pub min_count: u64,
}

impl TryFrom<Filter> for SegmentFilter {
    type Error = crate::error::EdgeError;

    fn try_from(f: Filter) -> Result<Self, Self::Error> {
        filter_to_segment(f, 0)
    }
}

/// Convert a `Filter` at nesting `depth`, rejecting trees deeper than
/// [`MAX_QUERY_NESTING_DEPTH`](crate::error::MAX_QUERY_NESTING_DEPTH) before the
/// recursion can overflow the stack. Each clause is converted at the same depth;
/// a nested `Condition::Filter` bumps it by one (see [`condition_to_segment`]).
fn filter_to_segment(f: Filter, depth: u32) -> Result<SegmentFilter, crate::error::EdgeError> {
    crate::error::check_nesting_depth("filter", depth)?;
    let Filter {
        must,
        should,
        must_not,
        min_should,
    } = f;
    let convert = |v: Vec<Condition>| {
        v.into_iter()
            .map(|c| condition_to_segment(c, depth))
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(SegmentFilter {
        must: must.map(&convert).transpose()?,
        should: should.map(&convert).transpose()?,
        must_not: must_not.map(&convert).transpose()?,
        min_should: min_should
            .map(
                |MinShould {
                     conditions,
                     min_count,
                 }| {
                    Ok::<_, crate::error::EdgeError>(SegmentMinShould {
                        conditions: convert(conditions)?,
                        min_count: crate::error::clamp_usize(min_count),
                    })
                },
            )
            .transpose()?,
    })
}

// ── Coverage map ────────────────────────────────────────────────────────────

/// Compile-time map of the engine's filter-condition tree onto the FFI
/// [`Filter`] / [`Condition`] / [`Match`] surface above.
///
/// Same contract as the maps in [`crate::update`] and [`crate::ops::query`]:
/// an exhaustive match with no wildcard arms, so a variant added to the
/// engine's [`Condition`](SegmentCondition), [`Match`](SegmentMatch),
/// [`RangeInterface`], [`AnyVariants`], or
/// [`ValueVariants`](SegmentValueVariants) stops this function from
/// compiling, forcing an explicit decision about the FFI surface.
///
/// Never called; it exists only for the exhaustiveness check.
#[allow(dead_code)]
fn assert_every_filter_condition_is_mapped(c: SegmentCondition) {
    match c {
        SegmentCondition::Field(field) => {
            let SegmentFieldCondition {
                // [`FieldCondition::key`]
                key: _,
                // [`FieldCondition::match`]
                r#match,
                // [`FieldCondition::range`] / [`FieldCondition::datetime_range`]
                range,
                // [`FieldCondition::geo_bounding_box`]
                geo_bounding_box: _,
                // [`FieldCondition::geo_radius`]
                geo_radius: _,
                // [`FieldCondition::geo_polygon`]
                geo_polygon: _,
                // [`FieldCondition::values_count`]
                values_count: _,
                // Not exposed as fields: the dedicated [`Condition::IsEmpty`] /
                // [`Condition::IsNull`] variants express the same predicates.
                is_empty: _,
                is_null: _,
            } = field;
            if let Some(m) = r#match {
                match m {
                    // [`Match::Value`]
                    SegmentMatch::Value(MatchValue { value }) => match value {
                        // [`ValueVariants::String`]
                        SegmentValueVariants::String(_) => {}
                        // [`ValueVariants::Integer`]
                        SegmentValueVariants::Integer(_) => {}
                        // [`ValueVariants::Bool`]
                        SegmentValueVariants::Bool(_) => {}
                    },
                    // [`Match::Text`]
                    SegmentMatch::Text(_) => {}
                    // [`Match::TextAny`]
                    SegmentMatch::TextAny(_) => {}
                    // [`Match::Phrase`]
                    SegmentMatch::Phrase(_) => {}
                    // [`Match::Prefix`]
                    SegmentMatch::Prefix(_) => {}
                    // [`Match::Any`]
                    SegmentMatch::Any(MatchAny { any }) => match any {
                        // [`Match::Any`] `strings`
                        AnyVariants::Strings(_) => {}
                        // [`Match::Any`] `integers`
                        AnyVariants::Integers(_) => {}
                    },
                    // [`Match::Except`] (same `AnyVariants` split as `Any`)
                    SegmentMatch::Except(MatchExcept { except: _ }) => {}
                }
            }
            if let Some(range) = range {
                match range {
                    // [`FieldCondition::range`] ([`RangeFloat`])
                    RangeInterface::Float(_) => {}
                    // [`FieldCondition::datetime_range`] ([`RangeDatetime`])
                    RangeInterface::DateTime(_) => {}
                }
            }
        }
        // [`Condition::IsEmpty`]
        SegmentCondition::IsEmpty(_) => {}
        // [`Condition::IsNull`]
        SegmentCondition::IsNull(_) => {}
        // [`Condition::HasId`]
        SegmentCondition::HasId(_) => {}
        // [`Condition::HasVector`]
        SegmentCondition::HasVector(_) => {}
        // [`Condition::Slice`]
        SegmentCondition::Slice(_) => {}
        // [`Condition::Nested`]
        SegmentCondition::Nested(_) => {}
        // [`Condition::Filter`]; its `min_should` field is [`Filter::min_should`]
        SegmentCondition::Filter(_) => {}
        // Not exposed: runtime-internal resharding hook, `#[serde(skip)]`-ed
        // out of every serialized surface; never host-constructed.
        SegmentCondition::CustomIdChecker(_) => {}
    }
}
