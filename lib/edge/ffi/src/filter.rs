use ahash::AHashSet;
use segment::types::{
    AnyVariants, Condition as SegmentCondition, FieldCondition as SegmentFieldCondition,
    Filter as SegmentFilter, GeoPoint as SegmentGeoPoint,
    GeoBoundingBox as SegmentGeoBoundingBox, GeoRadius as SegmentGeoRadius,
    HasIdCondition, HasVectorCondition, IsEmptyCondition, IsNullCondition,
    Match as SegmentMatch, MatchAny, MatchExcept, MatchText, MatchValue,
    PayloadField, PointIdType, Range, RangeInterface,
    ValuesCount as SegmentValuesCount, ValueVariants as SegmentValueVariants,
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

impl From<GeoPoint> for SegmentGeoPoint {
    fn from(p: GeoPoint) -> Self {
        SegmentGeoPoint::new(p.lon, p.lat).expect("valid geo point")
    }
}

impl From<SegmentGeoPoint> for GeoPoint {
    fn from(p: SegmentGeoPoint) -> Self {
        GeoPoint {
            lon: p.lon.into_inner(),
            lat: p.lat.into_inner(),
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

impl From<GeoBoundingBox> for SegmentGeoBoundingBox {
    fn from(b: GeoBoundingBox) -> Self {
        SegmentGeoBoundingBox {
            top_left: SegmentGeoPoint::from(b.top_left),
            bottom_right: SegmentGeoPoint::from(b.bottom_right),
        }
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

impl From<GeoRadius> for SegmentGeoRadius {
    fn from(r: GeoRadius) -> Self {
        SegmentGeoRadius {
            center: SegmentGeoPoint::from(r.center),
            radius: ordered_float::OrderedFloat(r.radius),
        }
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
    pub gte: Option<f64>,
    /// Exclusive lower bound (>).
    pub gt: Option<f64>,
    /// Inclusive upper bound (≤).
    pub lte: Option<f64>,
    /// Exclusive upper bound (<).
    pub lt: Option<f64>,
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
    /// Full-text match on a string field (requires a full-text index on the
    /// payload key).
    Text { text: String },
    /// The field value must equal any of the given strings or integers.
    Any { strings: Option<Vec<String>>, integers: Option<Vec<i64>> },
    /// The field value must equal none of the given strings or integers.
    Except { strings: Option<Vec<String>>, integers: Option<Vec<i64>> },
}

impl From<Match> for SegmentMatch {
    fn from(m: Match) -> Self {
        match m {
            Match::Value { value } => SegmentMatch::Value(MatchValue {
                value: SegmentValueVariants::from(value),
            }),
            Match::Text { text } => SegmentMatch::Text(MatchText { text }),
            Match::Any { strings, integers } => {
                let any = if let Some(strings) = strings {
                    AnyVariants::Strings(strings.into_iter().collect())
                } else if let Some(integers) = integers {
                    AnyVariants::Integers(integers.into_iter().collect())
                } else {
                    AnyVariants::Strings(Default::default())
                };
                SegmentMatch::Any(MatchAny { any })
            }
            Match::Except { strings, integers } => {
                let except = if let Some(strings) = strings {
                    AnyVariants::Strings(strings.into_iter().collect())
                } else if let Some(integers) = integers {
                    AnyVariants::Integers(integers.into_iter().collect())
                } else {
                    AnyVariants::Strings(Default::default())
                };
                SegmentMatch::Except(MatchExcept { except })
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
    pub gte: Option<u64>,
    /// Exclusive lower bound (>).
    pub gt: Option<u64>,
    /// Inclusive upper bound (≤).
    pub lte: Option<u64>,
    /// Exclusive upper bound (<).
    pub lt: Option<u64>,
}

impl From<ValuesCount> for SegmentValuesCount {
    fn from(v: ValuesCount) -> Self {
        SegmentValuesCount {
            lt: v.lt.map(|x| x as usize),
            gt: v.gt.map(|x| x as usize),
            gte: v.gte.map(|x| x as usize),
            lte: v.lte.map(|x| x as usize),
        }
    }
}

// ── FieldCondition ──────────────────────────────────────────────────────────

/// A filter condition applied to a single payload field.
///
/// Exactly one of `match`, `range`, `geo_bounding_box`, `geo_radius`, or
/// `values_count` should be set; setting more than one is an error. The
/// payload `key` uses JSON-path syntax for nested fields (e.g.
/// `"meta.location"`).
#[derive(Clone, Debug, uniffi::Record)]
pub struct FieldCondition {
    /// Payload key to test (JSON-path syntax supported).
    pub key: String,
    /// Scalar / text / list match.
    pub r#match: Option<Match>,
    /// Numeric range comparison.
    pub range: Option<RangeFloat>,
    /// Geographic bounding-box containment.
    pub geo_bounding_box: Option<GeoBoundingBox>,
    /// Geographic radius containment.
    pub geo_radius: Option<GeoRadius>,
    /// Cardinality filter over array-valued payloads.
    pub values_count: Option<ValuesCount>,
}

impl From<FieldCondition> for SegmentFieldCondition {
    fn from(c: FieldCondition) -> Self {
        SegmentFieldCondition {
            key: c.key.parse().expect("valid json path"),
            r#match: c.r#match.map(SegmentMatch::from),
            range: c.range.map(|r| {
                RangeInterface::Float(Range {
                    gte: r.gte.map(ordered_float::OrderedFloat),
                    gt: r.gt.map(ordered_float::OrderedFloat),
                    lte: r.lte.map(ordered_float::OrderedFloat),
                    lt: r.lt.map(ordered_float::OrderedFloat),
                })
            }),
            geo_bounding_box: c.geo_bounding_box.map(SegmentGeoBoundingBox::from),
            geo_radius: c.geo_radius.map(SegmentGeoRadius::from),
            geo_polygon: None,
            values_count: c.values_count.map(SegmentValuesCount::from),
            is_empty: None,
            is_null: None,
        }
    }
}

// ── Condition ───────────────────────────────────────────────────────────────

/// A single filter clause, composed into larger filters by [`Filter`].
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
    /// Nested filter — useful for grouping `should` clauses inside a
    /// `must` / `must_not`.
    Filter { filter: Filter },
}

impl From<Condition> for SegmentCondition {
    fn from(c: Condition) -> Self {
        match c {
            Condition::Field { condition } => SegmentCondition::Field(SegmentFieldCondition::from(condition)),
            Condition::IsEmpty { key } => SegmentCondition::IsEmpty(IsEmptyCondition {
                is_empty: PayloadField {
                    key: key.parse().expect("valid json path"),
                },
            }),
            Condition::IsNull { key } => SegmentCondition::IsNull(IsNullCondition {
                is_null: PayloadField {
                    key: key.parse().expect("valid json path"),
                },
            }),
            Condition::HasId { ids } => {
                let id_set: AHashSet<PointIdType> =
                    ids.into_iter().map(PointIdType::from).collect();
                SegmentCondition::HasId(HasIdCondition {
                    has_id: MaybeArc::NoArc(id_set),
                })
            }
            Condition::HasVector { vector_name } => {
                SegmentCondition::HasVector(HasVectorCondition {
                    has_vector: vector_name,
                })
            }
            Condition::Filter { filter } => SegmentCondition::Filter(SegmentFilter::from(filter)),
        }
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
///                        match: .value(value: .string(value: "news")),
///                        range: nil, geoBoundingBox: nil,
///                        geoRadius: nil, valuesCount: nil))],
///     should: nil,
///     mustNot: nil
/// )
/// ```
///
/// ```kotlin
/// val filter = Filter(
///     must = listOf(Condition.Field(FieldCondition(
///         key = "category",
///         `match` = Match.Value(ValueVariants.String("news")),
///         range = null, geoBoundingBox = null,
///         geoRadius = null, valuesCount = null,
///     ))),
///     should = null,
///     mustNot = null,
/// )
/// ```
#[derive(Clone, Debug, uniffi::Record)]
pub struct Filter {
    /// Clauses that must all match.
    pub must: Option<Vec<Condition>>,
    /// Clauses of which at least one must match.
    pub should: Option<Vec<Condition>>,
    /// Clauses that must all fail to match.
    pub must_not: Option<Vec<Condition>>,
}

impl From<Filter> for SegmentFilter {
    fn from(f: Filter) -> Self {
        SegmentFilter {
            must: f.must.map(|v| v.into_iter().map(SegmentCondition::from).collect()),
            should: f
                .should
                .map(|v| v.into_iter().map(SegmentCondition::from).collect()),
            must_not: f
                .must_not
                .map(|v| v.into_iter().map(SegmentCondition::from).collect()),
            min_should: None,
        }
    }
}
