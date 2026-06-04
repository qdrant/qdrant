use ahash::AHashSet;
use segment::types::{
    AnyVariants, Condition as SegmentCondition, FieldCondition as SegmentFieldCondition,
    Filter as SegmentFilter, GeoPoint as SegmentGeoPoint,
    GeoBoundingBox as SegmentGeoBoundingBox, GeoLineString as SegmentGeoLineString,
    GeoPolygon as SegmentGeoPolygon, GeoRadius as SegmentGeoRadius,
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

impl TryFrom<GeoPoint> for SegmentGeoPoint {
    type Error = crate::error::EdgeError;

    fn try_from(p: GeoPoint) -> Result<Self, Self::Error> {
        SegmentGeoPoint::new(p.lon, p.lat).map_err(|e| {
            crate::error::EdgeError::invalid_argument(format!(
                "invalid geo point (lon={}, lat={}): {e}",
                p.lon, p.lat
            ))
        })
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

impl TryFrom<GeoBoundingBox> for SegmentGeoBoundingBox {
    type Error = crate::error::EdgeError;

    fn try_from(b: GeoBoundingBox) -> Result<Self, Self::Error> {
        Ok(SegmentGeoBoundingBox {
            top_left: SegmentGeoPoint::try_from(b.top_left)?,
            bottom_right: SegmentGeoPoint::try_from(b.bottom_right)?,
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
        Ok(SegmentGeoRadius {
            center: SegmentGeoPoint::try_from(r.center)?,
            radius: ordered_float::OrderedFloat(r.radius),
        })
    }
}

// ── GeoLineString ────────────────────────────────────────────────────────────

/// A closed ring of geographic points. The first and last point must be equal
/// and a ring needs at least 4 points (Qdrant validates this engine-side).
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoLineString {
    pub points: Vec<GeoPoint>,
}

impl TryFrom<GeoLineString> for SegmentGeoLineString {
    type Error = crate::error::EdgeError;

    fn try_from(ls: GeoLineString) -> Result<Self, Self::Error> {
        let points = ls
            .points
            .into_iter()
            .map(SegmentGeoPoint::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SegmentGeoLineString { points })
    }
}

// ── GeoPolygon ───────────────────────────────────────────────────────────────

/// A polygon geo filter: points inside `exterior` and outside every `interior`
/// hole match.
///
/// Each line string must form a closed ring (first == last point) with at
/// least 4 points; Qdrant enforces this at the engine level.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GeoPolygon {
    /// The exterior ring that bounds the surface.
    pub exterior: GeoLineString,
    /// Optional interior rings (holes). Points inside a hole are excluded.
    pub interiors: Option<Vec<GeoLineString>>,
}

impl TryFrom<GeoPolygon> for SegmentGeoPolygon {
    type Error = crate::error::EdgeError;

    fn try_from(p: GeoPolygon) -> Result<Self, Self::Error> {
        let exterior = SegmentGeoLineString::try_from(p.exterior)?;
        let interiors = p
            .interiors
            .map(|rings| {
                rings
                    .into_iter()
                    .map(SegmentGeoLineString::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(SegmentGeoPolygon { exterior, interiors })
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

impl TryFrom<Match> for SegmentMatch {
    type Error = crate::error::EdgeError;

    fn try_from(m: Match) -> Result<Self, Self::Error> {
        match m {
            Match::Value { value } => Ok(SegmentMatch::Value(MatchValue {
                value: SegmentValueVariants::from(value),
            })),
            Match::Text { text } => Ok(SegmentMatch::Text(MatchText { text })),
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
            lt: v.lt.map(crate::error::clamp_usize),
            gt: v.gt.map(crate::error::clamp_usize),
            gte: v.gte.map(crate::error::clamp_usize),
            lte: v.lte.map(crate::error::clamp_usize),
        }
    }
}

// ── FieldCondition ──────────────────────────────────────────────────────────

/// A filter condition applied to a single payload field.
///
/// Exactly one of `match`, `range`, `geo_bounding_box`, `geo_radius`,
/// `geo_polygon`, or `values_count` should be set; setting more than one is
/// an error. The payload `key` uses JSON-path syntax for nested fields (e.g.
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
    /// Geographic polygon containment.
    pub geo_polygon: Option<GeoPolygon>,
    /// Cardinality filter over array-valued payloads.
    pub values_count: Option<ValuesCount>,
}

impl TryFrom<FieldCondition> for SegmentFieldCondition {
    type Error = crate::error::EdgeError;

    fn try_from(c: FieldCondition) -> Result<Self, Self::Error> {
        let key = crate::error::parse_json_path(&c.key)?;
        let geo_bounding_box = c
            .geo_bounding_box
            .map(SegmentGeoBoundingBox::try_from)
            .transpose()?;
        let geo_radius = c
            .geo_radius
            .map(SegmentGeoRadius::try_from)
            .transpose()?;
        let geo_polygon = c
            .geo_polygon
            .map(SegmentGeoPolygon::try_from)
            .transpose()?;
        let r#match = c.r#match.map(SegmentMatch::try_from).transpose()?;
        Ok(SegmentFieldCondition {
            key,
            r#match,
            range: c.range.map(|r| {
                RangeInterface::Float(Range {
                    gte: r.gte.map(ordered_float::OrderedFloat),
                    gt: r.gt.map(ordered_float::OrderedFloat),
                    lte: r.lte.map(ordered_float::OrderedFloat),
                    lt: r.lt.map(ordered_float::OrderedFloat),
                })
            }),
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count: c.values_count.map(SegmentValuesCount::from),
            is_empty: None,
            is_null: None,
        })
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

impl TryFrom<Condition> for SegmentCondition {
    type Error = crate::error::EdgeError;

    fn try_from(c: Condition) -> Result<Self, Self::Error> {
        match c {
            Condition::Field { condition } => {
                Ok(SegmentCondition::Field(SegmentFieldCondition::try_from(condition)?))
            }
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
            Condition::HasVector { vector_name } => {
                Ok(SegmentCondition::HasVector(HasVectorCondition {
                    has_vector: vector_name,
                }))
            }
            Condition::Filter { filter } => {
                Ok(SegmentCondition::Filter(SegmentFilter::try_from(filter)?))
            }
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

impl TryFrom<Filter> for SegmentFilter {
    type Error = crate::error::EdgeError;

    fn try_from(f: Filter) -> Result<Self, Self::Error> {
        let must = f
            .must
            .map(|v| v.into_iter().map(SegmentCondition::try_from).collect::<Result<Vec<_>, _>>())
            .transpose()?;
        let should = f
            .should
            .map(|v| v.into_iter().map(SegmentCondition::try_from).collect::<Result<Vec<_>, _>>())
            .transpose()?;
        let must_not = f
            .must_not
            .map(|v| v.into_iter().map(SegmentCondition::try_from).collect::<Result<Vec<_>, _>>())
            .transpose()?;
        Ok(SegmentFilter {
            must,
            should,
            must_not,
            min_should: None,
        })
    }
}
