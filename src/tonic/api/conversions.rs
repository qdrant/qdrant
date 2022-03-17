use crate::tonic::qdrant::condition::ConditionOneOf;
use crate::tonic::qdrant::point_id::PointIdOptions;
use crate::tonic::qdrant::points_selector::PointsSelectorOneOf;
use crate::tonic::qdrant::r#match::MatchValue;
use crate::tonic::qdrant::with_payload_selector::SelectorOptions;
use crate::tonic::qdrant::{
    Condition, FieldCondition, Filter, GeoBoundingBox, GeoPoint, GeoRadius, HasIdCondition, Match,
    PointId, PointStruct, PointsOperationResponse, PointsSelector, Range, RetrievedPoint,
    ScoredPoint, SearchParams, UpdateResult, WithPayloadSelector,
};
use collection::operations::point_ops::PointIdsList;
use prost_types::value::Kind;
use prost_types::ListValue;
use segment::types::{
    Payload, PayloadSelectorExclude, PayloadSelectorInclude, PointIdType, WithPayloadInterface,
};
use serde_json::{Map, Number, Value};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tonic::Status;
use uuid::Uuid;

pub fn payload_to_proto(payload: Payload) -> HashMap<String, prost_types::Value> {
    payload
        .into_iter()
        .map(|(k, v)| (k, json_to_proto(v)))
        .collect()
}

fn json_to_proto(json_value: Value) -> prost_types::Value {
    match json_value {
        Value::Null => prost_types::Value {
            kind: Some(Kind::NullValue(0)),
        },
        Value::Bool(v) => prost_types::Value {
            kind: Some(Kind::BoolValue(v)),
        },
        Value::Number(n) => prost_types::Value {
            kind: Some(Kind::NumberValue(n.as_f64().unwrap())),
        },
        Value::String(s) => prost_types::Value {
            kind: Some(Kind::StringValue(s)),
        },
        Value::Array(v) => {
            let list = v.into_iter().map(json_to_proto).collect();
            prost_types::Value {
                kind: Some(Kind::ListValue(ListValue { values: list })),
            }
        }
        Value::Object(m) => {
            let map = m.into_iter().map(|(k, v)| (k, json_to_proto(v))).collect();
            prost_types::Value {
                kind: Some(Kind::StructValue(prost_types::Struct { fields: map })),
            }
        }
    }
}

pub fn proto_to_payloads(proto: HashMap<String, prost_types::Value>) -> Result<Payload, Status> {
    let mut map: Map<String, Value> = Map::new();
    for (k, v) in proto.into_iter() {
        map.insert(k, proto_to_json(v)?);
    }
    Ok(map.into())
}

fn proto_to_json(proto: prost_types::Value) -> Result<Value, Status> {
    match proto.kind {
        None => Ok(Value::default()),
        Some(kind) => match kind {
            Kind::NullValue(_) => Ok(Value::Null),
            Kind::NumberValue(n) => {
                let v = match Number::from_f64(n) {
                    Some(f) => f,
                    None => return Err(Status::invalid_argument("cannot convert to json number")),
                };
                Ok(Value::Number(v))
            }
            Kind::StringValue(s) => Ok(Value::String(s)),
            Kind::BoolValue(b) => Ok(Value::Bool(b)),
            Kind::StructValue(s) => {
                let mut map = Map::new();
                for (k, v) in s.fields.into_iter() {
                    map.insert(k, proto_to_json(v)?);
                }
                Ok(Value::Object(map))
            }
            Kind::ListValue(l) => {
                let mut list = Vec::new();
                for v in l.values.into_iter() {
                    list.push(proto_to_json(v)?);
                }
                Ok(Value::Array(list))
            }
        },
    }
}

impl From<segment::types::GeoPoint> for GeoPoint {
    fn from(geo: segment::types::GeoPoint) -> Self {
        Self {
            lon: geo.lon,
            lat: geo.lat,
        }
    }
}

impl TryFrom<WithPayloadSelector> for WithPayloadInterface {
    type Error = Status;

    fn try_from(value: WithPayloadSelector) -> Result<Self, Self::Error> {
        match value.selector_options {
            Some(options) => Ok(match options {
                SelectorOptions::Enable(flag) => WithPayloadInterface::Bool(flag),
                SelectorOptions::Exclude(s) => PayloadSelectorExclude::new(s.exclude).into(),
                SelectorOptions::Include(s) => PayloadSelectorInclude::new(s.include).into(),
            }),
            _ => Err(Status::invalid_argument("No PayloadSelector".to_string())),
        }
    }
}

impl From<SearchParams> for segment::types::SearchParams {
    fn from(params: SearchParams) -> Self {
        Self {
            hnsw_ef: params.hnsw_ef.map(|x| x as usize),
        }
    }
}

impl From<PointIdType> for PointId {
    fn from(point_id: PointIdType) -> Self {
        PointId {
            point_id_options: Some(match point_id {
                PointIdType::NumId(num) => PointIdOptions::Num(num),
                PointIdType::Uuid(uuid) => PointIdOptions::Uuid(uuid.to_string()),
            }),
        }
    }
}

impl From<segment::types::ScoredPoint> for ScoredPoint {
    fn from(point: segment::types::ScoredPoint) -> Self {
        Self {
            id: Some(point.id.into()),
            payload: point.payload.map(payload_to_proto).unwrap_or_default(),
            score: point.score,
            vector: point.vector.unwrap_or_default(),
            version: point.version,
        }
    }
}

impl From<collection::operations::types::Record> for RetrievedPoint {
    fn from(record: collection::operations::types::Record) -> Self {
        Self {
            id: Some(record.id.into()),
            payload: record.payload.map(payload_to_proto).unwrap_or_default(),
            vector: record.vector.unwrap_or_default(),
        }
    }
}

impl TryFrom<PointId> for PointIdType {
    type Error = Status;

    fn try_from(value: PointId) -> Result<Self, Self::Error> {
        match value.point_id_options {
            Some(PointIdOptions::Num(num_id)) => Ok(PointIdType::NumId(num_id)),
            Some(PointIdOptions::Uuid(uui_str)) => Uuid::parse_str(&uui_str)
                .map(PointIdType::Uuid)
                .map_err(|_err| {
                    Status::invalid_argument(format!("Unable to parse UUID: {}", uui_str))
                }),
            _ => Err(Status::invalid_argument(
                "No ID options provided".to_string(),
            )),
        }
    }
}

impl TryFrom<PointStruct> for collection::operations::point_ops::PointStruct {
    type Error = Status;

    fn try_from(value: PointStruct) -> Result<Self, Self::Error> {
        let PointStruct {
            id,
            vector,
            payload,
        } = value;

        let converted_payload = proto_to_payloads(payload)?;

        Ok(Self {
            id: id
                .ok_or_else(|| Status::invalid_argument("Empty ID is not allowed"))?
                .try_into()?,
            vector,
            payload: Some(converted_payload),
        })
    }
}

impl TryFrom<PointsSelector> for collection::operations::point_ops::PointsSelector {
    type Error = Status;

    fn try_from(value: PointsSelector) -> Result<Self, Self::Error> {
        match value.points_selector_one_of {
            Some(PointsSelectorOneOf::Points(points)) => Ok(
                collection::operations::point_ops::PointsSelector::PointIdsSelector(PointIdsList {
                    points: points
                        .ids
                        .into_iter()
                        .map(|p| p.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                }),
            ),
            Some(PointsSelectorOneOf::Filter(f)) => Ok(
                collection::operations::point_ops::PointsSelector::FilterSelector(
                    collection::operations::point_ops::FilterSelector {
                        filter: f.try_into()?,
                    },
                ),
            ),
            _ => Err(Status::invalid_argument("Malformed PointsSelector type")),
        }
    }
}

fn conditions_helper(
    conditions: Vec<Condition>,
) -> Result<Option<Vec<segment::types::Condition>>, tonic::Status> {
    if conditions.is_empty() {
        Ok(None)
    } else {
        let vec = conditions
            .into_iter()
            .map(|c| c.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Some(vec))
    }
}

impl TryFrom<Filter> for segment::types::Filter {
    type Error = Status;

    fn try_from(value: Filter) -> Result<Self, Self::Error> {
        Ok(Self {
            should: conditions_helper(value.should)?,
            must: conditions_helper(value.must)?,
            must_not: conditions_helper(value.must_not)?,
        })
    }
}

impl TryFrom<Condition> for segment::types::Condition {
    type Error = Status;

    fn try_from(value: Condition) -> Result<Self, Self::Error> {
        match value.condition_one_of {
            Some(ConditionOneOf::Field(field)) => {
                Ok(segment::types::Condition::Field(field.try_into()?))
            }
            Some(ConditionOneOf::HasId(has_id)) => {
                Ok(segment::types::Condition::HasId(has_id.try_into()?))
            }
            Some(ConditionOneOf::Filter(filter)) => {
                Ok(segment::types::Condition::Filter(filter.try_into()?))
            }
            _ => Err(Status::invalid_argument("Malformed Condition type")),
        }
    }
}

impl TryFrom<HasIdCondition> for segment::types::HasIdCondition {
    type Error = Status;

    fn try_from(value: HasIdCondition) -> Result<Self, Self::Error> {
        let set: HashSet<PointIdType> = value
            .has_id
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<_, _>>()?;
        Ok(Self { has_id: set })
    }
}

impl TryFrom<FieldCondition> for segment::types::FieldCondition {
    type Error = Status;

    fn try_from(value: FieldCondition) -> Result<Self, Self::Error> {
        let FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box,
            geo_radius,
        } = value;

        let geo_bounding_box =
            geo_bounding_box.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        let geo_radius = geo_radius.map_or_else(|| Ok(None), |g| g.try_into().map(Some))?;
        Ok(Self {
            key,
            r#match: r#match.map_or_else(|| Ok(None), |m| m.try_into().map(Some))?,
            range: range.map(|r| r.into()),
            geo_bounding_box,
            geo_radius,
        })
    }
}

impl TryFrom<GeoBoundingBox> for segment::types::GeoBoundingBox {
    type Error = Status;

    fn try_from(value: GeoBoundingBox) -> Result<Self, Self::Error> {
        match value {
            GeoBoundingBox {
                top_left: Some(t),
                bottom_right: Some(b),
            } => Ok(Self {
                top_left: t.into(),
                bottom_right: b.into(),
            }),
            _ => Err(Status::invalid_argument("Malformed GeoBoundingBox type")),
        }
    }
}

impl TryFrom<GeoRadius> for segment::types::GeoRadius {
    type Error = Status;

    fn try_from(value: GeoRadius) -> Result<Self, Self::Error> {
        match value {
            GeoRadius {
                center: Some(c),
                radius,
            } => Ok(Self {
                center: c.into(),
                radius: radius.into(),
            }),
            _ => Err(Status::invalid_argument("Malformed GeoRadius type")),
        }
    }
}

impl From<GeoPoint> for segment::types::GeoPoint {
    fn from(value: GeoPoint) -> Self {
        Self {
            lon: value.lon,
            lat: value.lat,
        }
    }
}

impl From<Range> for segment::types::Range {
    fn from(value: Range) -> Self {
        Self {
            lt: value.lt,
            gt: value.gt,
            gte: value.gte,
            lte: value.lte,
        }
    }
}

impl TryFrom<Match> for segment::types::Match {
    type Error = Status;

    fn try_from(value: Match) -> Result<Self, Self::Error> {
        match value.match_value {
            Some(mv) => Ok(match mv {
                MatchValue::Keyword(kw) => kw.into(),
                MatchValue::Integer(int) => int.into(),
            }),
            _ => Err(Status::invalid_argument("Malformed Match condition")),
        }
    }
}

impl From<(Instant, collection::operations::types::UpdateResult)> for PointsOperationResponse {
    fn from(value: (Instant, collection::operations::types::UpdateResult)) -> Self {
        let (timing, response) = value;
        Self {
            result: Some(response.into()),
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<collection::operations::types::UpdateResult> for UpdateResult {
    fn from(value: collection::operations::types::UpdateResult) -> Self {
        Self {
            operation_id: value.operation_id,
            status: value.status as i32,
        }
    }
}
