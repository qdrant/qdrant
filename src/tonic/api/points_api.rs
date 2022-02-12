use tonic::{Request, Response, Status};

use crate::common::points::{
    do_clear_payload, do_create_index, do_delete_index, do_delete_payload, do_delete_points,
    do_set_payload, do_update_points, CreateFieldIndex,
};
use crate::tonic::api::common::error_to_status;
use crate::tonic::qdrant::condition::ConditionOneOf;
use crate::tonic::qdrant::payload::PayloadOneOf::{Float, Geo, Integer, Keyword};
use crate::tonic::qdrant::points_selector::PointsSelectorOneOf;
use crate::tonic::qdrant::points_server::Points;
use crate::tonic::qdrant::r#match::MatchValue;
use crate::tonic::qdrant::{
    ClearPayloadPoints, Condition, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePoints, FieldCondition, Filter, FloatPayload, GeoBoundingBox,
    GeoPayload, GeoPoint, GeoRadius, HasIdCondition, IntegerPayload, KeywordPayload, Match,
    Payload, PointStruct, PointsOperationResponse, PointsSelector, Range, SetPayloadPoints,
    UpdateResult, UpsertPoints,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{
    PointIdsList, PointInsertOperations, PointOperations, PointsList,
};
use collection::operations::types::UpdateResult as CollectionUpdateResult;
use collection::operations::CollectionUpdateOperations;
use segment::types::{
    PayloadInterface, PayloadInterfaceStrict, PayloadKeyType, PayloadVariant, PointIdType,
};
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use std::time::Instant;
use storage::content_manager::toc::TableOfContent;

pub struct PointsService {
    toc: Arc<TableOfContent>,
}

impl PointsService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl Points for PointsService {
    async fn upsert(
        &self,
        request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let UpsertPoints {
            collection,
            wait,
            points,
        } = request.into_inner();

        let points = points
            .into_iter()
            .map(|point| point.try_into())
            .collect::<Result<_, _>>()?;

        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperations::PointsList(PointsList { points }),
        ));

        let timing = Instant::now();
        let result = do_update_points(
            self.toc.as_ref(),
            &collection,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePoints {
            collection,
            wait,
            points,
        } = request.into_inner();

        let points_selector = match points {
            None => return Err(Status::invalid_argument("PointSelector is missing")),
            Some(p) => p.try_into()?,
        };

        let timing = Instant::now();
        let result = do_delete_points(
            self.toc.as_ref(),
            &collection,
            points_selector,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let SetPayloadPoints {
            collection,
            wait,
            payload,
            points,
        } = request.into_inner();

        let operation = collection::operations::payload_ops::SetPayload {
            payload: payload_to_interface(payload)?,
            points: points.into_iter().map(|p| p.into()).collect(),
        };

        let timing = Instant::now();
        let result = do_set_payload(
            self.toc.as_ref(),
            &collection,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePayloadPoints {
            collection,
            wait,
            keys,
            points,
        } = request.into_inner();

        let operation = DeletePayload {
            keys,
            points: points.into_iter().map(|p| p.into()).collect(),
        };

        let timing = Instant::now();
        let result = do_delete_payload(
            self.toc.as_ref(),
            &collection,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let ClearPayloadPoints {
            collection,
            wait,
            points,
        } = request.into_inner();

        let points_selector = match points {
            None => return Err(Status::invalid_argument("PointSelector is missing")),
            Some(p) => p.try_into()?,
        };

        let timing = Instant::now();
        let result = do_clear_payload(
            self.toc.as_ref(),
            &collection,
            points_selector,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let CreateFieldIndexCollection {
            collection,
            wait,
            field_name,
        } = request.into_inner();

        let operation = CreateFieldIndex { field_name };

        let timing = Instant::now();
        let result = do_create_index(
            self.toc.as_ref(),
            &collection,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeleteFieldIndexCollection {
            collection,
            wait,
            field_name,
        } = request.into_inner();

        let timing = Instant::now();
        let result = do_delete_index(
            self.toc.as_ref(),
            &collection,
            field_name,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
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

        let converted_payload = payload_to_interface(payload)?;

        Ok(Self {
            id: PointIdType::NumId(id),
            vector,
            payload: Some(converted_payload),
        })
    }
}

fn payload_to_interface(
    payload: HashMap<String, Payload>,
) -> Result<HashMap<PayloadKeyType, PayloadInterface>, Status> {
    let mut converted_payload = HashMap::new();
    for (key, payload_value) in payload.into_iter() {
        let value = match payload_value.payload_one_of {
            Some(Keyword(k)) => k.into(),
            Some(Integer(i)) => i.into(),
            Some(Float(f)) => f.into(),
            Some(Geo(g)) => g.into(),
            None => return Err(Status::invalid_argument("Unknown payload type")),
        };
        converted_payload.insert(key, value);
    }
    Ok(converted_payload)
}

impl TryFrom<PointsSelector> for collection::operations::point_ops::PointsSelector {
    type Error = Status;

    fn try_from(value: PointsSelector) -> Result<Self, Self::Error> {
        match value.points_selector_one_of {
            Some(PointsSelectorOneOf::Points(points)) => Ok(
                collection::operations::point_ops::PointsSelector::PointIdsSelector(PointIdsList {
                    points: points.ids.into_iter().map(|p| p.into()).collect(),
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
        let set: HashSet<PointIdType> = value.has_id.into_iter().map(|p| p.into()).collect();
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

impl From<KeywordPayload> for PayloadInterface {
    fn from(value: KeywordPayload) -> Self {
        PayloadInterface::Payload(PayloadInterfaceStrict::Keyword(PayloadVariant::List(
            value.values,
        )))
    }
}

impl From<IntegerPayload> for PayloadInterface {
    fn from(value: IntegerPayload) -> Self {
        PayloadInterface::Payload(PayloadInterfaceStrict::Integer(PayloadVariant::List(
            value.values,
        )))
    }
}

impl From<FloatPayload> for PayloadInterface {
    fn from(value: FloatPayload) -> Self {
        PayloadInterface::Payload(PayloadInterfaceStrict::Float(PayloadVariant::List(
            value.values,
        )))
    }
}

impl From<GeoPayload> for PayloadInterface {
    fn from(value: GeoPayload) -> Self {
        let variant =
            PayloadVariant::List(value.values.into_iter().map(|point| point.into()).collect());
        PayloadInterface::Payload(PayloadInterfaceStrict::Geo(variant))
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

impl From<(Instant, CollectionUpdateResult)> for PointsOperationResponse {
    fn from(value: (Instant, CollectionUpdateResult)) -> Self {
        let (timing, response) = value;
        Self {
            result: Some(response.into()),
            time: timing.elapsed().as_secs_f64(),
        }
    }
}

impl From<CollectionUpdateResult> for UpdateResult {
    fn from(value: CollectionUpdateResult) -> Self {
        Self {
            operation_id: value.operation_id,
            status: value.status as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_grpc() {
        // For running build from IDE
        eprintln!("hello");
    }
}
