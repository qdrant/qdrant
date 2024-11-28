use std::sync::Arc;
use std::time::Duration;

use api::rest::*;
use collection::operations::conversions::*;
use collection::operations::payload_ops::*;
use collection::operations::point_ops::*;
use collection::operations::vector_ops::*;
use collection::operations::*;
use collection::shards::shard::ShardId;
use segment::types::{ExtendedPointId, Filter};
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use validator::Validate;

use self::shard_selector_internal::ShardSelectorInternal;
use self::verification::StrictModeVerification;
use super::auth::extract_access;
use super::verification::{CheckedTocProvider as _, StrictModeCheckedTocProvider};
use crate::actix::update::*;
use crate::common::inference::extract_token;
use crate::common::points::{CreateFieldIndex, UpdateOperations, *};
use crate::tonic::api::validate;

pub async fn update<T>(
    disp: Arc<Dispatcher>,
    mut request: tonic::Request<T>,
) -> Result<tonic::Response<api::grpc::qdrant::PointsOperationResponse>, tonic::Status>
where
    T: GrpcMessage + Validate,
    T::GrpcOperation: GrpcOperation,
    <T::GrpcOperation as GrpcOperation>::RestOperation: RestRequest + StrictModeVerification,
{
    validate(request.get_ref())?;

    let access = extract_access(&mut request);
    let inference_token = extract_token(&request);

    let (operation, params) = request.into_inner().extract_operation();
    let rest = operation.into_rest_operation()?;

    let strict_mode_checker = StrictModeCheckedTocProvider::new(&disp);

    let toc = strict_mode_checker
        .check_strict_mode(&rest, &params.collection_name, None, &access)
        .await?;

    let Update {
        operation,
        shard_key,
    } = rest.into_update(inference_token).await?;

    let shard_selector = ShardSelectorInternal::from(shard_key);

    let result = toc
        .update(
            &params.collection_name,
            OperationWithClockTag::from(operation),
            params.wait,
            params.ordering,
            shard_selector,
            access,
        )
        .await?;

    let resp = api::grpc::qdrant::PointsOperationResponse {
        result: Some(result.into()),
        time: Duration::ZERO.as_secs_f64(),
    };

    Ok(tonic::Response::new(resp))
}

pub async fn update_batch(
    toc: Arc<Dispatcher>,
    request: tonic::Request<api::grpc::qdrant::UpdateBatchPoints>,
) -> Result<tonic::Response<api::grpc::qdrant::UpdateBatchResponse>, tonic::Status> {
    todo!()
}

pub async fn update_internal<T>(
    toc: Arc<TableOfContent>,
    request: tonic::Request<T>,
) -> Result<tonic::Response<api::grpc::qdrant::PointsOperationResponseInternal>, tonic::Status>
where
    T: InternalGrpcMessage,
    T::GrpcMessage: GrpcMessage,
    <T::GrpcMessage as GrpcMessage>::GrpcOperation: GrpcOperation,
    <<T::GrpcMessage as GrpcMessage>::GrpcOperation as GrpcOperation>::RestOperation: RestRequest,
{
    todo!()
}

//
// Helper wrappers
//

#[derive(Clone, Debug)]
pub struct SetPayloadHelper<T>(pub T);

impl<T> From<T> for SetPayloadHelper<T> {
    fn from(set_payload: T) -> Self {
        Self(set_payload)
    }
}

#[derive(Clone, Debug)]
pub struct OverwritePayloadHelper<T>(pub T);

impl<T> From<T> for OverwritePayloadHelper<T> {
    fn from(set_payload: T) -> Self {
        Self(set_payload)
    }
}

//
// Conversion traits and types
//

pub trait GrpcOperation {
    type RestOperation;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status>;
}

pub trait GrpcMessage {
    type GrpcOperation;
    fn extract_operation(self) -> (Self::GrpcOperation, UpdateParams);
}

pub trait InternalGrpcMessage {
    type GrpcMessage;
    fn extract_message(self) -> Result<(Self::GrpcMessage, InternalUpdateParams), tonic::Status>;
}

#[derive(Clone, Debug)]
pub struct UpdateParams {
    collection_name: String,
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
    ordering: WriteOrdering,
    wait: bool,
}

impl UpdateParams {
    pub fn from_grpc(
        collection_name: String,
        ordering: Option<api::grpc::qdrant::WriteOrdering>,
        wait: Option<bool>,
    ) -> UpdateParams {
        UpdateParams {
            collection_name,
            shard_id: None,
            clock_tag: None,
            ordering: write_ordering_from_proto(ordering).unwrap(), // TODO!
            wait: wait.unwrap_or(false),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct InternalUpdateParams {
    shard_id: Option<ShardId>,
    clock_tag: Option<ClockTag>,
}

//
// gRPC to REST conversions
//

impl GrpcOperation for api::grpc::qdrant::points_update_operation::PointStructList {
    type RestOperation = PointInsertOperations;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points,
            shard_key_selector,
        } = self;

        let points = points
            .into_iter()
            .map(PointStruct::try_from)
            .collect::<Result<_, _>>()?;

        let rest = PointInsertOperations::PointsList(PointsList {
            points,
            shard_key: shard_key_selector.map(Into::into),
        });

        Ok(rest)
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::DeletePoints {
    type RestOperation = DeletePointsHelper;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points,
            shard_key_selector,
        } = self;

        let rest =
            PointsSelectorHelper::try_from(points)?.into_rest(shard_key_selector.map(Into::into));

        Ok(Self::RestOperation::from(rest))
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::UpdateVectors {
    type RestOperation = UpdateVectors;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points,
            shard_key_selector,
        } = self;

        let points = points
            .into_iter()
            .map(PointVectors::try_from)
            .collect::<Result<_, _>>()?;

        let rest = Self::RestOperation {
            points,
            shard_key: shard_key_selector.map(Into::into),
        };

        Ok(rest)
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::DeleteVectors {
    type RestOperation = DeleteVectors;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points_selector,
            vectors,
            shard_key_selector,
        } = self;

        let (points, filter) = PointsSelectorHelper::try_from(points_selector)?.into_inner();

        let vector = vectors
            .map(|vectors| vectors.names.into_iter().collect())
            .unwrap_or_default();

        let op = Self::RestOperation {
            points,
            filter,
            vector,
            shard_key: shard_key_selector.map(Into::into),
        };

        Ok(op)
    }
}

impl GrpcOperation for SetPayloadHelper<api::grpc::qdrant::points_update_operation::SetPayload> {
    type RestOperation = crate::actix::update::SetPayloadHelper;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let rest = self.0.into_rest_operation()?;
        Ok(Self::RestOperation::from(rest))
    }
}

impl GrpcOperation
    for OverwritePayloadHelper<api::grpc::qdrant::points_update_operation::SetPayload>
{
    type RestOperation = crate::actix::update::OverwritePayloadHelper;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let rest = self.0.into_rest_operation()?;
        Ok(Self::RestOperation::from(rest))
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::SetPayload {
    type RestOperation = SetPayload;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            payload,
            points_selector,
            shard_key_selector,
            key,
        } = self;

        let payload = api::conversions::json::proto_to_payloads(payload)?;

        let (points, filter) = match points_selector.try_into()? {
            PointsSelectorHelper::Points(points) => (Some(points), None),
            PointsSelectorHelper::Filter(filter) => (None, Some(filter)),
        };

        let key = key
            .as_deref()
            .map(api::conversions::json::json_path_from_proto)
            .transpose()?;

        let rest = Self::RestOperation {
            payload,
            points,
            filter,
            key,
            shard_key: shard_key_selector.map(Into::into),
        };

        Ok(rest)
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::OverwritePayload {
    type RestOperation = SetPayload;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            payload,
            points_selector,
            shard_key_selector,
            key,
        } = self;

        let payload = api::conversions::json::proto_to_payloads(payload)?;

        let (points, filter) = match points_selector.try_into()? {
            PointsSelectorHelper::Points(points) => (Some(points), None),
            PointsSelectorHelper::Filter(filter) => (None, Some(filter)),
        };

        let key = key
            .as_deref()
            .map(api::conversions::json::json_path_from_proto)
            .transpose()?;

        let rest = Self::RestOperation {
            payload,
            points,
            filter,
            key,
            shard_key: shard_key_selector.map(Into::into),
        };

        Ok(rest)
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::DeletePayload {
    type RestOperation = DeletePayload;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            keys,
            points_selector,
            shard_key_selector,
        } = self;

        let keys = keys
            .iter()
            .map(|key| api::conversions::json::json_path_from_proto(key))
            .collect::<Result<_, _>>()?;

        let (points, filter) = PointsSelectorHelper::try_from(points_selector)?.into_inner();

        let rest = Self::RestOperation {
            keys,
            points,
            filter,
            shard_key: shard_key_selector.map(Into::into),
        };

        Ok(rest)
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::ClearPayload {
    type RestOperation = ClearPayloadHelper;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points,
            shard_key_selector,
        } = self;

        let rest =
            PointsSelectorHelper::try_from(points)?.into_rest(shard_key_selector.map(Into::into));

        Ok(Self::RestOperation::from(rest))
    }
}

impl GrpcOperation for Vec<api::grpc::qdrant::PointsUpdateOperation> {
    type RestOperation = UpdateOperations;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let operations = self
            .into_iter()
            .map(GrpcOperation::into_rest_operation)
            .collect::<Result<_, _>>()?;

        Ok(Self::RestOperation { operations })
    }
}

impl GrpcOperation for api::grpc::qdrant::PointsUpdateOperation {
    type RestOperation = UpdateOperation;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        self.operation
            .ok_or_else(|| tonic::Status::invalid_argument("TODO"))? // TODO
            .into_rest_operation()
    }
}

impl GrpcOperation for api::grpc::qdrant::points_update_operation::Operation {
    type RestOperation = UpdateOperation;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let rest = match self {
            Self::Upsert(point_list) => {
                // ...
                UpdateOperation::Upsert(UpsertOperation {
                    upsert: point_list.into_rest_operation()?,
                })
            }
            Self::DeletePoints(delete_points) => {
                // ...
                UpdateOperation::Delete(DeleteOperation {
                    delete: delete_points.into_rest_operation()?.0,
                })
            }
            Self::DeleteDeprecated(points_selector) => {
                UpdateOperation::Delete(DeleteOperation { delete: todo!() })
            }
            Self::UpdateVectors(update_vectors) => {
                UpdateOperation::UpdateVectors(UpdateVectorsOperation {
                    update_vectors: update_vectors.into_rest_operation()?,
                })
            }
            Self::DeleteVectors(delete_vectors) => {
                UpdateOperation::DeleteVectors(DeleteVectorsOperation {
                    delete_vectors: delete_vectors.into_rest_operation()?,
                })
            }
            Self::SetPayload(set_payload) => {
                // ...
                UpdateOperation::SetPayload(SetPayloadOperation {
                    set_payload: set_payload.into_rest_operation()?,
                })
            }
            Self::OverwritePayload(overwrite_payload) => {
                UpdateOperation::OverwritePayload(OverwritePayloadOperation {
                    overwrite_payload: overwrite_payload.into_rest_operation()?,
                })
            }
            Self::DeletePayload(delete_payload) => {
                // ...
                UpdateOperation::DeletePayload(DeletePayloadOperation {
                    delete_payload: delete_payload.into_rest_operation()?,
                })
            }
            Self::ClearPayload(clear_payload) => {
                // ...
                UpdateOperation::ClearPayload(ClearPayloadOperation {
                    clear_payload: clear_payload.into_rest_operation()?.0,
                })
            }
            Self::ClearPayloadDeprecated(points_selector) => {
                // ...
                UpdateOperation::ClearPayload(ClearPayloadOperation {
                    clear_payload: todo!(),
                })
            }
        };

        Ok(rest)
    }
}

#[derive(Clone, Debug)]
pub struct CreateFieldIndexOperation {
    field_name: String,
    field_type: Option<i32>,
    field_index_params: Option<api::grpc::qdrant::PayloadIndexParams>,
}

impl GrpcOperation for CreateFieldIndexOperation {
    type RestOperation = CreateFieldIndex;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            field_name,
            field_type,
            field_index_params,
        } = self;

        let field_name = api::conversions::json::json_path_from_proto(&field_name)?;

        let field_schema =
            crate::tonic::api::points_common::convert_field_type(field_type, field_index_params)?;

        let rest = Self::RestOperation {
            field_name,
            field_schema,
        };

        Ok(rest)
    }
}

#[derive(Clone, Debug)]
pub struct DeleteFieldIndexOperation {
    field_name: String,
}

impl GrpcOperation for DeleteFieldIndexOperation {
    type RestOperation = DeleteFieldIndexHelper;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self { field_name } = self;
        let rest = api::conversions::json::json_path_from_proto(&field_name)?;
        Ok(Self::RestOperation::from(rest))
    }
}

#[derive(Clone, Debug)]
pub struct SyncPointsOperation {
    points: Vec<api::grpc::qdrant::PointStruct>,
    from_id: Option<api::grpc::qdrant::PointId>,
    to_id: Option<api::grpc::qdrant::PointId>,
}

impl GrpcOperation for SyncPointsOperation {
    type RestOperation = SyncPointsRequest;

    fn into_rest_operation(self) -> Result<Self::RestOperation, tonic::Status> {
        let Self {
            points,
            from_id,
            to_id,
        } = self;

        let points = points
            .into_iter()
            .map(PointStruct::try_from)
            .collect::<Result<_, _>>()?;

        let rest = Self::RestOperation {
            points,
            from_id: from_id.map(ExtendedPointId::try_from).transpose()?,
            to_id: to_id.map(ExtendedPointId::try_from).transpose()?,
        };

        Ok(rest)
    }
}

//
// gRPC to batch conversions
//

macro_rules! grpc_message {
    ($message:ty, $operation:ty, $($field:ident),+ $(,)*) => {
        impl GrpcMessage for $message {
            type GrpcOperation = $operation;

            fn extract_operation(self) -> (Self::GrpcOperation, UpdateParams) {
                let Self {
                    collection_name,
                    ordering,
                    wait,
                    $($field),+
                } = self;

                let operation = Self::GrpcOperation {
                    $($field),+
                };

                let params = UpdateParams::from_grpc(collection_name, ordering, wait);

                (operation, params)
            }
        }
    };
}

grpc_message!(
    api::grpc::qdrant::UpsertPoints,
    api::grpc::qdrant::points_update_operation::PointStructList,
    points,
    shard_key_selector,
);

grpc_message!(
    api::grpc::qdrant::DeletePoints,
    api::grpc::qdrant::points_update_operation::DeletePoints,
    points,
    shard_key_selector,
);

grpc_message!(
    api::grpc::qdrant::UpdatePointVectors,
    api::grpc::qdrant::points_update_operation::UpdateVectors,
    points,
    shard_key_selector,
);

grpc_message!(
    api::grpc::qdrant::DeletePointVectors,
    api::grpc::qdrant::points_update_operation::DeleteVectors,
    points_selector,
    vectors,
    shard_key_selector,
);

impl GrpcMessage for SetPayloadHelper<api::grpc::qdrant::SetPayloadPoints> {
    type GrpcOperation = SetPayloadHelper<api::grpc::qdrant::points_update_operation::SetPayload>;

    fn extract_operation(self) -> (Self::GrpcOperation, UpdateParams) {
        let (op, params) = self.0.extract_operation();
        (op.into(), params)
    }
}

impl GrpcMessage for OverwritePayloadHelper<api::grpc::qdrant::SetPayloadPoints> {
    type GrpcOperation =
        OverwritePayloadHelper<api::grpc::qdrant::points_update_operation::SetPayload>;

    fn extract_operation(self) -> (Self::GrpcOperation, UpdateParams) {
        let (op, params) = self.0.extract_operation();
        (op.into(), params)
    }
}

grpc_message!(
    api::grpc::qdrant::SetPayloadPoints,
    api::grpc::qdrant::points_update_operation::SetPayload,
    payload,
    points_selector,
    key,
    shard_key_selector,
);

grpc_message!(
    api::grpc::qdrant::DeletePayloadPoints,
    api::grpc::qdrant::points_update_operation::DeletePayload,
    keys,
    points_selector,
    shard_key_selector,
);

grpc_message!(
    api::grpc::qdrant::ClearPayloadPoints,
    api::grpc::qdrant::points_update_operation::ClearPayload,
    points,
    shard_key_selector,
);

impl GrpcMessage for api::grpc::qdrant::UpdateBatchPoints {
    type GrpcOperation = Vec<api::grpc::qdrant::PointsUpdateOperation>;

    fn extract_operation(self) -> (Self::GrpcOperation, UpdateParams) {
        let Self {
            collection_name,
            wait,
            operations,
            ordering,
        } = self;

        let operation = operations;
        let params = UpdateParams::from_grpc(collection_name, ordering, wait);

        (operation, params)
    }
}

grpc_message!(
    api::grpc::qdrant::CreateFieldIndexCollection,
    CreateFieldIndexOperation,
    field_name,
    field_type,
    field_index_params,
);

grpc_message!(
    api::grpc::qdrant::DeleteFieldIndexCollection,
    DeleteFieldIndexOperation,
    field_name,
);

grpc_message!(
    api::grpc::qdrant::SyncPoints,
    SyncPointsOperation,
    points,
    from_id,
    to_id,
);

//
// Internal gRPC to public conversions
//

macro_rules! internal_grpc_message {
    ($internal:ty, $public:ty, $field:ident $(,)*) => {
        impl InternalGrpcMessage for $internal {
            type GrpcMessage = $public;

            fn extract_message(
                self,
            ) -> Result<(Self::GrpcMessage, InternalUpdateParams), tonic::Status> {
                let Self {
                    $field,
                    shard_id,
                    clock_tag,
                } = self;

                let message = $field
                    .ok_or_else(|| tonic::Status::invalid_argument("missing update operation"))?; // TODO: Reference `$field` in the error message?

                let params = InternalUpdateParams {
                    shard_id,
                    clock_tag: clock_tag.map(Into::into),
                };

                Ok((message, params))
            }
        }
    };
}

internal_grpc_message!(
    api::grpc::qdrant::UpsertPointsInternal,
    api::grpc::qdrant::UpsertPoints,
    upsert_points,
);

internal_grpc_message!(
    api::grpc::qdrant::DeletePointsInternal,
    api::grpc::qdrant::DeletePoints,
    delete_points,
);

internal_grpc_message!(
    api::grpc::qdrant::UpdateVectorsInternal,
    api::grpc::qdrant::UpdatePointVectors,
    update_vectors,
);

internal_grpc_message!(
    api::grpc::qdrant::DeleteVectorsInternal,
    api::grpc::qdrant::DeletePointVectors,
    delete_vectors,
);

impl InternalGrpcMessage for SetPayloadHelper<api::grpc::qdrant::SetPayloadPointsInternal> {
    type GrpcMessage = SetPayloadHelper<api::grpc::qdrant::SetPayloadPoints>;

    fn extract_message(self) -> Result<(Self::GrpcMessage, InternalUpdateParams), tonic::Status> {
        let (message, params) = self.0.extract_message()?;
        Ok((Self::GrpcMessage::from(message), params))
    }
}

impl InternalGrpcMessage for OverwritePayloadHelper<api::grpc::qdrant::SetPayloadPointsInternal> {
    type GrpcMessage = OverwritePayloadHelper<api::grpc::qdrant::SetPayloadPoints>;

    fn extract_message(self) -> Result<(Self::GrpcMessage, InternalUpdateParams), tonic::Status> {
        let (message, params) = self.0.extract_message()?;
        Ok((Self::GrpcMessage::from(message), params))
    }
}

internal_grpc_message!(
    api::grpc::qdrant::SetPayloadPointsInternal,
    api::grpc::qdrant::SetPayloadPoints,
    set_payload_points,
);

internal_grpc_message!(
    api::grpc::qdrant::DeletePayloadPointsInternal,
    api::grpc::qdrant::DeletePayloadPoints,
    delete_payload_points,
);

internal_grpc_message!(
    api::grpc::qdrant::ClearPayloadPointsInternal,
    api::grpc::qdrant::ClearPayloadPoints,
    clear_payload_points,
);

internal_grpc_message!(
    api::grpc::qdrant::CreateFieldIndexCollectionInternal,
    api::grpc::qdrant::CreateFieldIndexCollection,
    create_field_index_collection,
);

internal_grpc_message!(
    api::grpc::qdrant::DeleteFieldIndexCollectionInternal,
    api::grpc::qdrant::DeleteFieldIndexCollection,
    delete_field_index_collection,
);

internal_grpc_message!(
    api::grpc::qdrant::SyncPointsInternal,
    api::grpc::qdrant::SyncPoints,
    sync_points,
);

//
// gRPC-specific conversion helpers
//

#[derive(Clone, Debug)]
enum PointsSelectorHelper {
    Points(Vec<ExtendedPointId>),
    Filter(Filter),
}

impl PointsSelectorHelper {
    pub fn into_rest(self, shard_key: Option<ShardKeySelector>) -> PointsSelector {
        match self {
            PointsSelectorHelper::Points(points) => {
                PointsSelector::PointIdsSelector(PointIdsList { points, shard_key })
            }

            PointsSelectorHelper::Filter(filter) => {
                PointsSelector::FilterSelector(FilterSelector { filter, shard_key })
            }
        }
    }

    pub fn into_inner(self) -> (Option<Vec<ExtendedPointId>>, Option<Filter>) {
        match self {
            PointsSelectorHelper::Points(points) => (Some(points), None),
            PointsSelectorHelper::Filter(filter) => (None, Some(filter)),
        }
    }
}

impl TryFrom<Option<api::grpc::qdrant::PointsSelector>> for PointsSelectorHelper {
    type Error = tonic::Status;

    fn try_from(selector: Option<api::grpc::qdrant::PointsSelector>) -> Result<Self, Self::Error> {
        let Some(selector) = selector else {
            return Err(tonic::Status::invalid_argument(
                "points selector is missing",
            ));
        };

        Self::try_from(selector)
    }
}

impl TryFrom<api::grpc::qdrant::PointsSelector> for PointsSelectorHelper {
    type Error = tonic::Status;

    fn try_from(selector: api::grpc::qdrant::PointsSelector) -> Result<Self, Self::Error> {
        let Some(selector) = selector.points_selector_one_of else {
            return Err(tonic::Status::invalid_argument("malformed points selector"));
        };

        let selector = match selector {
            api::grpc::qdrant::points_selector::PointsSelectorOneOf::Points(points) => {
                let ids = points
                    .ids
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?;

                Self::Points(ids)
            }

            api::grpc::qdrant::points_selector::PointsSelectorOneOf::Filter(filter) => {
                let filter = filter.try_into()?;
                Self::Filter(filter)
            }
        };

        Ok(selector)
    }
}
