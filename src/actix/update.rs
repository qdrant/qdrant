use std::sync::Arc;

use api::rest::schema::*;
use collection::collection::Collection;
use collection::operations::payload_ops::*;
use collection::operations::point_ops::{WriteOrdering, *};
use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::{CollectionError, UpdateResult};
use collection::operations::vector_ops::*;
use collection::operations::verification::StrictModeVerification;
use collection::operations::*;
use schemars::JsonSchema;
use segment::json_path::JsonPath;
use segment::types::{ExtendedPointId, StrictModeConfig};
use serde::{Deserialize, Serialize};
use storage::content_manager::collection_verification::check_strict_mode;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac;
use validator::Validate;

use super::api::CollectionPath;
use super::helpers;
use crate::common::inference::service::*;
use crate::common::inference::update_requests::*;
use crate::common::inference::InferenceToken;
use crate::common::points::*;

pub async fn update<T>(
    dispatcher: Arc<Dispatcher>,
    access: rbac::Access,
    inference_token: InferenceToken,
    collection: CollectionPath,
    request: T,
    params: UpdateParam,
) -> actix_web::HttpResponse
where
    T: RestRequest + StrictModeVerification,
{
    helpers::time(update_impl(
        dispatcher,
        access,
        inference_token,
        collection,
        request,
        params,
    ))
    .await
}

async fn update_impl<T>(
    dispatcher: Arc<Dispatcher>,
    access: rbac::Access,
    inference_token: InferenceToken,
    collection: CollectionPath,
    request: T,
    params: UpdateParam,
) -> Result<UpdateResult, StorageError>
where
    T: RestRequest + StrictModeVerification,
{
    let pass = check_strict_mode(&request, None, &collection.name, &dispatcher, &access).await?;

    let update = request.into_update(inference_token).await?;

    let operation = OperationWithClockTag::from(update.operation);
    let shard_selector = ShardSelectorInternal::from(update.shard_key);

    let ordering = params.ordering.unwrap_or(WriteOrdering::Weak);
    let wait = params.wait.unwrap_or(false);

    dispatcher
        .toc(&access, &pass)
        .update(
            &collection.name,
            operation,
            wait,
            ordering,
            shard_selector,
            access,
        )
        .await
}

pub async fn update_batch(
    dispatcher: Arc<Dispatcher>,
    access: rbac::Access,
    inference_token: InferenceToken,
    collection: CollectionPath,
    batch: UpdateOperations,
    params: UpdateParam,
) -> actix_web::HttpResponse {
    helpers::time(update_batch_impl(
        dispatcher,
        access,
        inference_token,
        collection,
        batch,
        params,
    ))
    .await
}

async fn update_batch_impl(
    dispatcher: Arc<Dispatcher>,
    access: rbac::Access,
    inference_token: InferenceToken,
    collection: CollectionPath,
    batch: UpdateOperations,
    params: UpdateParam,
) -> Result<Vec<UpdateResult>, StorageError> {
    let pass = check_strict_mode(&batch, None, &collection.name, &dispatcher, &access).await?;

    let mut results = Vec::new();

    let ordering = params.ordering.unwrap_or(WriteOrdering::Weak);
    let wait = params.wait.unwrap_or(false);

    for operation in batch.operations {
        let update = operation.into_update(inference_token.clone()).await?;

        let operation = OperationWithClockTag::from(update.operation);
        let shard_selector = ShardSelectorInternal::from(update.shard_key);

        let result = dispatcher
            .toc(&access, &pass)
            .update(
                &collection.name,
                operation,
                wait,
                ordering,
                shard_selector,
                access.clone(), // TODO: Change `TableOfContent::update` signature to `&Access`!
            )
            .await?;

        results.push(result);
    }

    Ok(results)
}

//
// Helper types and wrappers
//

#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct UpdateParam {
    ordering: Option<WriteOrdering>,
    wait: Option<bool>,
}

#[derive(Debug)]
pub struct DeletePointsHelper(pub PointsSelector);

impl From<PointsSelector> for DeletePointsHelper {
    fn from(selector: PointsSelector) -> Self {
        Self(selector)
    }
}

#[derive(Clone, Debug)]
pub struct SetPayloadHelper(pub SetPayload);

impl From<SetPayload> for SetPayloadHelper {
    fn from(set_payload: SetPayload) -> Self {
        Self(set_payload)
    }
}

#[derive(Clone, Debug)]
pub struct OverwritePayloadHelper(pub SetPayload);

impl From<SetPayload> for OverwritePayloadHelper {
    fn from(set_payload: SetPayload) -> Self {
        Self(set_payload)
    }
}

#[derive(Debug)]
pub struct ClearPayloadHelper(pub PointsSelector);

impl From<PointsSelector> for ClearPayloadHelper {
    fn from(selector: PointsSelector) -> Self {
        Self(selector)
    }
}

#[derive(Clone, Debug)]
pub struct DeleteFieldIndexHelper(pub JsonPath);

impl From<JsonPath> for DeleteFieldIndexHelper {
    fn from(json_path: JsonPath) -> Self {
        Self(json_path)
    }
}

//
// Conversion traits and  types
//

pub trait RestRequest {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError>;
}

pub trait BatchRequest {
    type RestRequest;
}

#[derive(Clone, Debug)]
pub struct Update {
    pub operation: CollectionUpdateOperations,
    pub shard_key: Option<ShardKeySelector>,
}

impl Update {
    pub fn new<T, U>(operation: T, shard_key: Option<U>) -> Self
    where
        T: Into<CollectionUpdateOperations>,
        U: Into<ShardKeySelector>,
    {
        Self {
            operation: operation.into(),
            shard_key: shard_key.map(Into::into),
        }
    }
}

impl<T> From<T> for Update
where
    T: Into<CollectionUpdateOperations>,
{
    fn from(operation: T) -> Self {
        Self {
            operation: operation.into(),
            shard_key: None,
        }
    }
}

//
// REST conversions
//

impl RestRequest for PointInsertOperations {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch.into_update(inference_token).await,
            PointInsertOperations::PointsList(list) => list.into_update(inference_token).await,
        }
    }
}

impl RestRequest for PointsBatch {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self { batch, shard_key } = self;

        let batch = convert_batch(batch, inference_token).await?;
        let op = PointInsertOperationsInternal::PointsBatch(batch);

        Ok(Update::new(op, shard_key))
    }
}

impl RestRequest for PointsList {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self { points, shard_key } = self;

        let points = convert_point_struct(points, InferenceType::Update, inference_token).await?;
        let op = PointInsertOperationsInternal::PointsList(points);

        Ok(Update::new(op, shard_key))
    }
}

impl RestRequest for DeletePointsHelper {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let upd = match self.0 {
            PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
                Update::new(PointOperations::DeletePoints { ids: points }, shard_key)
            }

            PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
                Update::new(PointOperations::DeletePointsByFilter(filter), shard_key)
            }
        };

        Ok(upd)
    }
}

impl RestRequest for UpdateVectors {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self { points, shard_key } = self;

        let points = convert_point_vectors(points, InferenceType::Update, inference_token).await?;
        let op = UpdateVectorsOp { points };

        Ok(Update::new(op, shard_key))
    }
}

impl RestRequest for DeleteVectors {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self {
            points,
            filter,
            vector,
            shard_key,
        } = self;

        let vectors = vector.into_iter().collect();

        let op = match (points, filter) {
            (Some(points), None) => VectorOperations::DeleteVectors(points.into(), vectors),
            (None, Some(filter)) => VectorOperations::DeleteVectorsByFilter(filter, vectors),

            (Some(_), Some(_)) => {
                return Err(StorageError::bad_input("TODO")); // TODO!
            }

            (None, None) => {
                return Err(StorageError::bad_input("TODO")); // TODO!
            }
        };

        Ok(Update::new(op, shard_key))
    }
}

impl RestRequest for SetPayloadHelper {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let op = PayloadOps::SetPayload(set_payload_op_from_rest(self.0));
        Ok(Update::from(op))
    }
}

impl RestRequest for OverwritePayloadHelper {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let op = PayloadOps::OverwritePayload(set_payload_op_from_rest(self.0));
        Ok(Update::from(op))
    }
}

impl RestRequest for DeletePayload {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self {
            keys,
            points,
            filter,
            shard_key,
        } = self;

        let op = DeletePayloadOp {
            keys,
            points,
            filter,
        };

        Ok(Update::new(op, shard_key))
    }
}

impl RestRequest for ClearPayloadHelper {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let upd = match self.0 {
            PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
                Update::new(PayloadOps::ClearPayload { points }, shard_key)
            }

            PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
                Update::new(PayloadOps::ClearPayloadByFilter(filter), shard_key)
            }
        };

        Ok(upd)
    }
}

impl RestRequest for CreateFieldIndex {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self {
            field_name,
            field_schema,
        } = self;

        let op = CreateIndex {
            field_name,
            field_schema,
        };

        Ok(Update::from(op))
    }
}

impl RestRequest for DeleteFieldIndexHelper {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let op = FieldIndexOperations::DeleteIndex(self.0);
        Ok(Update::from(op))
    }
}

#[derive(Clone, Debug)]
pub struct SyncPointsRequest {
    pub points: Vec<PointStruct>,
    pub from_id: Option<ExtendedPointId>,
    pub to_id: Option<ExtendedPointId>,
}

impl RestRequest for SyncPointsRequest {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        let Self {
            points,
            from_id,
            to_id,
        } = self;

        let points = convert_point_struct(points, InferenceType::Update, inference_token).await?;

        let op = PointSyncOperation {
            points,
            from_id,
            to_id,
        };

        Ok(Update::from(op))
    }
}

//
// Strict mode verification for helper types
//

macro_rules! impl_strict_mode_verification {
    ($type:ty) => {
        impl StrictModeVerification for $type {
            fn query_limit(&self) -> Option<usize> {
                self.0.query_limit()
            }

            fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
                self.0.indexed_filter_read()
            }

            fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
                self.0.indexed_filter_write()
            }

            fn request_exact(&self) -> Option<bool> {
                self.0.request_exact()
            }

            fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
                self.0.request_search_params()
            }

            async fn check_custom(
                &self,
                collection: &Collection,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_custom(collection, config).await
            }

            fn check_request_query_limit(
                &self,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_request_query_limit(config)
            }

            fn check_request_exact(
                &self,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_request_exact(config)
            }

            fn check_request_filter(
                &self,
                collection: &Collection,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_request_filter(collection, config)
            }

            async fn check_search_params(
                &self,
                collection: &Collection,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_search_params(collection, config).await
            }

            async fn check_strict_mode(
                &self,
                collection: &Collection,
                config: &StrictModeConfig,
            ) -> Result<(), CollectionError> {
                self.0.check_strict_mode(collection, config).await
            }
        }
    };
}

impl_strict_mode_verification!(DeletePointsHelper);
impl_strict_mode_verification!(SetPayloadHelper);
impl_strict_mode_verification!(OverwritePayloadHelper);
impl_strict_mode_verification!(ClearPayloadHelper);

impl StrictModeVerification for CreateFieldIndex {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

impl StrictModeVerification for DeleteFieldIndexHelper {
    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&segment::types::Filter> {
        None
    }

    fn request_exact(&self) -> Option<bool> {
        None
    }

    fn request_search_params(&self) -> Option<&segment::types::SearchParams> {
        None
    }
}

//
// Batch to REST conversions
//

impl RestRequest for UpdateOperation {
    async fn into_update(self, inference_token: InferenceToken) -> Result<Update, StorageError> {
        match self {
            Self::Upsert(upsert) => upsert.into_update(inference_token).await,
            Self::Delete(delete) => delete.into_update(inference_token).await,
            Self::UpdateVectors(update_vectors) => {
                update_vectors.into_update(inference_token).await
            }
            Self::DeleteVectors(delete_vectors) => {
                delete_vectors.into_update(inference_token).await
            }
            Self::SetPayload(set_payload) => set_payload.into_update(inference_token).await,
            Self::OverwritePayload(overwrite_payload) => {
                overwrite_payload.into_update(inference_token).await
            }
            Self::DeletePayload(delete_payload) => {
                delete_payload.into_update(inference_token).await
            }
            Self::ClearPayload(clear_payload) => clear_payload.into_update(inference_token).await,
        }
    }
}

macro_rules! batch_request {
    ($batch:ty, $rest:ty, $field:ident) => {
        impl RestRequest for $batch {
            async fn into_update(
                self,
                inference_token: InferenceToken,
            ) -> Result<Update, StorageError> {
                <$rest>::from(self).into_update(inference_token).await
            }
        }

        impl BatchRequest for $batch {
            type RestRequest = $rest;
        }

        impl From<$batch> for $rest {
            fn from(batch: $batch) -> Self {
                batch.$field.into()
            }
        }
    };
}

batch_request!(UpsertOperation, PointInsertOperations, upsert);
batch_request!(DeleteOperation, DeletePointsHelper, delete);
batch_request!(UpdateVectorsOperation, UpdateVectors, update_vectors);
batch_request!(DeleteVectorsOperation, DeleteVectors, delete_vectors);

batch_request!(SetPayloadOperation, SetPayloadHelper, set_payload);

batch_request!(
    OverwritePayloadOperation,
    OverwritePayloadHelper,
    overwrite_payload
);

batch_request!(DeletePayloadOperation, DeletePayload, delete_payload);
batch_request!(ClearPayloadOperation, ClearPayloadHelper, clear_payload);

//
// REST-specific conversion helpers
//

fn set_payload_op_from_rest(set_payload: SetPayload) -> SetPayloadOp {
    let SetPayload {
        payload,
        points,
        filter,
        shard_key: _,
        key,
    } = set_payload;

    SetPayloadOp {
        payload,
        points,
        filter,
        key,
    }
}
