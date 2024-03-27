use std::sync::Arc;
use std::time::Duration;

use api::grpc::qdrant::points_internal_server::PointsInternal;
use api::grpc::qdrant::{
    ClearPayloadPointsInternal, CoreSearchBatchPointsInternal, CountPointsInternal, CountResponse,
    CreateFieldIndexCollectionInternal, DeleteFieldIndexCollectionInternal,
    DeletePayloadPointsInternal, DeletePointsInternal, DeleteVectorsInternal, GetPointsInternal,
    GetResponse, PointsOperationResponseInternal, RecommendPointsInternal, RecommendResponse,
    ScrollPointsInternal, ScrollResponse, SearchBatchPointsInternal, SearchBatchResponse,
    SearchPointsInternal, SearchResponse, SetPayloadPointsInternal, SyncPointsInternal,
    UpdateVectorsInternal, UpsertPointsInternal,
};
use storage::content_manager::toc::TableOfContent;
use storage::rbac::access::Access;
use tonic::{Request, Response, Status};

use super::points_common::core_search_list;
use super::validate_and_log;
use crate::tonic::api::points_common::{
    clear_payload, count, create_field_index_internal, delete, delete_field_index_internal,
    delete_payload, delete_vectors, get, overwrite_payload, recommend, scroll, set_payload, sync,
    update_vectors, upsert,
};

/// This API is intended for P2P communication within a distributed deployment.
pub struct PointsInternalService {
    toc: Arc<TableOfContent>,
}

impl PointsInternalService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl PointsInternal for PointsInternalService {
    async fn upsert(
        &self,
        request: Request<UpsertPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let UpsertPointsInternal {
            upsert_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let upsert_points =
            upsert_points.ok_or_else(|| Status::invalid_argument("UpsertPoints is missing"))?;

        upsert(
            self.toc.clone(),
            upsert_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn delete(
        &self,
        request: Request<DeletePointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeletePointsInternal {
            delete_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_points =
            delete_points.ok_or_else(|| Status::invalid_argument("DeletePoints is missing"))?;

        delete(
            self.toc.clone(),
            delete_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn update_vectors(
        &self,
        request: Request<UpdateVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let request = request.into_inner();

        let shard_id = request.shard_id;
        let clock_tag = request.clock_tag;

        let update_point_vectors = request
            .update_vectors
            .ok_or_else(|| Status::invalid_argument("UpdateVectors is missing"))?;

        update_vectors(
            self.toc.clone(),
            update_point_vectors,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn delete_vectors(
        &self,
        request: Request<DeleteVectorsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let request = request.into_inner();

        let shard_id = request.shard_id;
        let clock_tag = request.clock_tag;

        let delete_point_vectors = request
            .delete_vectors
            .ok_or_else(|| Status::invalid_argument("DeleteVectors is missing"))?;

        delete_vectors(
            self.toc.clone(),
            delete_point_vectors,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let SetPayloadPointsInternal {
            set_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let set_payload_points = set_payload_points
            .ok_or_else(|| Status::invalid_argument("SetPayloadPoints is missing"))?;

        set_payload(
            self.toc.clone(),
            set_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn overwrite_payload(
        &self,
        request: Request<SetPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let SetPayloadPointsInternal {
            set_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let set_payload_points = set_payload_points
            .ok_or_else(|| Status::invalid_argument("SetPayloadPoints is missing"))?;

        overwrite_payload(
            self.toc.clone(),
            set_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeletePayloadPointsInternal {
            delete_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_payload_points = delete_payload_points
            .ok_or_else(|| Status::invalid_argument("DeletePayloadPoints is missing"))?;

        delete_payload(
            self.toc.clone(),
            delete_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let ClearPayloadPointsInternal {
            clear_payload_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let clear_payload_points = clear_payload_points
            .ok_or_else(|| Status::invalid_argument("ClearPayloadPoints is missing"))?;

        clear_payload(
            self.toc.clone(),
            clear_payload_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let CreateFieldIndexCollectionInternal {
            create_field_index_collection,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let create_field_index_collection = create_field_index_collection
            .ok_or_else(|| Status::invalid_argument("CreateFieldIndexCollection is missing"))?;

        create_field_index_internal(
            self.toc.clone(),
            create_field_index_collection,
            clock_tag.map(Into::into),
            shard_id,
        )
        .await
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollectionInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let DeleteFieldIndexCollectionInternal {
            delete_field_index_collection,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let delete_field_index_collection = delete_field_index_collection
            .ok_or_else(|| Status::invalid_argument("DeleteFieldIndexCollection is missing"))?;

        delete_field_index_internal(
            self.toc.clone(),
            delete_field_index_collection,
            clock_tag.map(Into::into),
            shard_id,
        )
        .await
    }

    async fn search(
        &self,
        _request: Request<SearchPointsInternal>,
    ) -> Result<Response<SearchResponse>, Status> {
        return Err(Status::unimplemented(
            "search API was deprecated and removed, use core_search_batch instead. \
        Please make sure versions of your cluster is consistent",
        ));
    }

    async fn search_batch(
        &self,
        _request: Request<SearchBatchPointsInternal>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        return Err(Status::unimplemented(
            "search_batch API was deprecated and removed, use core_search_batch instead. \
        Please make sure versions of your cluster is consistent",
        ));
    }

    async fn core_search_batch(
        &self,
        request: Request<CoreSearchBatchPointsInternal>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        validate_and_log(request.get_ref());

        let CoreSearchBatchPointsInternal {
            collection_name,
            search_points,
            shard_id,
            timeout,
        } = request.into_inner();

        let timeout = timeout.map(Duration::from_secs);

        // Individual `read_consistency` values are ignored by `core_search_batch`...
        //
        // search_points
        //     .iter_mut()
        //     .for_each(|search_points| search_points.read_consistency = None);

        core_search_list(
            self.toc.as_ref(),
            collection_name,
            search_points,
            None, // *Has* to be `None`!
            shard_id,
            Access::full(),
            timeout,
        )
        .await
    }

    async fn recommend(
        &self,
        request: Request<RecommendPointsInternal>,
    ) -> Result<Response<RecommendResponse>, Status> {
        validate_and_log(request.get_ref());

        let RecommendPointsInternal {
            recommend_points,
            ..  // shard_id - is not used in internal API,
            // because it is transformed into regular search requests on the first node
        } = request.into_inner();

        let mut recommend_points = recommend_points
            .ok_or_else(|| Status::invalid_argument("RecommendPoints is missing"))?;

        recommend_points.read_consistency = None; // *Have* to be `None`!

        recommend(self.toc.as_ref(), recommend_points, Access::full()).await
    }

    async fn scroll(
        &self,
        request: Request<ScrollPointsInternal>,
    ) -> Result<Response<ScrollResponse>, Status> {
        validate_and_log(request.get_ref());

        let ScrollPointsInternal {
            scroll_points,
            shard_id,
        } = request.into_inner();

        let mut scroll_points =
            scroll_points.ok_or_else(|| Status::invalid_argument("ScrollPoints is missing"))?;

        scroll_points.read_consistency = None; // *Have* to be `None`!

        scroll(self.toc.as_ref(), scroll_points, shard_id, Access::full()).await
    }

    async fn get(
        &self,
        request: Request<GetPointsInternal>,
    ) -> Result<Response<GetResponse>, Status> {
        validate_and_log(request.get_ref());

        let GetPointsInternal {
            get_points,
            shard_id,
        } = request.into_inner();

        let mut get_points =
            get_points.ok_or_else(|| Status::invalid_argument("GetPoints is missing"))?;

        get_points.read_consistency = None; // *Have* to be `None`!

        get(self.toc.as_ref(), get_points, shard_id, Access::full()).await
    }

    async fn count(
        &self,
        request: Request<CountPointsInternal>,
    ) -> Result<Response<CountResponse>, Status> {
        validate_and_log(request.get_ref());

        let CountPointsInternal {
            count_points,
            shard_id,
        } = request.into_inner();

        let count_points =
            count_points.ok_or_else(|| Status::invalid_argument("CountPoints is missing"))?;
        count(self.toc.as_ref(), count_points, shard_id, Access::full()).await
    }

    async fn sync(
        &self,
        request: Request<SyncPointsInternal>,
    ) -> Result<Response<PointsOperationResponseInternal>, Status> {
        validate_and_log(request.get_ref());

        let SyncPointsInternal {
            sync_points,
            shard_id,
            clock_tag,
        } = request.into_inner();

        let sync_points =
            sync_points.ok_or_else(|| Status::invalid_argument("SyncPoints is missing"))?;
        sync(
            self.toc.clone(),
            sync_points,
            clock_tag.map(Into::into),
            shard_id,
            Access::full(),
        )
        .await
    }
}
