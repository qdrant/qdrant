//! Thin wrappers around gRPC service traits that extract `collection_name`
//! from every request and attach it as a [`CollectionName`] extension on the
//! response. The telemetry Tower layer ([`TonicTelemetryService`]) later reads
//! this extension to record per-collection metrics without the individual
//! handlers having to know about telemetry at all.

use api::grpc::qdrant::points_server::Points;
use api::grpc::qdrant::shard_snapshots_server::ShardSnapshots;
use api::grpc::qdrant::snapshots_server::Snapshots;
use api::grpc::qdrant::{
    ClearPayloadPoints, CountPoints, CountResponse, CreateFieldIndexCollection,
    CreateFullSnapshotRequest, CreateShardSnapshotRequest, CreateSnapshotRequest,
    CreateSnapshotResponse, DeleteFieldIndexCollection, DeleteFullSnapshotRequest,
    DeletePayloadPoints, DeletePointVectors, DeletePoints, DeleteShardSnapshotRequest,
    DeleteSnapshotRequest, DeleteSnapshotResponse, DiscoverBatchPoints, DiscoverBatchResponse,
    DiscoverPoints, DiscoverResponse, FacetCounts, FacetResponse, GetPoints, GetResponse,
    ListFullSnapshotsRequest, ListShardSnapshotsRequest, ListSnapshotsRequest,
    ListSnapshotsResponse, PointsOperationResponse, QueryBatchPoints, QueryBatchResponse,
    QueryGroupsResponse, QueryPointGroups, QueryPoints, QueryResponse, RecommendBatchPoints,
    RecommendBatchResponse, RecommendGroupsResponse, RecommendPointGroups, RecommendPoints,
    RecommendResponse, RecoverShardSnapshotRequest, RecoverSnapshotResponse, ScrollPoints,
    ScrollResponse, SearchBatchPoints, SearchBatchResponse, SearchGroupsResponse,
    SearchMatrixOffsetsResponse, SearchMatrixPairsResponse, SearchMatrixPoints, SearchPointGroups,
    SearchPoints, SearchResponse, SetPayloadPoints, UpdateBatchPoints, UpdateBatchResponse,
    UpdatePointVectors, UpsertPoints,
};
use tonic::{Request, Response, Status};

use crate::common::telemetry_ops::requests_telemetry::CollectionName;

/// Wraps a [`Points`] service, attaching `collection_name` to every response.
pub struct PointsTelemetryWrapper<T> {
    inner: T,
}

impl<T> PointsTelemetryWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl<T: Points> Points for PointsTelemetryWrapper<T> {
    async fn upsert(
        &self,
        request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.upsert(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete(
        &self,
        request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn get(&self, request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.get(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn update_vectors(
        &self,
        request: Request<UpdatePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.update_vectors(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete_vectors(
        &self,
        request: Request<DeletePointVectors>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete_vectors(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.set_payload(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn overwrite_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.overwrite_payload(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete_payload(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.clear_payload(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn update_batch(
        &self,
        request: Request<UpdateBatchPoints>,
    ) -> Result<Response<UpdateBatchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.update_batch(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.create_field_index(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete_field_index(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn search(
        &self,
        request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.search(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn search_batch(
        &self,
        request: Request<SearchBatchPoints>,
    ) -> Result<Response<SearchBatchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.search_batch(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn search_groups(
        &self,
        request: Request<SearchPointGroups>,
    ) -> Result<Response<SearchGroupsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.search_groups(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn scroll(
        &self,
        request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.scroll(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn recommend(
        &self,
        request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.recommend(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn recommend_batch(
        &self,
        request: Request<RecommendBatchPoints>,
    ) -> Result<Response<RecommendBatchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.recommend_batch(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn recommend_groups(
        &self,
        request: Request<RecommendPointGroups>,
    ) -> Result<Response<RecommendGroupsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.recommend_groups(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn discover(
        &self,
        request: Request<DiscoverPoints>,
    ) -> Result<Response<DiscoverResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.discover(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn discover_batch(
        &self,
        request: Request<DiscoverBatchPoints>,
    ) -> Result<Response<DiscoverBatchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.discover_batch(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn count(
        &self,
        request: Request<CountPoints>,
    ) -> Result<Response<CountResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.count(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn query(
        &self,
        request: Request<QueryPoints>,
    ) -> Result<Response<QueryResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.query(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn query_batch(
        &self,
        request: Request<QueryBatchPoints>,
    ) -> Result<Response<QueryBatchResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.query_batch(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn query_groups(
        &self,
        request: Request<QueryPointGroups>,
    ) -> Result<Response<QueryGroupsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.query_groups(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn facet(
        &self,
        request: Request<FacetCounts>,
    ) -> Result<Response<FacetResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.facet(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn search_matrix_pairs(
        &self,
        request: Request<SearchMatrixPoints>,
    ) -> Result<Response<SearchMatrixPairsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.search_matrix_pairs(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn search_matrix_offsets(
        &self,
        request: Request<SearchMatrixPoints>,
    ) -> Result<Response<SearchMatrixOffsetsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.search_matrix_offsets(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }
}

/// Wraps a [`Snapshots`] service, attaching `collection_name` to every
/// collection-scoped response. Full-snapshot methods are passed through as-is.
pub struct SnapshotsTelemetryWrapper<T> {
    inner: T,
}

impl<T> SnapshotsTelemetryWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl<T: Snapshots> Snapshots for SnapshotsTelemetryWrapper<T> {
    async fn create(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.create(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn list(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.list(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn create_full(
        &self,
        request: Request<CreateFullSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        self.inner.create_full(request).await
    }

    async fn list_full(
        &self,
        request: Request<ListFullSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        self.inner.list_full(request).await
    }

    async fn delete_full(
        &self,
        request: Request<DeleteFullSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        self.inner.delete_full(request).await
    }
}

/// Wraps a [`ShardSnapshots`] service, attaching `collection_name` to every response.
pub struct ShardSnapshotsTelemetryWrapper<T> {
    inner: T,
}

impl<T> ShardSnapshotsTelemetryWrapper<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl<T: ShardSnapshots> ShardSnapshots for ShardSnapshotsTelemetryWrapper<T> {
    async fn create(
        &self,
        request: Request<CreateShardSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.create(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn list(
        &self,
        request: Request<ListShardSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.list(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn delete(
        &self,
        request: Request<DeleteShardSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.delete(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }

    async fn recover(
        &self,
        request: Request<RecoverShardSnapshotRequest>,
    ) -> Result<Response<RecoverSnapshotResponse>, Status> {
        let cn = request.get_ref().collection_name.clone();
        let mut resp = self.inner.recover(request).await?;
        resp.extensions_mut().insert(CollectionName(cn));
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use api::grpc::qdrant::points_server::Points;
    use api::grpc::qdrant::shard_snapshots_server::ShardSnapshots;
    use api::grpc::qdrant::snapshots_server::Snapshots;
    use api::grpc::qdrant::*;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::common::telemetry_ops::requests_telemetry::CollectionName;

    // Points
    macro_rules! mock_and_test_points {
        ($($method:ident($req:ident) -> $resp:ident),* $(,)?) => {
            struct MockPoints;

            #[tonic::async_trait]
            #[allow(unused_variables)]
            impl Points for MockPoints {
                $(
                    async fn $method(&self, r: Request<$req>) -> Result<Response<$resp>, Status> {
                        Ok(Response::new(Default::default()))
                    }
                )*
            }

            $(
                #[tokio::test]
                async fn $method() {
                    let w = PointsTelemetryWrapper::new(MockPoints);
                    let r = w
                        .$method(Request::new($req {
                            collection_name: stringify!($method).into(),
                            ..Default::default()
                        }))
                        .await
                        .unwrap();
                    assert_eq!(
                        r.extensions().get::<CollectionName>().unwrap().0,
                        stringify!($method),
                    );
                }
            )*
        };
    }

    mock_and_test_points! {
        upsert(UpsertPoints) -> PointsOperationResponse,
        delete(DeletePoints) -> PointsOperationResponse,
        get(GetPoints) -> GetResponse,
        update_vectors(UpdatePointVectors) -> PointsOperationResponse,
        delete_vectors(DeletePointVectors) -> PointsOperationResponse,
        set_payload(SetPayloadPoints) -> PointsOperationResponse,
        overwrite_payload(SetPayloadPoints) -> PointsOperationResponse,
        delete_payload(DeletePayloadPoints) -> PointsOperationResponse,
        clear_payload(ClearPayloadPoints) -> PointsOperationResponse,
        update_batch(UpdateBatchPoints) -> UpdateBatchResponse,
        create_field_index(CreateFieldIndexCollection) -> PointsOperationResponse,
        delete_field_index(DeleteFieldIndexCollection) -> PointsOperationResponse,
        search(SearchPoints) -> SearchResponse,
        search_batch(SearchBatchPoints) -> SearchBatchResponse,
        search_groups(SearchPointGroups) -> SearchGroupsResponse,
        scroll(ScrollPoints) -> ScrollResponse,
        recommend(RecommendPoints) -> RecommendResponse,
        recommend_batch(RecommendBatchPoints) -> RecommendBatchResponse,
        recommend_groups(RecommendPointGroups) -> RecommendGroupsResponse,
        discover(DiscoverPoints) -> DiscoverResponse,
        discover_batch(DiscoverBatchPoints) -> DiscoverBatchResponse,
        count(CountPoints) -> CountResponse,
        query(QueryPoints) -> QueryResponse,
        query_batch(QueryBatchPoints) -> QueryBatchResponse,
        query_groups(QueryPointGroups) -> QueryGroupsResponse,
        facet(FacetCounts) -> FacetResponse,
        search_matrix_pairs(SearchMatrixPoints) -> SearchMatrixPairsResponse,
        search_matrix_offsets(SearchMatrixPoints) -> SearchMatrixOffsetsResponse,
    }

    // Snapshots
    macro_rules! mock_and_test_snapshots {
        (
            with_cn: { $($method:ident($req:ident) -> $resp:ident),* $(,)? }
            passthrough: { $($pt_method:ident($pt_req:ident) -> $pt_resp:ident),* $(,)? }
        ) => {
            struct MockSnapshots;

            #[tonic::async_trait]
            #[allow(unused_variables)]
            impl Snapshots for MockSnapshots {
                $(
                    async fn $method(&self, r: Request<$req>) -> Result<Response<$resp>, Status> {
                        Ok(Response::new(Default::default()))
                    }
                )*
                $(
                    async fn $pt_method(&self, r: Request<$pt_req>) -> Result<Response<$pt_resp>, Status> {
                        Ok(Response::new(Default::default()))
                    }
                )*
            }

            mod snapshots_tests {
                use super::*;

                $(
                    #[tokio::test]
                    #[allow(clippy::needless_update)]
                    async fn $method() {
                        let w = SnapshotsTelemetryWrapper::new(MockSnapshots);
                        let r = w
                            .$method(Request::new($req {
                                collection_name: stringify!($method).into(),
                                ..Default::default()
                            }))
                            .await
                            .unwrap();
                        assert_eq!(
                            r.extensions().get::<CollectionName>().unwrap().0,
                            stringify!($method),
                        );
                    }
                )*

                $(
                    #[tokio::test]
                    async fn $pt_method() {
                        let w = SnapshotsTelemetryWrapper::new(MockSnapshots);
                        let r = w
                            .$pt_method(Request::new($pt_req::default()))
                            .await
                            .unwrap();
                        assert!(
                            r.extensions().get::<CollectionName>().is_none(),
                            "passthrough method should not attach CollectionName",
                        );
                    }
                )*
            }
        };
    }

    mock_and_test_snapshots! {
        with_cn: {
            create(CreateSnapshotRequest) -> CreateSnapshotResponse,
            list(ListSnapshotsRequest) -> ListSnapshotsResponse,
            delete(DeleteSnapshotRequest) -> DeleteSnapshotResponse,
        }
        passthrough: {
            create_full(CreateFullSnapshotRequest) -> CreateSnapshotResponse,
            list_full(ListFullSnapshotsRequest) -> ListSnapshotsResponse,
            delete_full(DeleteFullSnapshotRequest) -> DeleteSnapshotResponse,
        }
    }

    // ShardSnapshots
    macro_rules! mock_and_test_shard_snapshots {
        ($($method:ident($req:ident) -> $resp:ident),* $(,)?) => {
            struct MockShardSnapshots;

            #[tonic::async_trait]
            #[allow(unused_variables)]
            impl ShardSnapshots for MockShardSnapshots {
                $(
                    async fn $method(&self, r: Request<$req>) -> Result<Response<$resp>, Status> {
                        Ok(Response::new(Default::default()))
                    }
                )*
            }

            mod shard_snapshots_tests {
                use super::*;

                $(
                    #[tokio::test]
                    async fn $method() {
                        let w = ShardSnapshotsTelemetryWrapper::new(MockShardSnapshots);
                        let r = w
                            .$method(Request::new($req {
                                collection_name: stringify!($method).into(),
                                ..Default::default()
                            }))
                            .await
                            .unwrap();
                        assert_eq!(
                            r.extensions().get::<CollectionName>().unwrap().0,
                            stringify!($method),
                        );
                    }
                )*
            }
        };
    }

    mock_and_test_shard_snapshots! {
        create(CreateShardSnapshotRequest) -> CreateSnapshotResponse,
        list(ListShardSnapshotsRequest) -> ListSnapshotsResponse,
        delete(DeleteShardSnapshotRequest) -> DeleteSnapshotResponse,
        recover(RecoverShardSnapshotRequest) -> RecoverSnapshotResponse,
    }
}
