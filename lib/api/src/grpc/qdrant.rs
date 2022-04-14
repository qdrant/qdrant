#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoRequest {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionDescription {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoResponse {
    #[prost(message, optional, tag="1")]
    pub result: ::core::option::Option<CollectionInfo>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsResponse {
    #[prost(message, repeated, tag="1")]
    pub collections: ::prost::alloc::vec::Vec<CollectionDescription>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizerStatus {
    #[prost(bool, tag="1")]
    pub ok: bool,
    #[prost(string, tag="2")]
    pub error: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HnswConfigDiff {
    ///
    ///Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    #[prost(uint64, optional, tag="1")]
    pub m: ::core::option::Option<u64>,
    ///
    ///Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    #[prost(uint64, optional, tag="2")]
    pub ef_construct: ::core::option::Option<u64>,
    ///
    ///Minimal amount of points for additional payload-based indexing.
    ///If payload chunk is smaller than `full_scan_threshold` additional indexing won't be used -
    ///in this case full-scan search should be preferred by query planner and additional indexing is not required.
    #[prost(uint64, optional, tag="3")]
    pub full_scan_threshold: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WalConfigDiff {
    /// Size of a single WAL block file
    #[prost(uint64, optional, tag="1")]
    pub wal_capacity_mb: ::core::option::Option<u64>,
    /// Number of segments to create in advance
    #[prost(uint64, optional, tag="2")]
    pub wal_segments_ahead: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizersConfigDiff {
    ///
    ///The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    #[prost(double, optional, tag="1")]
    pub deleted_threshold: ::core::option::Option<f64>,
    ///
    ///The minimal number of vectors in a segment, required to perform segment optimization
    #[prost(uint64, optional, tag="2")]
    pub vacuum_min_vector_number: ::core::option::Option<u64>,
    ///
    ///Target amount of segments optimizer will try to keep.
    ///Real amount of segments may vary depending on multiple parameters:
    ///
    ///- Amount of stored points.
    ///- Current write RPS.
    ///
    ///It is recommended to select default number of segments as a factor of the number of search threads,
    ///so that each segment would be handled evenly by one of the threads.
    #[prost(uint64, optional, tag="3")]
    pub default_segment_number: ::core::option::Option<u64>,
    ///
    ///Do not create segments larger this number of points.
    ///Large segments might require disproportionately long indexation times,
    ///therefore it makes sense to limit the size of segments.
    ///
    ///If indexation speed have more priority for your - make this parameter lower.
    ///If search speed is more important - make this parameter higher.
    #[prost(uint64, optional, tag="4")]
    pub max_segment_size: ::core::option::Option<u64>,
    ///
    ///Maximum number of vectors to store in-memory per segment.
    ///Segments larger than this threshold will be stored as read-only memmaped file.
    #[prost(uint64, optional, tag="5")]
    pub memmap_threshold: ::core::option::Option<u64>,
    ///
    ///Maximum number of vectors allowed for plain index.
    ///Default value based on <https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>
    #[prost(uint64, optional, tag="6")]
    pub indexing_threshold: ::core::option::Option<u64>,
    ///
    ///Starting from this amount of vectors per-segment the engine will start building index for payload.
    #[prost(uint64, optional, tag="7")]
    pub payload_indexing_threshold: ::core::option::Option<u64>,
    ///
    ///Interval between forced flushes.
    #[prost(uint64, optional, tag="8")]
    pub flush_interval_sec: ::core::option::Option<u64>,
    ///
    ///Max number of threads, which can be used for optimization. If 0 - `NUM_CPU - 1` will be used
    #[prost(uint64, optional, tag="9")]
    pub max_optimization_threads: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCollection {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Size of the vectors
    #[prost(uint64, tag="2")]
    pub vector_size: u64,
    /// Distance function used for comparing vectors
    #[prost(enumeration="Distance", tag="3")]
    pub distance: i32,
    /// Configuration of vector index
    #[prost(message, optional, tag="4")]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Configuration of the Write-Ahead-Log
    #[prost(message, optional, tag="5")]
    pub wal_config: ::core::option::Option<WalConfigDiff>,
    /// Configuration of the optimizers
    #[prost(message, optional, tag="6")]
    pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Number of shards in the collection, default = 1
    #[prost(uint32, optional, tag="7")]
    pub shard_number: ::core::option::Option<u32>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag="8")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateCollection {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// New configuration parameters for the collection
    #[prost(message, optional, tag="2")]
    pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag="3")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteCollection {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag="2")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionOperationResponse {
    /// if operation made changes
    #[prost(bool, tag="1")]
    pub result: bool,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionParams {
    /// Size of the vectors
    #[prost(uint64, tag="1")]
    pub vector_size: u64,
    /// Distance function used for comparing vectors
    #[prost(enumeration="Distance", tag="2")]
    pub distance: i32,
    /// Number of shards in collection
    #[prost(uint32, tag="3")]
    pub shard_number: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionConfig {
    /// Collection parameters
    #[prost(message, optional, tag="1")]
    pub params: ::core::option::Option<CollectionParams>,
    /// Configuration of vector index
    #[prost(message, optional, tag="2")]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Configuration of the optimizers
    #[prost(message, optional, tag="3")]
    pub optimizer_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Configuration of the Write-Ahead-Log
    #[prost(message, optional, tag="4")]
    pub wal_config: ::core::option::Option<WalConfigDiff>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadSchemaInfo {
    /// Field data type
    #[prost(enumeration="PayloadSchemaType", tag="1")]
    pub data_type: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionInfo {
    /// operating condition of the collection
    #[prost(enumeration="CollectionStatus", tag="1")]
    pub status: i32,
    /// status of collection optimizers
    #[prost(message, optional, tag="2")]
    pub optimizer_status: ::core::option::Option<OptimizerStatus>,
    /// number of vectors in the collection
    #[prost(uint64, tag="3")]
    pub vectors_count: u64,
    /// Number of independent segments
    #[prost(uint64, tag="4")]
    pub segments_count: u64,
    /// Used disk space
    #[prost(uint64, tag="5")]
    pub disk_data_size: u64,
    /// Used RAM (not implemented)
    #[prost(uint64, tag="6")]
    pub ram_data_size: u64,
    /// Configuration
    #[prost(message, optional, tag="7")]
    pub config: ::core::option::Option<CollectionConfig>,
    /// Collection data types
    #[prost(map="string, message", tag="8")]
    pub payload_schema: ::std::collections::HashMap<::prost::alloc::string::String, PayloadSchemaInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeAliases {
    /// List of actions
    #[prost(message, repeated, tag="1")]
    pub actions: ::prost::alloc::vec::Vec<AliasOperations>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag="2")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasOperations {
    #[prost(oneof="alias_operations::Action", tags="1, 2, 3")]
    pub action: ::core::option::Option<alias_operations::Action>,
}
/// Nested message and enum types in `AliasOperations`.
pub mod alias_operations {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        #[prost(message, tag="1")]
        CreateAlias(super::CreateAlias),
        #[prost(message, tag="2")]
        RenameAlias(super::RenameAlias),
        #[prost(message, tag="3")]
        DeleteAlias(super::DeleteAlias),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateAlias {
    /// Name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// New name of the alias
    #[prost(string, tag="2")]
    pub alias_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameAlias {
    /// Name of the alias to rename
    #[prost(string, tag="1")]
    pub old_alias_name: ::prost::alloc::string::String,
    /// Name of the alias
    #[prost(string, tag="2")]
    pub new_alias_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteAlias {
    /// Name of the alias
    #[prost(string, tag="1")]
    pub alias_name: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Distance {
    UnknownDistance = 0,
    Cosine = 1,
    Euclid = 2,
    Dot = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CollectionStatus {
    UnknownCollectionStatus = 0,
    /// All segments are ready
    Green = 1,
    /// Optimization in process
    Yellow = 2,
    /// Something went wrong
    Red = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PayloadSchemaType {
    UnknownType = 0,
    Keyword = 1,
    Integer = 2,
    Float = 3,
    Geo = 4,
}
/// Generated client implementations.
pub mod collections_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct CollectionsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CollectionsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CollectionsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CollectionsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CollectionsClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        ///
        ///Get detailed information about specified existing collection
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCollectionInfoRequest>,
        ) -> Result<tonic::Response<super::GetCollectionInfoResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Get list name of all existing collections
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<super::ListCollectionsRequest>,
        ) -> Result<tonic::Response<super::ListCollectionsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Collections/List");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Create new collection with given parameters
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Collections/Create",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Update parameters of the existing collection
        pub async fn update(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Collections/Update",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Drop collection and all associated data
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Collections/Delete",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Update Aliases of the existing collection
        pub async fn update_aliases(
            &mut self,
            request: impl tonic::IntoRequest<super::ChangeAliases>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Collections/UpdateAliases",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod collections_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with CollectionsServer.
    #[async_trait]
    pub trait Collections: Send + Sync + 'static {
        ///
        ///Get detailed information about specified existing collection
        async fn get(
            &self,
            request: tonic::Request<super::GetCollectionInfoRequest>,
        ) -> Result<tonic::Response<super::GetCollectionInfoResponse>, tonic::Status>;
        ///
        ///Get list name of all existing collections
        async fn list(
            &self,
            request: tonic::Request<super::ListCollectionsRequest>,
        ) -> Result<tonic::Response<super::ListCollectionsResponse>, tonic::Status>;
        ///
        ///Create new collection with given parameters
        async fn create(
            &self,
            request: tonic::Request<super::CreateCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>;
        ///
        ///Update parameters of the existing collection
        async fn update(
            &self,
            request: tonic::Request<super::UpdateCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>;
        ///
        ///Drop collection and all associated data
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteCollection>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>;
        ///
        ///Update Aliases of the existing collection
        async fn update_aliases(
            &self,
            request: tonic::Request<super::ChangeAliases>,
        ) -> Result<tonic::Response<super::CollectionOperationResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct CollectionsServer<T: Collections> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Collections> CollectionsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CollectionsServer<T>
    where
        T: Collections,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.Collections/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::GetCollectionInfoRequest>
                    for GetSvc<T> {
                        type Response = super::GetCollectionInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetCollectionInfoRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::ListCollectionsRequest>
                    for ListSvc<T> {
                        type Response = super::ListCollectionsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListCollectionsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/Create" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::CreateCollection>
                    for CreateSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateCollection>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/Update" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::UpdateCollection>
                    for UpdateSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateCollection>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).update(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::DeleteCollection>
                    for DeleteSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteCollection>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/UpdateAliases" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateAliasesSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::ChangeAliases>
                    for UpdateAliasesSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ChangeAliases>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).update_aliases(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateAliasesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Collections> Clone for CollectionsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Collections> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Collections> tonic::transport::NamedService for CollectionsServer<T> {
        const NAME: &'static str = "qdrant.Collections";
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoRequestInternal {
    #[prost(message, optional, tag="1")]
    pub get_collection_info_request: ::core::option::Option<GetCollectionInfoRequest>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
/// Generated client implementations.
pub mod collections_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct CollectionsInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CollectionsInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CollectionsInternalClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CollectionsInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CollectionsInternalClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCollectionInfoRequestInternal>,
        ) -> Result<tonic::Response<super::GetCollectionInfoResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.CollectionsInternal/Get",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod collections_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with CollectionsInternalServer.
    #[async_trait]
    pub trait CollectionsInternal: Send + Sync + 'static {
        async fn get(
            &self,
            request: tonic::Request<super::GetCollectionInfoRequestInternal>,
        ) -> Result<tonic::Response<super::GetCollectionInfoResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct CollectionsInternalServer<T: CollectionsInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CollectionsInternal> CollectionsInternalServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CollectionsInternalServer<T>
    where
        T: CollectionsInternal,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.CollectionsInternal/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: CollectionsInternal>(pub Arc<T>);
                    impl<
                        T: CollectionsInternal,
                    > tonic::server::UnaryService<
                        super::GetCollectionInfoRequestInternal,
                    > for GetSvc<T> {
                        type Response = super::GetCollectionInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::GetCollectionInfoRequestInternal,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: CollectionsInternal> Clone for CollectionsInternalServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: CollectionsInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CollectionsInternal> tonic::transport::NamedService
    for CollectionsInternalServer<T> {
        const NAME: &'static str = "qdrant.CollectionsInternal";
    }
}
// ---------------------------------------------
// ------------- Point Id Requests -------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointId {
    #[prost(oneof="point_id::PointIdOptions", tags="1, 2")]
    pub point_id_options: ::core::option::Option<point_id::PointIdOptions>,
}
/// Nested message and enum types in `PointId`.
pub mod point_id {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PointIdOptions {
        /// Numerical ID of the point
        #[prost(uint64, tag="1")]
        Num(u64),
        /// UUID
        #[prost(string, tag="2")]
        Uuid(::prost::alloc::string::String),
    }
}
// ---------------------------------------------
// ---------------- RPC Requests ---------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    #[prost(message, repeated, tag="3")]
    pub points: ::prost::alloc::vec::Vec<PointStruct>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// Affected points
    #[prost(message, optional, tag="3")]
    pub points: ::core::option::Option<PointsSelector>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// List of points to retrieve
    #[prost(message, repeated, tag="2")]
    pub ids: ::prost::alloc::vec::Vec<PointId>,
    /// Return point vector with the result.
    #[prost(bool, optional, tag="3")]
    pub with_vector: ::core::option::Option<bool>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag="4")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPayloadPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// New payload values
    #[prost(map="string, message", tag="3")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, ::prost_types::Value>,
    /// List of point to modify
    #[prost(message, repeated, tag="4")]
    pub points: ::prost::alloc::vec::Vec<PointId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePayloadPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// List of keys to delete
    #[prost(string, repeated, tag="3")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Affected points
    #[prost(message, repeated, tag="4")]
    pub points: ::prost::alloc::vec::Vec<PointId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearPayloadPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// Affected points
    #[prost(message, optional, tag="3")]
    pub points: ::core::option::Option<PointsSelector>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateFieldIndexCollection {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// Field name to index
    #[prost(string, tag="3")]
    pub field_name: ::prost::alloc::string::String,
    /// Field type.
    #[prost(enumeration="FieldType", optional, tag="4")]
    pub field_type: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteFieldIndexCollection {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag="2")]
    pub wait: ::core::option::Option<bool>,
    /// Field name to delete
    #[prost(string, tag="3")]
    pub field_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadIncludeSelector {
    /// List of payload keys to include into result
    #[prost(string, repeated, tag="1")]
    pub include: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadExcludeSelector {
    /// List of payload keys to exclude from the result
    #[prost(string, repeated, tag="1")]
    pub exclude: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WithPayloadSelector {
    #[prost(oneof="with_payload_selector::SelectorOptions", tags="1, 2, 3")]
    pub selector_options: ::core::option::Option<with_payload_selector::SelectorOptions>,
}
/// Nested message and enum types in `WithPayloadSelector`.
pub mod with_payload_selector {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum SelectorOptions {
        /// If `true` - return all payload, if `false` - none
        #[prost(bool, tag="1")]
        Enable(bool),
        #[prost(message, tag="2")]
        Include(super::PayloadIncludeSelector),
        #[prost(message, tag="3")]
        Exclude(super::PayloadExcludeSelector),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchParams {
    ///
    ///Params relevant to HNSW index. Size of the beam in a beam-search.
    ///Larger the value - more accurate the result, more time required for search.
    #[prost(uint64, optional, tag="1")]
    pub hnsw_ef: ::core::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// vector
    #[prost(float, repeated, tag="2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag="3")]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint64, tag="4")]
    pub top: u64,
    /// Return point vector with the result.
    #[prost(bool, optional, tag="5")]
    pub with_vector: ::core::option::Option<bool>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag="6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag="7")]
    pub params: ::core::option::Option<SearchParams>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollPoints {
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag="2")]
    pub filter: ::core::option::Option<Filter>,
    /// Start with this ID
    #[prost(message, optional, tag="3")]
    pub offset: ::core::option::Option<PointId>,
    /// Max number of result
    #[prost(uint32, optional, tag="4")]
    pub limit: ::core::option::Option<u32>,
    /// Return point vector with the result.
    #[prost(bool, optional, tag="5")]
    pub with_vector: ::core::option::Option<bool>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag="6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendPoints {
    /// name of the collection
    #[prost(string, tag="1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Look for vectors closest to those
    #[prost(message, repeated, tag="2")]
    pub positive: ::prost::alloc::vec::Vec<PointId>,
    /// Try to avoid vectors like this
    #[prost(message, repeated, tag="3")]
    pub negative: ::prost::alloc::vec::Vec<PointId>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag="4")]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint64, tag="5")]
    pub top: u64,
    /// Return point vector with the result.
    #[prost(bool, optional, tag="6")]
    pub with_vector: ::core::option::Option<bool>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag="7")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag="8")]
    pub params: ::core::option::Option<SearchParams>,
}
// ---------------------------------------------
// ---------------- RPC Response ---------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsOperationResponse {
    #[prost(message, optional, tag="1")]
    pub result: ::core::option::Option<UpdateResult>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateResult {
    /// Number of operation
    #[prost(uint64, tag="1")]
    pub operation_id: u64,
    /// Operation status
    #[prost(enumeration="UpdateStatus", tag="2")]
    pub status: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScoredPoint {
    /// Point id
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<PointId>,
    /// Payload
    #[prost(map="string, message", tag="2")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, ::prost_types::Value>,
    /// Similarity score
    #[prost(float, tag="3")]
    pub score: f32,
    /// Vector
    #[prost(float, repeated, tag="4")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    /// Last update operation applied to this point
    #[prost(uint64, tag="5")]
    pub version: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, repeated, tag="1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollResponse {
    /// Use this offset for the next query
    #[prost(message, optional, tag="1")]
    pub next_page_offset: ::core::option::Option<PointId>,
    #[prost(message, repeated, tag="2")]
    pub result: ::prost::alloc::vec::Vec<RetrievedPoint>,
    /// Time spent to process
    #[prost(double, tag="3")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetrievedPoint {
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<PointId>,
    #[prost(map="string, message", tag="2")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, ::prost_types::Value>,
    #[prost(float, repeated, tag="3")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(message, repeated, tag="1")]
    pub result: ::prost::alloc::vec::Vec<RetrievedPoint>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendResponse {
    #[prost(message, repeated, tag="1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Time spent to process
    #[prost(double, tag="2")]
    pub time: f64,
}
// ---------------------------------------------
// ------------- Filter Conditions -------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    /// At least one of those conditions should match
    #[prost(message, repeated, tag="1")]
    pub should: ::prost::alloc::vec::Vec<Condition>,
    /// All conditions must match
    #[prost(message, repeated, tag="2")]
    pub must: ::prost::alloc::vec::Vec<Condition>,
    /// All conditions must NOT match
    #[prost(message, repeated, tag="3")]
    pub must_not: ::prost::alloc::vec::Vec<Condition>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Condition {
    #[prost(oneof="condition::ConditionOneOf", tags="1, 2, 3, 4")]
    pub condition_one_of: ::core::option::Option<condition::ConditionOneOf>,
}
/// Nested message and enum types in `Condition`.
pub mod condition {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ConditionOneOf {
        #[prost(message, tag="1")]
        Field(super::FieldCondition),
        #[prost(message, tag="2")]
        IsEmpty(super::IsEmptyCondition),
        #[prost(message, tag="3")]
        HasId(super::HasIdCondition),
        #[prost(message, tag="4")]
        Filter(super::Filter),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsEmptyCondition {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HasIdCondition {
    #[prost(message, repeated, tag="1")]
    pub has_id: ::prost::alloc::vec::Vec<PointId>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldCondition {
    #[prost(string, tag="1")]
    pub key: ::prost::alloc::string::String,
    /// Check if point has field with a given value
    #[prost(message, optional, tag="2")]
    pub r#match: ::core::option::Option<Match>,
    /// Check if points value lies in a given range
    #[prost(message, optional, tag="3")]
    pub range: ::core::option::Option<Range>,
    /// Check if points geo location lies in a given area
    #[prost(message, optional, tag="4")]
    pub geo_bounding_box: ::core::option::Option<GeoBoundingBox>,
    /// Check if geo point is within a given radius
    #[prost(message, optional, tag="5")]
    pub geo_radius: ::core::option::Option<GeoRadius>,
    /// Check number of values for a specific field
    #[prost(message, optional, tag="6")]
    pub values_count: ::core::option::Option<ValuesCount>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Match {
    #[prost(oneof="r#match::MatchValue", tags="1, 2, 3")]
    pub match_value: ::core::option::Option<r#match::MatchValue>,
}
/// Nested message and enum types in `Match`.
pub mod r#match {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum MatchValue {
        /// Match string keyword
        #[prost(string, tag="1")]
        Keyword(::prost::alloc::string::String),
        /// Match integer
        #[prost(int64, tag="2")]
        Integer(i64),
        /// Match boolean
        #[prost(bool, tag="3")]
        Boolean(bool),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Range {
    #[prost(double, optional, tag="1")]
    pub lt: ::core::option::Option<f64>,
    #[prost(double, optional, tag="2")]
    pub gt: ::core::option::Option<f64>,
    #[prost(double, optional, tag="3")]
    pub gte: ::core::option::Option<f64>,
    #[prost(double, optional, tag="4")]
    pub lte: ::core::option::Option<f64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoBoundingBox {
    /// north-west corner
    #[prost(message, optional, tag="1")]
    pub top_left: ::core::option::Option<GeoPoint>,
    /// south-east corner
    #[prost(message, optional, tag="2")]
    pub bottom_right: ::core::option::Option<GeoPoint>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoRadius {
    /// Center of the circle
    #[prost(message, optional, tag="1")]
    pub center: ::core::option::Option<GeoPoint>,
    /// In meters
    #[prost(float, tag="2")]
    pub radius: f32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValuesCount {
    #[prost(uint64, optional, tag="1")]
    pub lt: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="2")]
    pub gt: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="3")]
    pub gte: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag="4")]
    pub lte: ::core::option::Option<u64>,
}
// ---------------------------------------------
// -------------- Points Selector --------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsSelector {
    #[prost(oneof="points_selector::PointsSelectorOneOf", tags="1, 2")]
    pub points_selector_one_of: ::core::option::Option<points_selector::PointsSelectorOneOf>,
}
/// Nested message and enum types in `PointsSelector`.
pub mod points_selector {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PointsSelectorOneOf {
        #[prost(message, tag="1")]
        Points(super::PointsIdsList),
        #[prost(message, tag="2")]
        Filter(super::Filter),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsIdsList {
    #[prost(message, repeated, tag="1")]
    pub ids: ::prost::alloc::vec::Vec<PointId>,
}
// ---------------------------------------------
// ------------------- Point -------------------
// ---------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointStruct {
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<PointId>,
    #[prost(float, repeated, tag="2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    #[prost(map="string, message", tag="3")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, ::prost_types::Value>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoPoint {
    #[prost(double, tag="1")]
    pub lon: f64,
    #[prost(double, tag="2")]
    pub lat: f64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FieldType {
    Keyword = 0,
    Integer = 1,
    Float = 2,
    Geo = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UpdateStatus {
    UnknownUpdateStatus = 0,
    /// Update is received, but not processed yet
    Acknowledged = 1,
    /// Update is applied and ready for search
    Completed = 2,
}
/// Generated client implementations.
pub mod points_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct PointsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PointsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PointsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PointsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            PointsClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        ///
        ///Perform insert + updates on points. If point with given ID already exists - it will be overwritten.
        pub async fn upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::UpsertPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Upsert");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Delete points
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Delete");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Retrieve points
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetPoints>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Get");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Set payload for points
        pub async fn set_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/SetPayload");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Delete specified key payload for points
        pub async fn delete_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Points/DeletePayload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Remove all payload for specified points
        pub async fn clear_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::ClearPayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Points/ClearPayload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Create index for field in collection
        pub async fn create_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateFieldIndexCollection>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Points/CreateFieldIndex",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Delete field index for collection
        pub async fn delete_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteFieldIndexCollection>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Points/DeleteFieldIndex",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Retrieve closest points based on vector similarity and given filtering conditions
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchPoints>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Search");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Iterate over all or filtered points points
        pub async fn scroll(
            &mut self,
            request: impl tonic::IntoRequest<super::ScrollPoints>,
        ) -> Result<tonic::Response<super::ScrollResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Scroll");
            self.inner.unary(request.into_request(), path, codec).await
        }
        ///
        ///Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        pub async fn recommend(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendPoints>,
        ) -> Result<tonic::Response<super::RecommendResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Recommend");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod points_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with PointsServer.
    #[async_trait]
    pub trait Points: Send + Sync + 'static {
        ///
        ///Perform insert + updates on points. If point with given ID already exists - it will be overwritten.
        async fn upsert(
            &self,
            request: tonic::Request<super::UpsertPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Delete points
        async fn delete(
            &self,
            request: tonic::Request<super::DeletePoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Retrieve points
        async fn get(
            &self,
            request: tonic::Request<super::GetPoints>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status>;
        ///
        ///Set payload for points
        async fn set_payload(
            &self,
            request: tonic::Request<super::SetPayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Delete specified key payload for points
        async fn delete_payload(
            &self,
            request: tonic::Request<super::DeletePayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Remove all payload for specified points
        async fn clear_payload(
            &self,
            request: tonic::Request<super::ClearPayloadPoints>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Create index for field in collection
        async fn create_field_index(
            &self,
            request: tonic::Request<super::CreateFieldIndexCollection>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Delete field index for collection
        async fn delete_field_index(
            &self,
            request: tonic::Request<super::DeleteFieldIndexCollection>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        ///
        ///Retrieve closest points based on vector similarity and given filtering conditions
        async fn search(
            &self,
            request: tonic::Request<super::SearchPoints>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        ///
        ///Iterate over all or filtered points points
        async fn scroll(
            &self,
            request: tonic::Request<super::ScrollPoints>,
        ) -> Result<tonic::Response<super::ScrollResponse>, tonic::Status>;
        ///
        ///Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        async fn recommend(
            &self,
            request: tonic::Request<super::RecommendPoints>,
        ) -> Result<tonic::Response<super::RecommendResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct PointsServer<T: Points> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Points> PointsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for PointsServer<T>
    where
        T: Points,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.Points/Upsert" => {
                    #[allow(non_camel_case_types)]
                    struct UpsertSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::UpsertPoints>
                    for UpsertSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpsertPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).upsert(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::DeletePoints>
                    for DeleteSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::GetPoints>
                    for GetSvc<T> {
                        type Response = super::GetResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/SetPayload" => {
                    #[allow(non_camel_case_types)]
                    struct SetPayloadSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::SetPayloadPoints>
                    for SetPayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetPayloadPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_payload(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/DeletePayload" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePayloadSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::DeletePayloadPoints>
                    for DeletePayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePayloadPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_payload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeletePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/ClearPayload" => {
                    #[allow(non_camel_case_types)]
                    struct ClearPayloadSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::ClearPayloadPoints>
                    for ClearPayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClearPayloadPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).clear_payload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ClearPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/CreateFieldIndex" => {
                    #[allow(non_camel_case_types)]
                    struct CreateFieldIndexSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::CreateFieldIndexCollection>
                    for CreateFieldIndexSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateFieldIndexCollection>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_field_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/DeleteFieldIndex" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteFieldIndexSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::DeleteFieldIndexCollection>
                    for DeleteFieldIndexSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteFieldIndexCollection>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_field_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::SearchPoints>
                    for SearchSvc<T> {
                        type Response = super::SearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Scroll" => {
                    #[allow(non_camel_case_types)]
                    struct ScrollSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::ScrollPoints>
                    for ScrollSvc<T> {
                        type Response = super::ScrollResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ScrollPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).scroll(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ScrollSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Recommend" => {
                    #[allow(non_camel_case_types)]
                    struct RecommendSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::RecommendPoints>
                    for RecommendSvc<T> {
                        type Response = super::RecommendResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecommendPoints>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).recommend(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Points> Clone for PointsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Points> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Points> tonic::transport::NamedService for PointsServer<T> {
        const NAME: &'static str = "qdrant.Points";
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertPointsInternal {
    #[prost(message, optional, tag="1")]
    pub upsert_points: ::core::option::Option<UpsertPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePointsInternal {
    #[prost(message, optional, tag="1")]
    pub delete_points: ::core::option::Option<DeletePoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPayloadPointsInternal {
    #[prost(message, optional, tag="1")]
    pub set_payload_points: ::core::option::Option<SetPayloadPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePayloadPointsInternal {
    #[prost(message, optional, tag="1")]
    pub delete_payload_points: ::core::option::Option<DeletePayloadPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearPayloadPointsInternal {
    #[prost(message, optional, tag="1")]
    pub clear_payload_points: ::core::option::Option<ClearPayloadPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateFieldIndexCollectionInternal {
    #[prost(message, optional, tag="1")]
    pub create_field_index_collection: ::core::option::Option<CreateFieldIndexCollection>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteFieldIndexCollectionInternal {
    #[prost(message, optional, tag="1")]
    pub delete_field_index_collection: ::core::option::Option<DeleteFieldIndexCollection>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchPointsInternal {
    #[prost(message, optional, tag="1")]
    pub search_points: ::core::option::Option<SearchPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollPointsInternal {
    #[prost(message, optional, tag="1")]
    pub scroll_points: ::core::option::Option<ScrollPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendPointsInternal {
    #[prost(message, optional, tag="1")]
    pub recommend_points: ::core::option::Option<RecommendPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPointsInternal {
    #[prost(message, optional, tag="1")]
    pub get_points: ::core::option::Option<GetPoints>,
    #[prost(uint32, tag="2")]
    pub shard_id: u32,
}
/// Generated client implementations.
pub mod points_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct PointsInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PointsInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> PointsInternalClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PointsInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            PointsInternalClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::UpsertPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Upsert",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Delete",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/SetPayload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/DeletePayload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn clear_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::ClearPayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/ClearPayload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateFieldIndexCollectionInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/CreateFieldIndex",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteFieldIndexCollectionInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/DeleteFieldIndex",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchPointsInternal>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Search",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn scroll(
            &mut self,
            request: impl tonic::IntoRequest<super::ScrollPointsInternal>,
        ) -> Result<tonic::Response<super::ScrollResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Scroll",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn recommend(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendPointsInternal>,
        ) -> Result<tonic::Response<super::RecommendResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Recommend",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetPointsInternal>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.PointsInternal/Get",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod points_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with PointsInternalServer.
    #[async_trait]
    pub trait PointsInternal: Send + Sync + 'static {
        async fn upsert(
            &self,
            request: tonic::Request<super::UpsertPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn delete(
            &self,
            request: tonic::Request<super::DeletePointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn set_payload(
            &self,
            request: tonic::Request<super::SetPayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn delete_payload(
            &self,
            request: tonic::Request<super::DeletePayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn clear_payload(
            &self,
            request: tonic::Request<super::ClearPayloadPointsInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn create_field_index(
            &self,
            request: tonic::Request<super::CreateFieldIndexCollectionInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn delete_field_index(
            &self,
            request: tonic::Request<super::DeleteFieldIndexCollectionInternal>,
        ) -> Result<tonic::Response<super::PointsOperationResponse>, tonic::Status>;
        async fn search(
            &self,
            request: tonic::Request<super::SearchPointsInternal>,
        ) -> Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        async fn scroll(
            &self,
            request: tonic::Request<super::ScrollPointsInternal>,
        ) -> Result<tonic::Response<super::ScrollResponse>, tonic::Status>;
        async fn recommend(
            &self,
            request: tonic::Request<super::RecommendPointsInternal>,
        ) -> Result<tonic::Response<super::RecommendResponse>, tonic::Status>;
        async fn get(
            &self,
            request: tonic::Request<super::GetPointsInternal>,
        ) -> Result<tonic::Response<super::GetResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct PointsInternalServer<T: PointsInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: PointsInternal> PointsInternalServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for PointsInternalServer<T>
    where
        T: PointsInternal,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.PointsInternal/Upsert" => {
                    #[allow(non_camel_case_types)]
                    struct UpsertSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::UpsertPointsInternal>
                    for UpsertSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpsertPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).upsert(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::DeletePointsInternal>
                    for DeleteSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/SetPayload" => {
                    #[allow(non_camel_case_types)]
                    struct SetPayloadSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::SetPayloadPointsInternal>
                    for SetPayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetPayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_payload(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/DeletePayload" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePayloadSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::DeletePayloadPointsInternal>
                    for DeletePayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_payload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeletePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/ClearPayload" => {
                    #[allow(non_camel_case_types)]
                    struct ClearPayloadSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::ClearPayloadPointsInternal>
                    for ClearPayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClearPayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).clear_payload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ClearPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/CreateFieldIndex" => {
                    #[allow(non_camel_case_types)]
                    struct CreateFieldIndexSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<
                        super::CreateFieldIndexCollectionInternal,
                    > for CreateFieldIndexSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::CreateFieldIndexCollectionInternal,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_field_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/DeleteFieldIndex" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteFieldIndexSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<
                        super::DeleteFieldIndexCollectionInternal,
                    > for DeleteFieldIndexSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::DeleteFieldIndexCollectionInternal,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_field_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Search" => {
                    #[allow(non_camel_case_types)]
                    struct SearchSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::SearchPointsInternal>
                    for SearchSvc<T> {
                        type Response = super::SearchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).search(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Scroll" => {
                    #[allow(non_camel_case_types)]
                    struct ScrollSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::ScrollPointsInternal>
                    for ScrollSvc<T> {
                        type Response = super::ScrollResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ScrollPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).scroll(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ScrollSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Recommend" => {
                    #[allow(non_camel_case_types)]
                    struct RecommendSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::RecommendPointsInternal>
                    for RecommendSvc<T> {
                        type Response = super::RecommendResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecommendPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).recommend(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Get" => {
                    #[allow(non_camel_case_types)]
                    struct GetSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::GetPointsInternal>
                    for GetSvc<T> {
                        type Response = super::GetResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetPointsInternal>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: PointsInternal> Clone for PointsInternalServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: PointsInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: PointsInternal> tonic::transport::NamedService for PointsInternalServer<T> {
        const NAME: &'static str = "qdrant.PointsInternal";
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftMessage {
    #[prost(bytes="vec", tag="1")]
    pub message: ::prost::alloc::vec::Vec<u8>,
}
/// Generated client implementations.
pub mod raft_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RaftClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            RaftClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        ///
        ///Send Raft message to another peer
        pub async fn send(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/qdrant.Raft/Send");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod raft_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with RaftServer.
    #[async_trait]
    pub trait Raft: Send + Sync + 'static {
        ///
        ///Send Raft message to another peer
        async fn send(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RaftServer<T: Raft> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Raft> RaftServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for RaftServer<T>
    where
        T: Raft,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.Raft/Send" => {
                    #[allow(non_camel_case_types)]
                    struct SendSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::RaftMessage>
                    for SendSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RaftMessage>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).send(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Raft> Clone for RaftServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Raft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Raft> tonic::transport::NamedService for RaftServer<T> {
        const NAME: &'static str = "qdrant.Raft";
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckReply {
    #[prost(string, tag="1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub version: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod qdrant_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct QdrantClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl QdrantClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> QdrantClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> QdrantClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            QdrantClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn health_check(
            &mut self,
            request: impl tonic::IntoRequest<super::HealthCheckRequest>,
        ) -> Result<tonic::Response<super::HealthCheckReply>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/qdrant.Qdrant/HealthCheck",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod qdrant_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with QdrantServer.
    #[async_trait]
    pub trait Qdrant: Send + Sync + 'static {
        async fn health_check(
            &self,
            request: tonic::Request<super::HealthCheckRequest>,
        ) -> Result<tonic::Response<super::HealthCheckReply>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct QdrantServer<T: Qdrant> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Qdrant> QdrantServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for QdrantServer<T>
    where
        T: Qdrant,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.Qdrant/HealthCheck" => {
                    #[allow(non_camel_case_types)]
                    struct HealthCheckSvc<T: Qdrant>(pub Arc<T>);
                    impl<
                        T: Qdrant,
                    > tonic::server::UnaryService<super::HealthCheckRequest>
                    for HealthCheckSvc<T> {
                        type Response = super::HealthCheckReply;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HealthCheckRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).health_check(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HealthCheckSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Qdrant> Clone for QdrantServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Qdrant> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Qdrant> tonic::transport::NamedService for QdrantServer<T> {
        const NAME: &'static str = "qdrant.Qdrant";
    }
}
