#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorParams {
    /// Size of the vectors
    #[prost(uint64, tag = "1")]
    #[validate(range(min = 1, max = 65536))]
    pub size: u64,
    /// Distance function used for comparing vectors
    #[prost(enumeration = "Distance", tag = "2")]
    pub distance: i32,
    /// Configuration of vector HNSW graph. If omitted - the collection configuration will be used
    #[prost(message, optional, tag = "3")]
    #[validate]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Configuration of vector quantization config. If omitted - the collection configuration will be used
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub quantization_config: ::core::option::Option<QuantizationConfig>,
    /// If true - serve vectors from disk. If set to false, the vectors will be loaded in RAM.
    #[prost(bool, optional, tag = "5")]
    pub on_disk: ::core::option::Option<bool>,
    /// Data type of the vectors
    #[prost(enumeration = "Datatype", optional, tag = "6")]
    pub datatype: ::core::option::Option<i32>,
    /// Configuration for multi-vector search
    #[prost(message, optional, tag = "7")]
    pub multivector_config: ::core::option::Option<MultiVectorConfig>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorParamsDiff {
    /// Update params for HNSW index. If empty object - it will be unset
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Update quantization params. If none - it is left unchanged.
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub quantization_config: ::core::option::Option<QuantizationConfigDiff>,
    /// If true - serve vectors from disk. If set to false, the vectors will be loaded in RAM.
    #[prost(bool, optional, tag = "3")]
    pub on_disk: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorParamsMap {
    #[prost(map = "string, message", tag = "1")]
    #[validate]
    pub map: ::std::collections::HashMap<::prost::alloc::string::String, VectorParams>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorParamsDiffMap {
    #[prost(map = "string, message", tag = "1")]
    #[validate]
    pub map: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        VectorParamsDiff,
    >,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorsConfig {
    #[prost(oneof = "vectors_config::Config", tags = "1, 2")]
    #[validate]
    pub config: ::core::option::Option<vectors_config::Config>,
}
/// Nested message and enum types in `VectorsConfig`.
pub mod vectors_config {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Config {
        #[prost(message, tag = "1")]
        Params(super::VectorParams),
        #[prost(message, tag = "2")]
        ParamsMap(super::VectorParamsMap),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorsConfigDiff {
    #[prost(oneof = "vectors_config_diff::Config", tags = "1, 2")]
    #[validate]
    pub config: ::core::option::Option<vectors_config_diff::Config>,
}
/// Nested message and enum types in `VectorsConfigDiff`.
pub mod vectors_config_diff {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Config {
        #[prost(message, tag = "1")]
        Params(super::VectorParamsDiff),
        #[prost(message, tag = "2")]
        ParamsMap(super::VectorParamsDiffMap),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SparseVectorParams {
    /// Configuration of sparse index
    #[prost(message, optional, tag = "1")]
    pub index: ::core::option::Option<SparseIndexConfig>,
    /// If set - apply modifier to the vector values
    #[prost(enumeration = "Modifier", optional, tag = "2")]
    pub modifier: ::core::option::Option<i32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SparseVectorConfig {
    #[prost(map = "string, message", tag = "1")]
    pub map: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        SparseVectorParams,
    >,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiVectorConfig {
    /// Comparator for multi-vector search
    #[prost(enumeration = "MultiVectorComparator", tag = "1")]
    pub comparator: i32,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionExistsRequest {
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionExists {
    #[prost(bool, tag = "1")]
    pub exists: bool,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionExistsResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<CollectionExists>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsRequest {}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionDescription {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<CollectionInfo>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionsResponse {
    #[prost(message, repeated, tag = "1")]
    pub collections: ::prost::alloc::vec::Vec<CollectionDescription>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizerStatus {
    #[prost(bool, tag = "1")]
    pub ok: bool,
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HnswConfigDiff {
    ///
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    #[prost(uint64, optional, tag = "1")]
    pub m: ::core::option::Option<u64>,
    ///
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build the index.
    #[prost(uint64, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_4")]
    pub ef_construct: ::core::option::Option<u64>,
    ///
    /// Minimal size (in KiloBytes) of vectors for additional payload-based indexing.
    /// If the payload chunk is smaller than `full_scan_threshold` additional indexing won't be used -
    /// in this case full-scan search should be preferred by query planner and additional indexing is not required.
    /// Note: 1 Kb = 1 vector of size 256
    #[prost(uint64, optional, tag = "3")]
    pub full_scan_threshold: ::core::option::Option<u64>,
    ///
    /// Number of parallel threads used for background index building.
    /// If 0 - automatically select from 8 to 16.
    /// Best to keep between 8 and 16 to prevent likelihood of building broken/inefficient HNSW graphs.
    /// On small CPUs, less threads are used.
    #[prost(uint64, optional, tag = "4")]
    pub max_indexing_threads: ::core::option::Option<u64>,
    ///
    /// Store HNSW index on disk. If set to false, the index will be stored in RAM.
    #[prost(bool, optional, tag = "5")]
    pub on_disk: ::core::option::Option<bool>,
    ///
    /// Number of additional payload-aware links per node in the index graph. If not set - regular M parameter will be used.
    #[prost(uint64, optional, tag = "6")]
    pub payload_m: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SparseIndexConfig {
    ///
    /// Prefer a full scan search upto (excluding) this number of vectors.
    /// Note: this is number of vectors, not KiloBytes.
    #[prost(uint64, optional, tag = "1")]
    pub full_scan_threshold: ::core::option::Option<u64>,
    ///
    /// Store inverted index on disk. If set to false, the index will be stored in RAM.
    #[prost(bool, optional, tag = "2")]
    pub on_disk: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WalConfigDiff {
    /// Size of a single WAL block file
    #[prost(uint64, optional, tag = "1")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub wal_capacity_mb: ::core::option::Option<u64>,
    /// Number of segments to create in advance
    #[prost(uint64, optional, tag = "2")]
    pub wal_segments_ahead: ::core::option::Option<u64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizersConfigDiff {
    ///
    /// The minimal fraction of deleted vectors in a segment, required to perform segment optimization
    #[prost(double, optional, tag = "1")]
    #[validate(custom = "crate::grpc::validate::validate_f64_range_1")]
    pub deleted_threshold: ::core::option::Option<f64>,
    ///
    /// The minimal number of vectors in a segment, required to perform segment optimization
    #[prost(uint64, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_100")]
    pub vacuum_min_vector_number: ::core::option::Option<u64>,
    ///
    /// Target amount of segments the optimizer will try to keep.
    /// Real amount of segments may vary depending on multiple parameters:
    ///
    /// - Amount of stored points.
    /// - Current write RPS.
    ///
    /// It is recommended to select the default number of segments as a factor of the number of search threads,
    /// so that each segment would be handled evenly by one of the threads.
    #[prost(uint64, optional, tag = "3")]
    pub default_segment_number: ::core::option::Option<u64>,
    ///
    /// Do not create segments larger this size (in kilobytes).
    /// Large segments might require disproportionately long indexation times,
    /// therefore it makes sense to limit the size of segments.
    ///
    /// If indexing speed is more important - make this parameter lower.
    /// If search speed is more important - make this parameter higher.
    /// Note: 1Kb = 1 vector of size 256
    /// If not set, will be automatically selected considering the number of available CPUs.
    #[prost(uint64, optional, tag = "4")]
    pub max_segment_size: ::core::option::Option<u64>,
    ///
    /// Maximum size (in kilobytes) of vectors to store in-memory per segment.
    /// Segments larger than this threshold will be stored as read-only memmaped file.
    ///
    /// Memmap storage is disabled by default, to enable it, set this threshold to a reasonable value.
    ///
    /// To disable memmap storage, set this to `0`.
    ///
    /// Note: 1Kb = 1 vector of size 256
    #[prost(uint64, optional, tag = "5")]
    pub memmap_threshold: ::core::option::Option<u64>,
    ///
    /// Maximum size (in kilobytes) of vectors allowed for plain index, exceeding this threshold will enable vector indexing
    ///
    /// Default value is 20,000, based on <<https://github.com/google-research/google-research/blob/master/scann/docs/algorithms.md>.>
    ///
    /// To disable vector indexing, set to `0`.
    ///
    /// Note: 1kB = 1 vector of size 256.
    #[prost(uint64, optional, tag = "6")]
    pub indexing_threshold: ::core::option::Option<u64>,
    ///
    /// Interval between forced flushes.
    #[prost(uint64, optional, tag = "7")]
    pub flush_interval_sec: ::core::option::Option<u64>,
    ///
    /// Max number of threads (jobs) for running optimizations per shard.
    /// Note: each optimization job will also use `max_indexing_threads` threads by itself for index building.
    /// If null - have no limit and choose dynamically to saturate CPU.
    /// If 0 - no optimization threads, optimizations will be disabled.
    #[prost(uint64, optional, tag = "8")]
    pub max_optimization_threads: ::core::option::Option<u64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarQuantization {
    /// Type of quantization
    #[prost(enumeration = "QuantizationType", tag = "1")]
    pub r#type: i32,
    /// Number of bits to use for quantization
    #[prost(float, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_f32_range_min_0_5_max_1")]
    pub quantile: ::core::option::Option<f32>,
    /// If true - quantized vectors always will be stored in RAM, ignoring the config of main storage
    #[prost(bool, optional, tag = "3")]
    pub always_ram: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProductQuantization {
    /// Compression ratio
    #[prost(enumeration = "CompressionRatio", tag = "1")]
    pub compression: i32,
    /// If true - quantized vectors always will be stored in RAM, ignoring the config of main storage
    #[prost(bool, optional, tag = "2")]
    pub always_ram: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryQuantization {
    /// If true - quantized vectors always will be stored in RAM, ignoring the config of main storage
    #[prost(bool, optional, tag = "1")]
    pub always_ram: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuantizationConfig {
    #[prost(oneof = "quantization_config::Quantization", tags = "1, 2, 3")]
    #[validate]
    pub quantization: ::core::option::Option<quantization_config::Quantization>,
}
/// Nested message and enum types in `QuantizationConfig`.
pub mod quantization_config {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Quantization {
        #[prost(message, tag = "1")]
        Scalar(super::ScalarQuantization),
        #[prost(message, tag = "2")]
        Product(super::ProductQuantization),
        #[prost(message, tag = "3")]
        Binary(super::BinaryQuantization),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Disabled {}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuantizationConfigDiff {
    #[prost(oneof = "quantization_config_diff::Quantization", tags = "1, 2, 3, 4")]
    #[validate]
    pub quantization: ::core::option::Option<quantization_config_diff::Quantization>,
}
/// Nested message and enum types in `QuantizationConfigDiff`.
pub mod quantization_config_diff {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Quantization {
        #[prost(message, tag = "1")]
        Scalar(super::ScalarQuantization),
        #[prost(message, tag = "2")]
        Product(super::ProductQuantization),
        #[prost(message, tag = "3")]
        Disabled(super::Disabled),
        #[prost(message, tag = "4")]
        Binary(super::BinaryQuantization),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCollection {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(
        length(min = 1, max = 255),
        custom = "common::validation::validate_collection_name"
    )]
    pub collection_name: ::prost::alloc::string::String,
    /// Configuration of vector index
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Configuration of the Write-Ahead-Log
    #[prost(message, optional, tag = "5")]
    #[validate]
    pub wal_config: ::core::option::Option<WalConfigDiff>,
    /// Configuration of the optimizers
    #[prost(message, optional, tag = "6")]
    #[validate]
    pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Number of shards in the collection, default is 1 for standalone, otherwise equal to the number of nodes. Minimum is 1
    #[prost(uint32, optional, tag = "7")]
    pub shard_number: ::core::option::Option<u32>,
    /// If true - point's payload will not be stored in memory
    #[prost(bool, optional, tag = "8")]
    pub on_disk_payload: ::core::option::Option<bool>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "9")]
    pub timeout: ::core::option::Option<u64>,
    /// Configuration for vectors
    #[prost(message, optional, tag = "10")]
    #[validate]
    pub vectors_config: ::core::option::Option<VectorsConfig>,
    /// Number of replicas of each shard that network tries to maintain, default = 1
    #[prost(uint32, optional, tag = "11")]
    pub replication_factor: ::core::option::Option<u32>,
    /// How many replicas should apply the operation for us to consider it successful, default = 1
    #[prost(uint32, optional, tag = "12")]
    pub write_consistency_factor: ::core::option::Option<u32>,
    /// Specify name of the other collection to copy data from
    #[prost(string, optional, tag = "13")]
    pub init_from_collection: ::core::option::Option<::prost::alloc::string::String>,
    /// Quantization configuration of vector
    #[prost(message, optional, tag = "14")]
    #[validate]
    pub quantization_config: ::core::option::Option<QuantizationConfig>,
    /// Sharding method
    #[prost(enumeration = "ShardingMethod", optional, tag = "15")]
    pub sharding_method: ::core::option::Option<i32>,
    /// Configuration for sparse vectors
    #[prost(message, optional, tag = "16")]
    pub sparse_vectors_config: ::core::option::Option<SparseVectorConfig>,
    /// Properties/metadata of the collection
    #[prost(string, optional, tag = "17")]
    pub comment: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateCollection {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// New configuration parameters for the collection. This operation is blocking, it will only proceed once all current optimizations are complete
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub optimizers_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Wait timeout for operation commit in seconds if blocking, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "3")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// New configuration parameters for the collection
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub params: ::core::option::Option<CollectionParamsDiff>,
    /// New HNSW parameters for the collection index
    #[prost(message, optional, tag = "5")]
    #[validate]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// New vector parameters
    #[prost(message, optional, tag = "6")]
    #[validate]
    pub vectors_config: ::core::option::Option<VectorsConfigDiff>,
    /// Quantization configuration of vector
    #[prost(message, optional, tag = "7")]
    #[validate]
    pub quantization_config: ::core::option::Option<QuantizationConfigDiff>,
    /// New sparse vector parameters
    #[prost(message, optional, tag = "8")]
    pub sparse_vectors_config: ::core::option::Option<SparseVectorConfig>,
    /// New comments for the collection
    #[prost(string, optional, tag = "9")]
    pub comment: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteCollection {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionOperationResponse {
    /// if operation made changes
    #[prost(bool, tag = "1")]
    pub result: bool,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionParams {
    /// Number of shards in collection
    #[prost(uint32, tag = "3")]
    pub shard_number: u32,
    /// If true - point's payload will not be stored in memory
    #[prost(bool, tag = "4")]
    pub on_disk_payload: bool,
    /// Configuration for vectors
    #[prost(message, optional, tag = "5")]
    #[validate]
    pub vectors_config: ::core::option::Option<VectorsConfig>,
    /// Number of replicas of each shard that network tries to maintain
    #[prost(uint32, optional, tag = "6")]
    pub replication_factor: ::core::option::Option<u32>,
    /// How many replicas should apply the operation for us to consider it successful
    #[prost(uint32, optional, tag = "7")]
    pub write_consistency_factor: ::core::option::Option<u32>,
    /// Fan-out every read request to these many additional remote nodes (and return first available response)
    #[prost(uint32, optional, tag = "8")]
    pub read_fan_out_factor: ::core::option::Option<u32>,
    /// Sharding method
    #[prost(enumeration = "ShardingMethod", optional, tag = "9")]
    pub sharding_method: ::core::option::Option<i32>,
    /// Configuration for sparse vectors
    #[prost(message, optional, tag = "10")]
    pub sparse_vectors_config: ::core::option::Option<SparseVectorConfig>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionParamsDiff {
    /// Number of replicas of each shard that network tries to maintain
    #[prost(uint32, optional, tag = "1")]
    pub replication_factor: ::core::option::Option<u32>,
    /// How many replicas should apply the operation for us to consider it successful
    #[prost(uint32, optional, tag = "2")]
    pub write_consistency_factor: ::core::option::Option<u32>,
    /// If true - point's payload will not be stored in memory
    #[prost(bool, optional, tag = "3")]
    pub on_disk_payload: ::core::option::Option<bool>,
    /// Fan-out every read request to these many additional remote nodes (and return first available response)
    #[prost(uint32, optional, tag = "4")]
    pub read_fan_out_factor: ::core::option::Option<u32>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionConfig {
    /// Collection parameters
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub params: ::core::option::Option<CollectionParams>,
    /// Configuration of vector index
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub hnsw_config: ::core::option::Option<HnswConfigDiff>,
    /// Configuration of the optimizers
    #[prost(message, optional, tag = "3")]
    pub optimizer_config: ::core::option::Option<OptimizersConfigDiff>,
    /// Configuration of the Write-Ahead-Log
    #[prost(message, optional, tag = "4")]
    pub wal_config: ::core::option::Option<WalConfigDiff>,
    /// Configuration of the vector quantization
    #[prost(message, optional, tag = "5")]
    #[validate]
    pub quantization_config: ::core::option::Option<QuantizationConfig>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TextIndexParams {
    /// Tokenizer type
    #[prost(enumeration = "TokenizerType", tag = "1")]
    pub tokenizer: i32,
    /// If true - all tokens will be lowercase
    #[prost(bool, optional, tag = "2")]
    pub lowercase: ::core::option::Option<bool>,
    /// Minimal token length
    #[prost(uint64, optional, tag = "3")]
    pub min_token_len: ::core::option::Option<u64>,
    /// Maximal token length
    #[prost(uint64, optional, tag = "4")]
    pub max_token_len: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntegerIndexParams {
    /// If true - support direct lookups.
    #[prost(bool, tag = "1")]
    pub lookup: bool,
    /// If true - support ranges filters.
    #[prost(bool, tag = "2")]
    pub range: bool,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadIndexParams {
    #[prost(oneof = "payload_index_params::IndexParams", tags = "1, 2")]
    pub index_params: ::core::option::Option<payload_index_params::IndexParams>,
}
/// Nested message and enum types in `PayloadIndexParams`.
pub mod payload_index_params {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum IndexParams {
        /// Parameters for text index
        #[prost(message, tag = "1")]
        TextIndexParams(super::TextIndexParams),
        /// Parameters for integer index
        #[prost(message, tag = "2")]
        IntegerIndexParams(super::IntegerIndexParams),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadSchemaInfo {
    /// Field data type
    #[prost(enumeration = "PayloadSchemaType", tag = "1")]
    pub data_type: i32,
    /// Field index parameters
    #[prost(message, optional, tag = "2")]
    pub params: ::core::option::Option<PayloadIndexParams>,
    /// Number of points indexed within this field indexed
    #[prost(uint64, optional, tag = "3")]
    pub points: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionInfo {
    /// operating condition of the collection
    #[prost(enumeration = "CollectionStatus", tag = "1")]
    pub status: i32,
    /// status of collection optimizers
    #[prost(message, optional, tag = "2")]
    pub optimizer_status: ::core::option::Option<OptimizerStatus>,
    /// Approximate number of vectors in the collection
    #[prost(uint64, optional, tag = "3")]
    pub vectors_count: ::core::option::Option<u64>,
    /// Number of independent segments
    #[prost(uint64, tag = "4")]
    pub segments_count: u64,
    /// Configuration
    #[prost(message, optional, tag = "7")]
    pub config: ::core::option::Option<CollectionConfig>,
    /// Collection data types
    #[prost(map = "string, message", tag = "8")]
    pub payload_schema: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        PayloadSchemaInfo,
    >,
    /// Approximate number of points in the collection
    #[prost(uint64, optional, tag = "9")]
    pub points_count: ::core::option::Option<u64>,
    /// Approximate number of indexed vectors in the collection.
    #[prost(uint64, optional, tag = "10")]
    pub indexed_vectors_count: ::core::option::Option<u64>,
    /// properties or metadata of the collection.
    #[prost(string, optional, tag = "11")]
    pub comment: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ChangeAliases {
    /// List of actions
    #[prost(message, repeated, tag = "1")]
    pub actions: ::prost::alloc::vec::Vec<AliasOperations>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasOperations {
    #[prost(oneof = "alias_operations::Action", tags = "1, 2, 3")]
    pub action: ::core::option::Option<alias_operations::Action>,
}
/// Nested message and enum types in `AliasOperations`.
pub mod alias_operations {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        #[prost(message, tag = "1")]
        CreateAlias(super::CreateAlias),
        #[prost(message, tag = "2")]
        RenameAlias(super::RenameAlias),
        #[prost(message, tag = "3")]
        DeleteAlias(super::DeleteAlias),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateAlias {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    /// New name of the alias
    #[prost(string, tag = "2")]
    pub alias_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RenameAlias {
    /// Name of the alias to rename
    #[prost(string, tag = "1")]
    pub old_alias_name: ::prost::alloc::string::String,
    /// Name of the alias
    #[prost(string, tag = "2")]
    pub new_alias_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteAlias {
    /// Name of the alias
    #[prost(string, tag = "1")]
    pub alias_name: ::prost::alloc::string::String,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListAliasesRequest {}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListCollectionAliasesRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasDescription {
    /// Name of the alias
    #[prost(string, tag = "1")]
    pub alias_name: ::prost::alloc::string::String,
    /// Name of the collection
    #[prost(string, tag = "2")]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListAliasesResponse {
    #[prost(message, repeated, tag = "1")]
    pub aliases: ::prost::alloc::vec::Vec<AliasDescription>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionClusterInfoRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardKey {
    #[prost(oneof = "shard_key::Key", tags = "1, 2")]
    pub key: ::core::option::Option<shard_key::Key>,
}
/// Nested message and enum types in `ShardKey`.
pub mod shard_key {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Key {
        /// String key
        #[prost(string, tag = "1")]
        Keyword(::prost::alloc::string::String),
        /// Number key
        #[prost(uint64, tag = "2")]
        Number(u64),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalShardInfo {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    /// Number of points in the shard
    #[prost(uint64, tag = "2")]
    pub points_count: u64,
    /// Is replica active
    #[prost(enumeration = "ReplicaState", tag = "3")]
    pub state: i32,
    /// User-defined shard key
    #[prost(message, optional, tag = "4")]
    pub shard_key: ::core::option::Option<ShardKey>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteShardInfo {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    /// Remote peer id
    #[prost(uint64, tag = "2")]
    pub peer_id: u64,
    /// Is replica active
    #[prost(enumeration = "ReplicaState", tag = "3")]
    pub state: i32,
    /// User-defined shard key
    #[prost(message, optional, tag = "4")]
    pub shard_key: ::core::option::Option<ShardKey>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardTransferInfo {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub from: u64,
    #[prost(uint64, tag = "3")]
    pub to: u64,
    /// If `true` transfer is a synchronization of a replicas; If `false` transfer is a moving of a shard from one peer to another
    #[prost(bool, tag = "4")]
    pub sync: bool,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionClusterInfoResponse {
    /// ID of this peer
    #[prost(uint64, tag = "1")]
    pub peer_id: u64,
    /// Total number of shards
    #[prost(uint64, tag = "2")]
    pub shard_count: u64,
    /// Local shards
    #[prost(message, repeated, tag = "3")]
    pub local_shards: ::prost::alloc::vec::Vec<LocalShardInfo>,
    /// Remote shards
    #[prost(message, repeated, tag = "4")]
    pub remote_shards: ::prost::alloc::vec::Vec<RemoteShardInfo>,
    /// Shard transfers
    #[prost(message, repeated, tag = "5")]
    pub shard_transfers: ::prost::alloc::vec::Vec<ShardTransferInfo>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MoveShard {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub from_peer_id: u64,
    #[prost(uint64, tag = "3")]
    pub to_peer_id: u64,
    #[prost(enumeration = "ShardTransferMethod", optional, tag = "4")]
    pub method: ::core::option::Option<i32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReplicateShard {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub from_peer_id: u64,
    #[prost(uint64, tag = "3")]
    pub to_peer_id: u64,
    #[prost(enumeration = "ShardTransferMethod", optional, tag = "4")]
    pub method: ::core::option::Option<i32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbortShardTransfer {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub from_peer_id: u64,
    #[prost(uint64, tag = "3")]
    pub to_peer_id: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RestartTransfer {
    /// Local shard id
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub from_peer_id: u64,
    #[prost(uint64, tag = "3")]
    pub to_peer_id: u64,
    #[prost(enumeration = "ShardTransferMethod", tag = "4")]
    pub method: i32,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Replica {
    #[prost(uint32, tag = "1")]
    pub shard_id: u32,
    #[prost(uint64, tag = "2")]
    pub peer_id: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardKey {
    /// User-defined shard key
    #[prost(message, optional, tag = "1")]
    pub shard_key: ::core::option::Option<ShardKey>,
    /// Number of shards to create per shard key
    #[prost(uint32, optional, tag = "2")]
    pub shards_number: ::core::option::Option<u32>,
    /// Number of replicas of each shard to create
    #[prost(uint32, optional, tag = "3")]
    pub replication_factor: ::core::option::Option<u32>,
    /// List of peer ids, allowed to create shards. If empty - all peers are allowed
    #[prost(uint64, repeated, tag = "4")]
    pub placement: ::prost::alloc::vec::Vec<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardKey {
    /// Shard key to delete
    #[prost(message, optional, tag = "1")]
    pub shard_key: ::core::option::Option<ShardKey>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateCollectionClusterSetupRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "6")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    #[prost(
        oneof = "update_collection_cluster_setup_request::Operation",
        tags = "2, 3, 4, 5, 7, 8, 9"
    )]
    #[validate]
    pub operation: ::core::option::Option<
        update_collection_cluster_setup_request::Operation,
    >,
}
/// Nested message and enum types in `UpdateCollectionClusterSetupRequest`.
pub mod update_collection_cluster_setup_request {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "2")]
        MoveShard(super::MoveShard),
        #[prost(message, tag = "3")]
        ReplicateShard(super::ReplicateShard),
        #[prost(message, tag = "4")]
        AbortTransfer(super::AbortShardTransfer),
        #[prost(message, tag = "5")]
        DropReplica(super::Replica),
        #[prost(message, tag = "7")]
        CreateShardKey(super::CreateShardKey),
        #[prost(message, tag = "8")]
        DeleteShardKey(super::DeleteShardKey),
        #[prost(message, tag = "9")]
        RestartTransfer(super::RestartTransfer),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateCollectionClusterSetupResponse {
    #[prost(bool, tag = "1")]
    pub result: bool,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardKeyRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Request to create shard key
    #[prost(message, optional, tag = "2")]
    pub request: ::core::option::Option<CreateShardKey>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "3")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardKeyRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Request to delete shard key
    #[prost(message, optional, tag = "2")]
    pub request: ::core::option::Option<DeleteShardKey>,
    /// Wait timeout for operation commit in seconds, if not specified - default value will be supplied
    #[prost(uint64, optional, tag = "3")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardKeyResponse {
    #[prost(bool, tag = "1")]
    pub result: bool,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardKeyResponse {
    #[prost(bool, tag = "1")]
    pub result: bool,
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Datatype {
    Default = 0,
    Float32 = 1,
    Uint8 = 2,
    Float16 = 3,
}
impl Datatype {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Datatype::Default => "Default",
            Datatype::Float32 => "Float32",
            Datatype::Uint8 => "Uint8",
            Datatype::Float16 => "Float16",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Default" => Some(Self::Default),
            "Float32" => Some(Self::Float32),
            "Uint8" => Some(Self::Uint8),
            "Float16" => Some(Self::Float16),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Modifier {
    None = 0,
    /// Apply Inverse Document Frequency
    Idf = 1,
}
impl Modifier {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Modifier::None => "None",
            Modifier::Idf => "Idf",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "None" => Some(Self::None),
            "Idf" => Some(Self::Idf),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MultiVectorComparator {
    MaxSim = 0,
}
impl MultiVectorComparator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            MultiVectorComparator::MaxSim => "MaxSim",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MaxSim" => Some(Self::MaxSim),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Distance {
    UnknownDistance = 0,
    Cosine = 1,
    Euclid = 2,
    Dot = 3,
    Manhattan = 4,
}
impl Distance {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Distance::UnknownDistance => "UnknownDistance",
            Distance::Cosine => "Cosine",
            Distance::Euclid => "Euclid",
            Distance::Dot => "Dot",
            Distance::Manhattan => "Manhattan",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UnknownDistance" => Some(Self::UnknownDistance),
            "Cosine" => Some(Self::Cosine),
            "Euclid" => Some(Self::Euclid),
            "Dot" => Some(Self::Dot),
            "Manhattan" => Some(Self::Manhattan),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
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
    /// Optimization is pending
    Grey = 4,
}
impl CollectionStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CollectionStatus::UnknownCollectionStatus => "UnknownCollectionStatus",
            CollectionStatus::Green => "Green",
            CollectionStatus::Yellow => "Yellow",
            CollectionStatus::Red => "Red",
            CollectionStatus::Grey => "Grey",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UnknownCollectionStatus" => Some(Self::UnknownCollectionStatus),
            "Green" => Some(Self::Green),
            "Yellow" => Some(Self::Yellow),
            "Red" => Some(Self::Red),
            "Grey" => Some(Self::Grey),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PayloadSchemaType {
    UnknownType = 0,
    Keyword = 1,
    Integer = 2,
    Float = 3,
    Geo = 4,
    Text = 5,
    Bool = 6,
    Datetime = 7,
}
impl PayloadSchemaType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PayloadSchemaType::UnknownType => "UnknownType",
            PayloadSchemaType::Keyword => "Keyword",
            PayloadSchemaType::Integer => "Integer",
            PayloadSchemaType::Float => "Float",
            PayloadSchemaType::Geo => "Geo",
            PayloadSchemaType::Text => "Text",
            PayloadSchemaType::Bool => "Bool",
            PayloadSchemaType::Datetime => "Datetime",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UnknownType" => Some(Self::UnknownType),
            "Keyword" => Some(Self::Keyword),
            "Integer" => Some(Self::Integer),
            "Float" => Some(Self::Float),
            "Geo" => Some(Self::Geo),
            "Text" => Some(Self::Text),
            "Bool" => Some(Self::Bool),
            "Datetime" => Some(Self::Datetime),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QuantizationType {
    UnknownQuantization = 0,
    Int8 = 1,
}
impl QuantizationType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            QuantizationType::UnknownQuantization => "UnknownQuantization",
            QuantizationType::Int8 => "Int8",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UnknownQuantization" => Some(Self::UnknownQuantization),
            "Int8" => Some(Self::Int8),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressionRatio {
    X4 = 0,
    X8 = 1,
    X16 = 2,
    X32 = 3,
    X64 = 4,
}
impl CompressionRatio {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CompressionRatio::X4 => "x4",
            CompressionRatio::X8 => "x8",
            CompressionRatio::X16 => "x16",
            CompressionRatio::X32 => "x32",
            CompressionRatio::X64 => "x64",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "x4" => Some(Self::X4),
            "x8" => Some(Self::X8),
            "x16" => Some(Self::X16),
            "x32" => Some(Self::X32),
            "x64" => Some(Self::X64),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShardingMethod {
    /// Auto-sharding based on record ids
    Auto = 0,
    /// Shard by user-defined key
    Custom = 1,
}
impl ShardingMethod {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShardingMethod::Auto => "Auto",
            ShardingMethod::Custom => "Custom",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Auto" => Some(Self::Auto),
            "Custom" => Some(Self::Custom),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TokenizerType {
    Unknown = 0,
    Prefix = 1,
    Whitespace = 2,
    Word = 3,
    Multilingual = 4,
}
impl TokenizerType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TokenizerType::Unknown => "Unknown",
            TokenizerType::Prefix => "Prefix",
            TokenizerType::Whitespace => "Whitespace",
            TokenizerType::Word => "Word",
            TokenizerType::Multilingual => "Multilingual",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Unknown" => Some(Self::Unknown),
            "Prefix" => Some(Self::Prefix),
            "Whitespace" => Some(Self::Whitespace),
            "Word" => Some(Self::Word),
            "Multilingual" => Some(Self::Multilingual),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReplicaState {
    /// Active and sound
    Active = 0,
    /// Failed for some reason
    Dead = 1,
    /// The shard is partially loaded and is currently receiving data from other shards
    Partial = 2,
    /// Collection is being created
    Initializing = 3,
    /// A shard which receives data, but is not used for search; Useful for backup shards
    Listener = 4,
    /// Deprecated: snapshot shard transfer is in progress; Updates should not be sent to (and are ignored by) the shard
    PartialSnapshot = 5,
    /// Shard is undergoing recovered by an external node; Normally rejects updates, accepts updates if force is true
    Recovery = 6,
    /// Points are being migrated to this shard as part of resharding
    Resharding = 7,
}
impl ReplicaState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReplicaState::Active => "Active",
            ReplicaState::Dead => "Dead",
            ReplicaState::Partial => "Partial",
            ReplicaState::Initializing => "Initializing",
            ReplicaState::Listener => "Listener",
            ReplicaState::PartialSnapshot => "PartialSnapshot",
            ReplicaState::Recovery => "Recovery",
            ReplicaState::Resharding => "Resharding",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Active" => Some(Self::Active),
            "Dead" => Some(Self::Dead),
            "Partial" => Some(Self::Partial),
            "Initializing" => Some(Self::Initializing),
            "Listener" => Some(Self::Listener),
            "PartialSnapshot" => Some(Self::PartialSnapshot),
            "Recovery" => Some(Self::Recovery),
            "Resharding" => Some(Self::Resharding),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShardTransferMethod {
    /// Stream shard records in batches
    StreamRecords = 0,
    /// Snapshot the shard and recover it on the target peer
    Snapshot = 1,
    /// Resolve WAL delta between peers and transfer the difference
    WalDelta = 2,
}
impl ShardTransferMethod {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShardTransferMethod::StreamRecords => "StreamRecords",
            ShardTransferMethod::Snapshot => "Snapshot",
            ShardTransferMethod::WalDelta => "WalDelta",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "StreamRecords" => Some(Self::StreamRecords),
            "Snapshot" => Some(Self::Snapshot),
            "WalDelta" => Some(Self::WalDelta),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod collections_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CollectionsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CollectionsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CollectionsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Get detailed information about specified existing collection
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCollectionInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetCollectionInfoResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Collections", "Get"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get list name of all existing collections
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<super::ListCollectionsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListCollectionsResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Collections", "List"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Create new collection with given parameters
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Collections", "Create"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Update parameters of the existing collection
        pub async fn update(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Collections", "Update"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Drop collection and all associated data
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Collections", "Delete"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Update Aliases of the existing collection
        pub async fn update_aliases(
            &mut self,
            request: impl tonic::IntoRequest<super::ChangeAliases>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "UpdateAliases"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get list of all aliases for a collection
        pub async fn list_collection_aliases(
            &mut self,
            request: impl tonic::IntoRequest<super::ListCollectionAliasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListAliasesResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/ListCollectionAliases",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "ListCollectionAliases"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get list of all aliases for all existing collections
        pub async fn list_aliases(
            &mut self,
            request: impl tonic::IntoRequest<super::ListAliasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListAliasesResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/ListAliases",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "ListAliases"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get cluster information for a collection
        pub async fn collection_cluster_info(
            &mut self,
            request: impl tonic::IntoRequest<super::CollectionClusterInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionClusterInfoResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/CollectionClusterInfo",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "CollectionClusterInfo"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Check the existence of a collection
        pub async fn collection_exists(
            &mut self,
            request: impl tonic::IntoRequest<super::CollectionExistsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionExistsResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/CollectionExists",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "CollectionExists"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Update cluster setup for a collection
        pub async fn update_collection_cluster_setup(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateCollectionClusterSetupRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateCollectionClusterSetupResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/UpdateCollectionClusterSetup",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("qdrant.Collections", "UpdateCollectionClusterSetup"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Create shard key
        pub async fn create_shard_key(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateShardKeyRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateShardKeyResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/CreateShardKey",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "CreateShardKey"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete shard key
        pub async fn delete_shard_key(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteShardKeyRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardKeyResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Collections/DeleteShardKey",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Collections", "DeleteShardKey"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod collections_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CollectionsServer.
    #[async_trait]
    pub trait Collections: Send + Sync + 'static {
        ///
        /// Get detailed information about specified existing collection
        async fn get(
            &self,
            request: tonic::Request<super::GetCollectionInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetCollectionInfoResponse>,
            tonic::Status,
        >;
        ///
        /// Get list name of all existing collections
        async fn list(
            &self,
            request: tonic::Request<super::ListCollectionsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListCollectionsResponse>,
            tonic::Status,
        >;
        ///
        /// Create new collection with given parameters
        async fn create(
            &self,
            request: tonic::Request<super::CreateCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Update parameters of the existing collection
        async fn update(
            &self,
            request: tonic::Request<super::UpdateCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Drop collection and all associated data
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteCollection>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Update Aliases of the existing collection
        async fn update_aliases(
            &self,
            request: tonic::Request<super::ChangeAliases>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Get list of all aliases for a collection
        async fn list_collection_aliases(
            &self,
            request: tonic::Request<super::ListCollectionAliasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListAliasesResponse>,
            tonic::Status,
        >;
        ///
        /// Get list of all aliases for all existing collections
        async fn list_aliases(
            &self,
            request: tonic::Request<super::ListAliasesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListAliasesResponse>,
            tonic::Status,
        >;
        ///
        /// Get cluster information for a collection
        async fn collection_cluster_info(
            &self,
            request: tonic::Request<super::CollectionClusterInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionClusterInfoResponse>,
            tonic::Status,
        >;
        ///
        /// Check the existence of a collection
        async fn collection_exists(
            &self,
            request: tonic::Request<super::CollectionExistsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionExistsResponse>,
            tonic::Status,
        >;
        ///
        /// Update cluster setup for a collection
        async fn update_collection_cluster_setup(
            &self,
            request: tonic::Request<super::UpdateCollectionClusterSetupRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateCollectionClusterSetupResponse>,
            tonic::Status,
        >;
        ///
        /// Create shard key
        async fn create_shard_key(
            &self,
            request: tonic::Request<super::CreateShardKeyRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateShardKeyResponse>,
            tonic::Status,
        >;
        ///
        /// Delete shard key
        async fn delete_shard_key(
            &self,
            request: tonic::Request<super::DeleteShardKeyRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteShardKeyResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CollectionsServer<T: Collections> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::get(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::list(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::create(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::update(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::delete(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::update_aliases(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateAliasesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/ListCollectionAliases" => {
                    #[allow(non_camel_case_types)]
                    struct ListCollectionAliasesSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::ListCollectionAliasesRequest>
                    for ListCollectionAliasesSvc<T> {
                        type Response = super::ListAliasesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListCollectionAliasesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::list_collection_aliases(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListCollectionAliasesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/ListAliases" => {
                    #[allow(non_camel_case_types)]
                    struct ListAliasesSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::ListAliasesRequest>
                    for ListAliasesSvc<T> {
                        type Response = super::ListAliasesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListAliasesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::list_aliases(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListAliasesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/CollectionClusterInfo" => {
                    #[allow(non_camel_case_types)]
                    struct CollectionClusterInfoSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::CollectionClusterInfoRequest>
                    for CollectionClusterInfoSvc<T> {
                        type Response = super::CollectionClusterInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CollectionClusterInfoRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::collection_cluster_info(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CollectionClusterInfoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/CollectionExists" => {
                    #[allow(non_camel_case_types)]
                    struct CollectionExistsSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::CollectionExistsRequest>
                    for CollectionExistsSvc<T> {
                        type Response = super::CollectionExistsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CollectionExistsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::collection_exists(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CollectionExistsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/UpdateCollectionClusterSetup" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateCollectionClusterSetupSvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<
                        super::UpdateCollectionClusterSetupRequest,
                    > for UpdateCollectionClusterSetupSvc<T> {
                        type Response = super::UpdateCollectionClusterSetupResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UpdateCollectionClusterSetupRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::update_collection_cluster_setup(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateCollectionClusterSetupSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/CreateShardKey" => {
                    #[allow(non_camel_case_types)]
                    struct CreateShardKeySvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::CreateShardKeyRequest>
                    for CreateShardKeySvc<T> {
                        type Response = super::CreateShardKeyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateShardKeyRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::create_shard_key(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateShardKeySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Collections/DeleteShardKey" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteShardKeySvc<T: Collections>(pub Arc<T>);
                    impl<
                        T: Collections,
                    > tonic::server::UnaryService<super::DeleteShardKeyRequest>
                    for DeleteShardKeySvc<T> {
                        type Response = super::DeleteShardKeyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteShardKeyRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Collections>::delete_shard_key(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteShardKeySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Collections> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Collections> tonic::server::NamedService for CollectionsServer<T> {
        const NAME: &'static str = "qdrant.Collections";
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionInfoRequestInternal {
    #[prost(message, optional, tag = "1")]
    pub get_collection_info_request: ::core::option::Option<GetCollectionInfoRequest>,
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitiateShardTransferRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the temporary shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitForShardStateRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
    /// Shard state to wait for
    #[prost(enumeration = "ReplicaState", tag = "3")]
    pub state: i32,
    /// Timeout in seconds
    #[prost(uint64, tag = "4")]
    #[validate(range(min = 1))]
    pub timeout: u64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShardRecoveryPointRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShardRecoveryPointResponse {
    /// Recovery point of the shard
    #[prost(message, optional, tag = "1")]
    pub recovery_point: ::core::option::Option<RecoveryPoint>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoveryPoint {
    #[prost(message, repeated, tag = "1")]
    pub clocks: ::prost::alloc::vec::Vec<RecoveryPointClockTag>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoveryPointClockTag {
    #[prost(uint64, tag = "1")]
    pub peer_id: u64,
    #[prost(uint32, tag = "2")]
    pub clock_id: u32,
    #[prost(uint64, tag = "3")]
    pub clock_tick: u64,
    #[prost(uint64, tag = "4")]
    pub token: u64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateShardCutoffPointRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
    /// Cutoff point of the shard
    #[prost(message, optional, tag = "3")]
    pub cutoff: ::core::option::Option<RecoveryPoint>,
}
/// Generated client implementations.
pub mod collections_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CollectionsInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CollectionsInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CollectionsInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Get collection info
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCollectionInfoRequestInternal>,
        ) -> std::result::Result<
            tonic::Response<super::GetCollectionInfoResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.CollectionsInternal", "Get"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Initiate shard transfer
        pub async fn initiate(
            &mut self,
            request: impl tonic::IntoRequest<super::InitiateShardTransferRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.CollectionsInternal/Initiate",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.CollectionsInternal", "Initiate"));
            self.inner.unary(req, path, codec).await
        }
        /// *
        /// Wait for a shard to get into the given state
        pub async fn wait_for_shard_state(
            &mut self,
            request: impl tonic::IntoRequest<super::WaitForShardStateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.CollectionsInternal/WaitForShardState",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("qdrant.CollectionsInternal", "WaitForShardState"),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Get shard recovery point
        pub async fn get_shard_recovery_point(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShardRecoveryPointRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShardRecoveryPointResponse>,
            tonic::Status,
        > {
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
                "/qdrant.CollectionsInternal/GetShardRecoveryPoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "qdrant.CollectionsInternal",
                        "GetShardRecoveryPoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Update shard cutoff point
        pub async fn update_shard_cutoff_point(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateShardCutoffPointRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.CollectionsInternal/UpdateShardCutoffPoint",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "qdrant.CollectionsInternal",
                        "UpdateShardCutoffPoint",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod collections_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CollectionsInternalServer.
    #[async_trait]
    pub trait CollectionsInternal: Send + Sync + 'static {
        ///
        /// Get collection info
        async fn get(
            &self,
            request: tonic::Request<super::GetCollectionInfoRequestInternal>,
        ) -> std::result::Result<
            tonic::Response<super::GetCollectionInfoResponse>,
            tonic::Status,
        >;
        ///
        /// Initiate shard transfer
        async fn initiate(
            &self,
            request: tonic::Request<super::InitiateShardTransferRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        /// *
        /// Wait for a shard to get into the given state
        async fn wait_for_shard_state(
            &self,
            request: tonic::Request<super::WaitForShardStateRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Get shard recovery point
        async fn get_shard_recovery_point(
            &self,
            request: tonic::Request<super::GetShardRecoveryPointRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShardRecoveryPointResponse>,
            tonic::Status,
        >;
        ///
        /// Update shard cutoff point
        async fn update_shard_cutoff_point(
            &self,
            request: tonic::Request<super::UpdateShardCutoffPointRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CollectionOperationResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CollectionsInternalServer<T: CollectionsInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CollectionsInternal>::get(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.CollectionsInternal/Initiate" => {
                    #[allow(non_camel_case_types)]
                    struct InitiateSvc<T: CollectionsInternal>(pub Arc<T>);
                    impl<
                        T: CollectionsInternal,
                    > tonic::server::UnaryService<super::InitiateShardTransferRequest>
                    for InitiateSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InitiateShardTransferRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CollectionsInternal>::initiate(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = InitiateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.CollectionsInternal/WaitForShardState" => {
                    #[allow(non_camel_case_types)]
                    struct WaitForShardStateSvc<T: CollectionsInternal>(pub Arc<T>);
                    impl<
                        T: CollectionsInternal,
                    > tonic::server::UnaryService<super::WaitForShardStateRequest>
                    for WaitForShardStateSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WaitForShardStateRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CollectionsInternal>::wait_for_shard_state(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WaitForShardStateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.CollectionsInternal/GetShardRecoveryPoint" => {
                    #[allow(non_camel_case_types)]
                    struct GetShardRecoveryPointSvc<T: CollectionsInternal>(pub Arc<T>);
                    impl<
                        T: CollectionsInternal,
                    > tonic::server::UnaryService<super::GetShardRecoveryPointRequest>
                    for GetShardRecoveryPointSvc<T> {
                        type Response = super::GetShardRecoveryPointResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetShardRecoveryPointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CollectionsInternal>::get_shard_recovery_point(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetShardRecoveryPointSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.CollectionsInternal/UpdateShardCutoffPoint" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateShardCutoffPointSvc<T: CollectionsInternal>(pub Arc<T>);
                    impl<
                        T: CollectionsInternal,
                    > tonic::server::UnaryService<super::UpdateShardCutoffPointRequest>
                    for UpdateShardCutoffPointSvc<T> {
                        type Response = super::CollectionOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateShardCutoffPointRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CollectionsInternal>::update_shard_cutoff_point(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateShardCutoffPointSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: CollectionsInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CollectionsInternal> tonic::server::NamedService
    for CollectionsInternalServer<T> {
        const NAME: &'static str = "qdrant.CollectionsInternal";
    }
}
/// `Struct` represents a structured data value, consisting of fields
/// which map to dynamically typed values. In some languages, `Struct`
/// might be supported by a native representation. For example, in
/// scripting languages like JS a struct is represented as an
/// object. The details of that representation are described together
/// with the proto support for the language.
///
/// The JSON representation for `Struct` is a JSON object.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Struct {
    /// Unordered map of dynamically typed values.
    #[prost(map = "string, message", tag = "1")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
}
/// `Value` represents a dynamically typed value which can be either
/// null, a number, a string, a boolean, a recursive struct value, or a
/// list of values. A producer of value is expected to set one of those
/// variants, absence of any variant indicates an error.
///
/// The JSON representation for `Value` is a JSON value.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    /// The kind of value.
    #[prost(oneof = "value::Kind", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub kind: ::core::option::Option<value::Kind>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    /// The kind of value.
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        /// Represents a null value.
        #[prost(enumeration = "super::NullValue", tag = "1")]
        NullValue(i32),
        /// Represents a double value.
        #[prost(double, tag = "2")]
        DoubleValue(f64),
        /// Represents an integer value
        #[prost(int64, tag = "3")]
        IntegerValue(i64),
        /// Represents a string value.
        #[prost(string, tag = "4")]
        StringValue(::prost::alloc::string::String),
        /// Represents a boolean value.
        #[prost(bool, tag = "5")]
        BoolValue(bool),
        /// Represents a structured value.
        #[prost(message, tag = "6")]
        StructValue(super::Struct),
        /// Represents a repeated `Value`.
        #[prost(message, tag = "7")]
        ListValue(super::ListValue),
    }
}
/// `ListValue` is a wrapper around a repeated field of values.
///
/// The JSON representation for `ListValue` is a JSON array.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListValue {
    /// Repeated field of dynamically typed values.
    #[prost(message, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<Value>,
}
/// `NullValue` is a singleton enumeration to represent the null value for the
/// `Value` type union.
///
///   The JSON representation for `NullValue` is JSON `null`.
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum NullValue {
    /// Null value.
    NullValue = 0,
}
impl NullValue {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            NullValue::NullValue => "NULL_VALUE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NULL_VALUE" => Some(Self::NullValue),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteOrdering {
    /// Write ordering guarantees
    #[prost(enumeration = "WriteOrderingType", tag = "1")]
    pub r#type: i32,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadConsistency {
    #[prost(oneof = "read_consistency::Value", tags = "1, 2")]
    pub value: ::core::option::Option<read_consistency::Value>,
}
/// Nested message and enum types in `ReadConsistency`.
pub mod read_consistency {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        /// Common read consistency configurations
        #[prost(enumeration = "super::ReadConsistencyType", tag = "1")]
        Type(i32),
        /// Send request to a specified number of nodes, and return points which are present on all of them
        #[prost(uint64, tag = "2")]
        Factor(u64),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointId {
    #[prost(oneof = "point_id::PointIdOptions", tags = "1, 2")]
    pub point_id_options: ::core::option::Option<point_id::PointIdOptions>,
}
/// Nested message and enum types in `PointId`.
pub mod point_id {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PointIdOptions {
        /// Numerical ID of the point
        #[prost(uint64, tag = "1")]
        Num(u64),
        /// UUID
        #[prost(string, tag = "2")]
        Uuid(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SparseIndices {
    #[prost(uint32, repeated, tag = "1")]
    pub data: ::prost::alloc::vec::Vec<u32>,
}
/// Legacy vector format, which determines the vector type by the configuration of its fields.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vector {
    /// Vector data (flatten for multi vectors)
    #[prost(float, repeated, tag = "1")]
    pub data: ::prost::alloc::vec::Vec<f32>,
    /// Sparse indices for sparse vectors
    #[prost(message, optional, tag = "2")]
    pub indices: ::core::option::Option<SparseIndices>,
    /// Number of vectors per multi vector
    #[prost(uint32, optional, tag = "3")]
    pub vectors_count: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DenseVector {
    #[prost(float, repeated, tag = "1")]
    pub data: ::prost::alloc::vec::Vec<f32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SparseVector {
    #[prost(float, repeated, tag = "1")]
    pub values: ::prost::alloc::vec::Vec<f32>,
    #[prost(uint32, repeated, tag = "2")]
    pub indices: ::prost::alloc::vec::Vec<u32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiDenseVector {
    #[prost(message, repeated, tag = "1")]
    pub vectors: ::prost::alloc::vec::Vec<DenseVector>,
}
/// Vector type to be used in queries. Ids will be substituted with their corresponding vectors from the collection.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorInput {
    #[prost(oneof = "vector_input::Variant", tags = "1, 2, 3, 4")]
    pub variant: ::core::option::Option<vector_input::Variant>,
}
/// Nested message and enum types in `VectorInput`.
pub mod vector_input {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Variant {
        #[prost(message, tag = "1")]
        Id(super::PointId),
        #[prost(message, tag = "2")]
        Dense(super::DenseVector),
        #[prost(message, tag = "3")]
        Sparse(super::SparseVector),
        #[prost(message, tag = "4")]
        MultiDense(super::MultiDenseVector),
    }
}
/// ---------------------------------------------
/// ----------------- ShardKeySelector ----------
/// ---------------------------------------------
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardKeySelector {
    /// List of shard keys which should be used in the request
    #[prost(message, repeated, tag = "1")]
    pub shard_keys: ::prost::alloc::vec::Vec<ShardKey>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    #[prost(message, repeated, tag = "3")]
    #[validate]
    pub points: ::prost::alloc::vec::Vec<PointStruct>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "5")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// Affected points
    #[prost(message, optional, tag = "3")]
    pub points: ::core::option::Option<PointsSelector>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "5")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// List of points to retrieve
    #[prost(message, repeated, tag = "2")]
    pub ids: ::prost::alloc::vec::Vec<PointId>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "4")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "5")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "6")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "7")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdatePointVectors {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// List of points and vectors to update
    #[prost(message, repeated, tag = "3")]
    pub points: ::prost::alloc::vec::Vec<PointVectors>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "5")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointVectors {
    /// ID to update vectors for
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<PointId>,
    /// Named vectors to update, leave others intact
    #[prost(message, optional, tag = "2")]
    pub vectors: ::core::option::Option<Vectors>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePointVectors {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// Affected points
    #[prost(message, optional, tag = "3")]
    pub points_selector: ::core::option::Option<PointsSelector>,
    /// List of vector names to delete
    #[prost(message, optional, tag = "4")]
    pub vectors: ::core::option::Option<VectorsSelector>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "5")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "6")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPayloadPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// New payload values
    #[prost(map = "string, message", tag = "3")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    /// Affected points
    #[prost(message, optional, tag = "5")]
    pub points_selector: ::core::option::Option<PointsSelector>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "6")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "7")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
    /// Option for indicate property of payload
    #[prost(string, optional, tag = "8")]
    pub key: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePayloadPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// List of keys to delete
    #[prost(string, repeated, tag = "3")]
    pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Affected points
    #[prost(message, optional, tag = "5")]
    pub points_selector: ::core::option::Option<PointsSelector>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "6")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "7")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearPayloadPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// Affected points
    #[prost(message, optional, tag = "3")]
    pub points: ::core::option::Option<PointsSelector>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
    /// Option for custom sharding to specify used shard keys
    #[prost(message, optional, tag = "5")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateFieldIndexCollection {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// Field name to index
    #[prost(string, tag = "3")]
    #[validate(length(min = 1))]
    pub field_name: ::prost::alloc::string::String,
    /// Field type.
    #[prost(enumeration = "FieldType", optional, tag = "4")]
    pub field_type: ::core::option::Option<i32>,
    /// Payload index params.
    #[prost(message, optional, tag = "5")]
    pub field_index_params: ::core::option::Option<PayloadIndexParams>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "6")]
    pub ordering: ::core::option::Option<WriteOrdering>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteFieldIndexCollection {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    /// Field name to delete
    #[prost(string, tag = "3")]
    #[validate(length(min = 1))]
    pub field_name: ::prost::alloc::string::String,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadIncludeSelector {
    /// List of payload keys to include into result
    #[prost(string, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PayloadExcludeSelector {
    /// List of payload keys to exclude from the result
    #[prost(string, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WithPayloadSelector {
    #[prost(oneof = "with_payload_selector::SelectorOptions", tags = "1, 2, 3")]
    pub selector_options: ::core::option::Option<with_payload_selector::SelectorOptions>,
}
/// Nested message and enum types in `WithPayloadSelector`.
pub mod with_payload_selector {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum SelectorOptions {
        /// If `true` - return all payload, if `false` - none
        #[prost(bool, tag = "1")]
        Enable(bool),
        #[prost(message, tag = "2")]
        Include(super::PayloadIncludeSelector),
        #[prost(message, tag = "3")]
        Exclude(super::PayloadExcludeSelector),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedVectors {
    #[prost(map = "string, message", tag = "1")]
    #[validate]
    pub vectors: ::std::collections::HashMap<::prost::alloc::string::String, Vector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Vectors {
    #[prost(oneof = "vectors::VectorsOptions", tags = "1, 2")]
    #[validate]
    pub vectors_options: ::core::option::Option<vectors::VectorsOptions>,
}
/// Nested message and enum types in `Vectors`.
pub mod vectors {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum VectorsOptions {
        #[prost(message, tag = "1")]
        Vector(super::Vector),
        #[prost(message, tag = "2")]
        Vectors(super::NamedVectors),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorsSelector {
    /// List of vectors to include into result
    #[prost(string, repeated, tag = "1")]
    pub names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WithVectorsSelector {
    #[prost(oneof = "with_vectors_selector::SelectorOptions", tags = "1, 2")]
    pub selector_options: ::core::option::Option<with_vectors_selector::SelectorOptions>,
}
/// Nested message and enum types in `WithVectorsSelector`.
pub mod with_vectors_selector {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum SelectorOptions {
        /// If `true` - return all vectors, if `false` - none
        #[prost(bool, tag = "1")]
        Enable(bool),
        /// List of payload keys to include into result
        #[prost(message, tag = "2")]
        Include(super::VectorsSelector),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuantizationSearchParams {
    ///
    /// If set to true, search will ignore quantized vector data
    #[prost(bool, optional, tag = "1")]
    pub ignore: ::core::option::Option<bool>,
    ///
    /// If true, use original vectors to re-score top-k results. If ignored, qdrant decides automatically does rescore enabled or not.
    #[prost(bool, optional, tag = "2")]
    pub rescore: ::core::option::Option<bool>,
    ///
    /// Oversampling factor for quantization.
    ///
    /// Defines how many extra vectors should be pre-selected using quantized index,
    /// and then re-scored using original vectors.
    ///
    /// For example, if `oversampling` is 2.4 and `limit` is 100, then 240 vectors will be pre-selected using quantized index,
    /// and then top-100 will be returned after re-scoring.
    #[prost(double, optional, tag = "3")]
    #[validate(custom = "crate::grpc::validate::validate_f64_range_min_1")]
    pub oversampling: ::core::option::Option<f64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchParams {
    ///
    /// Params relevant to HNSW index. Size of the beam in a beam-search.
    /// Larger the value - more accurate the result, more time required for search.
    #[prost(uint64, optional, tag = "1")]
    pub hnsw_ef: ::core::option::Option<u64>,
    ///
    /// Search without approximation. If set to true, search may run long but with exact results.
    #[prost(bool, optional, tag = "2")]
    pub exact: ::core::option::Option<bool>,
    ///
    /// If set to true, search will ignore quantized vector data
    #[prost(message, optional, tag = "3")]
    #[validate]
    pub quantization: ::core::option::Option<QuantizationSearchParams>,
    ///
    /// If enabled, the engine will only perform search among indexed or small segments.
    /// Using this option prevents slow searches in case of delayed index, but does not
    /// guarantee that all uploaded vectors will be included in search results
    #[prost(bool, optional, tag = "4")]
    pub indexed_only: ::core::option::Option<bool>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// vector
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "3")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint64, tag = "4")]
    #[validate(range(min = 1))]
    pub limit: u64,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag = "7")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    /// If provided - cut off results with worse scores
    #[prost(float, optional, tag = "8")]
    pub score_threshold: ::core::option::Option<f32>,
    /// Offset of the result
    #[prost(uint64, optional, tag = "9")]
    pub offset: ::core::option::Option<u64>,
    /// Which vector to use for search, if not specified - use default vector
    #[prost(string, optional, tag = "10")]
    pub vector_name: ::core::option::Option<::prost::alloc::string::String>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "11")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "12")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "13")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "14")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
    #[prost(message, optional, tag = "15")]
    pub sparse_indices: ::core::option::Option<SparseIndices>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchBatchPoints {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub search_points: ::prost::alloc::vec::Vec<SearchPoints>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "3")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "4")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WithLookup {
    /// Name of the collection to use for points lookup
    #[prost(string, tag = "1")]
    pub collection: ::prost::alloc::string::String,
    /// Options for specifying which payload to include (or not)
    #[prost(message, optional, tag = "2")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Options for specifying which vectors to include (or not)
    #[prost(message, optional, tag = "3")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchPointGroups {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Vector to compare against
    #[prost(float, repeated, tag = "2")]
    pub vector: ::prost::alloc::vec::Vec<f32>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "3")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint32, tag = "4")]
    #[validate(range(min = 1))]
    pub limit: u32,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "5")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag = "6")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    /// If provided - cut off results with worse scores
    #[prost(float, optional, tag = "7")]
    pub score_threshold: ::core::option::Option<f32>,
    /// Which vector to use for search, if not specified - use default vector
    #[prost(string, optional, tag = "8")]
    pub vector_name: ::core::option::Option<::prost::alloc::string::String>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "9")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Payload field to group by, must be a string or number field. If there are multiple values for the field, all of them will be used. One point can be in multiple groups.
    #[prost(string, tag = "10")]
    #[validate(length(min = 1))]
    pub group_by: ::prost::alloc::string::String,
    /// Maximum amount of points to return per group
    #[prost(uint32, tag = "11")]
    #[validate(range(min = 1))]
    pub group_size: u32,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "12")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// Options for specifying how to use the group id to lookup points in another collection
    #[prost(message, optional, tag = "13")]
    pub with_lookup: ::core::option::Option<WithLookup>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "14")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "15")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
    #[prost(message, optional, tag = "16")]
    pub sparse_indices: ::core::option::Option<SparseIndices>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartFrom {
    #[prost(oneof = "start_from::Value", tags = "1, 2, 3, 4")]
    pub value: ::core::option::Option<start_from::Value>,
}
/// Nested message and enum types in `StartFrom`.
pub mod start_from {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(double, tag = "1")]
        Float(f64),
        #[prost(int64, tag = "2")]
        Integer(i64),
        #[prost(message, tag = "3")]
        Timestamp(::prost_wkt_types::Timestamp),
        #[prost(string, tag = "4")]
        Datetime(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OrderBy {
    /// Payload key to order by
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Ascending or descending order
    #[prost(enumeration = "Direction", optional, tag = "2")]
    pub direction: ::core::option::Option<i32>,
    /// Start from this value
    #[prost(message, optional, tag = "3")]
    pub start_from: ::core::option::Option<StartFrom>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollPoints {
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Start with this ID
    #[prost(message, optional, tag = "3")]
    pub offset: ::core::option::Option<PointId>,
    /// Max number of result
    #[prost(uint32, optional, tag = "4")]
    #[validate(custom = "crate::grpc::validate::validate_u32_range_min_1")]
    pub limit: ::core::option::Option<u32>,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "7")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "8")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "9")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
    /// Order the records by a payload field
    #[prost(message, optional, tag = "10")]
    pub order_by: ::core::option::Option<OrderBy>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LookupLocation {
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    /// Which vector to use for search, if not specified - use default vector
    #[prost(string, optional, tag = "2")]
    pub vector_name: ::core::option::Option<::prost::alloc::string::String>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "3")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Look for vectors closest to the vectors from these points
    #[prost(message, repeated, tag = "2")]
    pub positive: ::prost::alloc::vec::Vec<PointId>,
    /// Try to avoid vectors like the vector from these points
    #[prost(message, repeated, tag = "3")]
    pub negative: ::prost::alloc::vec::Vec<PointId>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint64, tag = "5")]
    pub limit: u64,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "7")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag = "8")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    /// If provided - cut off results with worse scores
    #[prost(float, optional, tag = "9")]
    pub score_threshold: ::core::option::Option<f32>,
    /// Offset of the result
    #[prost(uint64, optional, tag = "10")]
    pub offset: ::core::option::Option<u64>,
    /// Define which vector to use for recommendation, if not specified - default vector
    #[prost(string, optional, tag = "11")]
    pub using: ::core::option::Option<::prost::alloc::string::String>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "12")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Name of the collection to use for points lookup, if not specified - use current collection
    #[prost(message, optional, tag = "13")]
    pub lookup_from: ::core::option::Option<LookupLocation>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "14")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// How to use the example vectors to find the results
    #[prost(enumeration = "RecommendStrategy", optional, tag = "16")]
    pub strategy: ::core::option::Option<i32>,
    /// Look for vectors closest to those
    #[prost(message, repeated, tag = "17")]
    #[validate]
    pub positive_vectors: ::prost::alloc::vec::Vec<Vector>,
    /// Try to avoid vectors like this
    #[prost(message, repeated, tag = "18")]
    #[validate]
    pub negative_vectors: ::prost::alloc::vec::Vec<Vector>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "19")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "20")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendBatchPoints {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub recommend_points: ::prost::alloc::vec::Vec<RecommendPoints>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "3")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "4")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendPointGroups {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Look for vectors closest to the vectors from these points
    #[prost(message, repeated, tag = "2")]
    pub positive: ::prost::alloc::vec::Vec<PointId>,
    /// Try to avoid vectors like the vector from these points
    #[prost(message, repeated, tag = "3")]
    pub negative: ::prost::alloc::vec::Vec<PointId>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of groups in result
    #[prost(uint32, tag = "5")]
    #[validate(range(min = 1))]
    pub limit: u32,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag = "7")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    /// If provided - cut off results with worse scores
    #[prost(float, optional, tag = "8")]
    pub score_threshold: ::core::option::Option<f32>,
    /// Define which vector to use for recommendation, if not specified - default vector
    #[prost(string, optional, tag = "9")]
    pub using: ::core::option::Option<::prost::alloc::string::String>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "10")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Name of the collection to use for points lookup, if not specified - use current collection
    #[prost(message, optional, tag = "11")]
    pub lookup_from: ::core::option::Option<LookupLocation>,
    /// Payload field to group by, must be a string or number field. If there are multiple values for the field, all of them will be used. One point can be in multiple groups.
    #[prost(string, tag = "12")]
    #[validate(length(min = 1))]
    pub group_by: ::prost::alloc::string::String,
    /// Maximum amount of points to return per group
    #[prost(uint32, tag = "13")]
    #[validate(range(min = 1))]
    pub group_size: u32,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "14")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// Options for specifying how to use the group id to lookup points in another collection
    #[prost(message, optional, tag = "15")]
    pub with_lookup: ::core::option::Option<WithLookup>,
    /// How to use the example vectors to find the results
    #[prost(enumeration = "RecommendStrategy", optional, tag = "17")]
    pub strategy: ::core::option::Option<i32>,
    /// Look for vectors closest to those
    #[prost(message, repeated, tag = "18")]
    #[validate]
    pub positive_vectors: ::prost::alloc::vec::Vec<Vector>,
    /// Try to avoid vectors like this
    #[prost(message, repeated, tag = "19")]
    #[validate]
    pub negative_vectors: ::prost::alloc::vec::Vec<Vector>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "20")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "21")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TargetVector {
    #[prost(oneof = "target_vector::Target", tags = "1")]
    pub target: ::core::option::Option<target_vector::Target>,
}
/// Nested message and enum types in `TargetVector`.
pub mod target_vector {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Target {
        #[prost(message, tag = "1")]
        Single(super::VectorExample),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VectorExample {
    #[prost(oneof = "vector_example::Example", tags = "1, 2")]
    pub example: ::core::option::Option<vector_example::Example>,
}
/// Nested message and enum types in `VectorExample`.
pub mod vector_example {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Example {
        #[prost(message, tag = "1")]
        Id(super::PointId),
        #[prost(message, tag = "2")]
        Vector(super::Vector),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContextExamplePair {
    #[prost(message, optional, tag = "1")]
    pub positive: ::core::option::Option<VectorExample>,
    #[prost(message, optional, tag = "2")]
    pub negative: ::core::option::Option<VectorExample>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Use this as the primary search objective
    #[prost(message, optional, tag = "2")]
    pub target: ::core::option::Option<TargetVector>,
    /// Search will be constrained by these pairs of examples
    #[prost(message, repeated, tag = "3")]
    pub context: ::prost::alloc::vec::Vec<ContextExamplePair>,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// Max number of result
    #[prost(uint64, tag = "5")]
    #[validate(range(min = 1))]
    pub limit: u64,
    /// Options for specifying which payload to include or not
    #[prost(message, optional, tag = "6")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    /// Search config
    #[prost(message, optional, tag = "7")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    /// Offset of the result
    #[prost(uint64, optional, tag = "8")]
    pub offset: ::core::option::Option<u64>,
    /// Define which vector to use for recommendation, if not specified - default vector
    #[prost(string, optional, tag = "9")]
    pub using: ::core::option::Option<::prost::alloc::string::String>,
    /// Options for specifying which vectors to include into response
    #[prost(message, optional, tag = "10")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    /// Name of the collection to use for points lookup, if not specified - use current collection
    #[prost(message, optional, tag = "11")]
    pub lookup_from: ::core::option::Option<LookupLocation>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "12")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "13")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "14")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverBatchPoints {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub discover_points: ::prost::alloc::vec::Vec<DiscoverPoints>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "3")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// If set, overrides global timeout setting for this request. Unit is seconds.
    #[prost(uint64, optional, tag = "4")]
    #[validate(custom = "crate::grpc::validate::validate_u64_range_min_1")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CountPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Filter conditions - return only those points that satisfy the specified conditions
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    /// If `true` - return exact count, if `false` - return approximate count
    #[prost(bool, optional, tag = "3")]
    pub exact: ::core::option::Option<bool>,
    /// Options for specifying read consistency guarantees
    #[prost(message, optional, tag = "4")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
    /// Specify in which shards to look for the points, if not specified - look in all shards
    #[prost(message, optional, tag = "5")]
    pub shard_key_selector: ::core::option::Option<ShardKeySelector>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsUpdateOperation {
    #[prost(
        oneof = "points_update_operation::Operation",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10"
    )]
    pub operation: ::core::option::Option<points_update_operation::Operation>,
}
/// Nested message and enum types in `PointsUpdateOperation`.
pub mod points_update_operation {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PointStructList {
        #[prost(message, repeated, tag = "1")]
        pub points: ::prost::alloc::vec::Vec<super::PointStruct>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "2")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct SetPayload {
        #[prost(map = "string, message", tag = "1")]
        pub payload: ::std::collections::HashMap<
            ::prost::alloc::string::String,
            super::Value,
        >,
        /// Affected points
        #[prost(message, optional, tag = "2")]
        pub points_selector: ::core::option::Option<super::PointsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "3")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
        /// Option for indicate property of payload
        #[prost(string, optional, tag = "4")]
        pub key: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct OverwritePayload {
        #[prost(map = "string, message", tag = "1")]
        pub payload: ::std::collections::HashMap<
            ::prost::alloc::string::String,
            super::Value,
        >,
        /// Affected points
        #[prost(message, optional, tag = "2")]
        pub points_selector: ::core::option::Option<super::PointsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "3")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
        /// Option for indicate property of payload
        #[prost(string, optional, tag = "4")]
        pub key: ::core::option::Option<::prost::alloc::string::String>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DeletePayload {
        #[prost(string, repeated, tag = "1")]
        pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        /// Affected points
        #[prost(message, optional, tag = "2")]
        pub points_selector: ::core::option::Option<super::PointsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "3")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UpdateVectors {
        /// List of points and vectors to update
        #[prost(message, repeated, tag = "1")]
        pub points: ::prost::alloc::vec::Vec<super::PointVectors>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "2")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DeleteVectors {
        /// Affected points
        #[prost(message, optional, tag = "1")]
        pub points_selector: ::core::option::Option<super::PointsSelector>,
        /// List of vector names to delete
        #[prost(message, optional, tag = "2")]
        pub vectors: ::core::option::Option<super::VectorsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "3")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(validator::Validate)]
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct DeletePoints {
        /// Affected points
        #[prost(message, optional, tag = "1")]
        pub points: ::core::option::Option<super::PointsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "2")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ClearPayload {
        /// Affected points
        #[prost(message, optional, tag = "1")]
        pub points: ::core::option::Option<super::PointsSelector>,
        /// Option for custom sharding to specify used shard keys
        #[prost(message, optional, tag = "2")]
        pub shard_key_selector: ::core::option::Option<super::ShardKeySelector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "1")]
        Upsert(PointStructList),
        #[prost(message, tag = "2")]
        DeleteDeprecated(super::PointsSelector),
        #[prost(message, tag = "3")]
        SetPayload(SetPayload),
        #[prost(message, tag = "4")]
        OverwritePayload(OverwritePayload),
        #[prost(message, tag = "5")]
        DeletePayload(DeletePayload),
        #[prost(message, tag = "6")]
        ClearPayloadDeprecated(super::PointsSelector),
        #[prost(message, tag = "7")]
        UpdateVectors(UpdateVectors),
        #[prost(message, tag = "8")]
        DeleteVectors(DeleteVectors),
        #[prost(message, tag = "9")]
        DeletePoints(DeletePoints),
        #[prost(message, tag = "10")]
        ClearPayload(ClearPayload),
    }
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateBatchPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    #[prost(message, repeated, tag = "3")]
    #[validate(length(min = 1))]
    pub operations: ::prost::alloc::vec::Vec<PointsUpdateOperation>,
    /// Write ordering guarantees
    #[prost(message, optional, tag = "4")]
    pub ordering: ::core::option::Option<WriteOrdering>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsOperationResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<UpdateResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateResult {
    /// Number of operation
    #[prost(uint64, optional, tag = "1")]
    pub operation_id: ::core::option::Option<u64>,
    /// Operation status
    #[prost(enumeration = "UpdateStatus", tag = "2")]
    pub status: i32,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScoredPoint {
    /// Point id
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<PointId>,
    /// Payload
    #[prost(map = "string, message", tag = "2")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    /// Similarity score
    #[prost(float, tag = "3")]
    pub score: f32,
    /// Last update operation applied to this point
    #[prost(uint64, tag = "5")]
    pub version: u64,
    /// Vectors to search
    #[prost(message, optional, tag = "6")]
    pub vectors: ::core::option::Option<Vectors>,
    /// Shard key
    #[prost(message, optional, tag = "7")]
    pub shard_key: ::core::option::Option<ShardKey>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupId {
    #[prost(oneof = "group_id::Kind", tags = "1, 2, 3")]
    pub kind: ::core::option::Option<group_id::Kind>,
}
/// Nested message and enum types in `GroupId`.
pub mod group_id {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        /// Represents a double value.
        #[prost(uint64, tag = "1")]
        UnsignedValue(u64),
        /// Represents an integer value
        #[prost(int64, tag = "2")]
        IntegerValue(i64),
        /// Represents a string value.
        #[prost(string, tag = "3")]
        StringValue(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointGroup {
    /// Group id
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<GroupId>,
    /// Points in the group
    #[prost(message, repeated, tag = "2")]
    pub hits: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Point(s) from the lookup collection that matches the group id
    #[prost(message, optional, tag = "3")]
    pub lookup: ::core::option::Option<RetrievedPoint>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupsResult {
    /// Groups
    #[prost(message, repeated, tag = "1")]
    pub groups: ::prost::alloc::vec::Vec<PointGroup>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchResult {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchBatchResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<BatchResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchGroupsResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<GroupsResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CountResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<CountResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollResponse {
    /// Use this offset for the next query
    #[prost(message, optional, tag = "1")]
    pub next_page_offset: ::core::option::Option<PointId>,
    #[prost(message, repeated, tag = "2")]
    pub result: ::prost::alloc::vec::Vec<RetrievedPoint>,
    /// Time spent to process
    #[prost(double, tag = "3")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CountResult {
    #[prost(uint64, tag = "1")]
    pub count: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetrievedPoint {
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<PointId>,
    #[prost(map = "string, message", tag = "2")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    #[prost(message, optional, tag = "4")]
    pub vectors: ::core::option::Option<Vectors>,
    /// Shard key
    #[prost(message, optional, tag = "5")]
    pub shard_key: ::core::option::Option<ShardKey>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<RetrievedPoint>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendBatchResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<BatchResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoverBatchResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<BatchResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendGroupsResponse {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<GroupsResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateBatchResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<UpdateResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    /// At least one of those conditions should match
    #[prost(message, repeated, tag = "1")]
    #[validate]
    pub should: ::prost::alloc::vec::Vec<Condition>,
    /// All conditions must match
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub must: ::prost::alloc::vec::Vec<Condition>,
    /// All conditions must NOT match
    #[prost(message, repeated, tag = "3")]
    #[validate]
    pub must_not: ::prost::alloc::vec::Vec<Condition>,
    /// At least minimum amount of given conditions should match
    #[prost(message, optional, tag = "4")]
    pub min_should: ::core::option::Option<MinShould>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MinShould {
    #[prost(message, repeated, tag = "1")]
    pub conditions: ::prost::alloc::vec::Vec<Condition>,
    #[prost(uint64, tag = "2")]
    pub min_count: u64,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Condition {
    #[prost(oneof = "condition::ConditionOneOf", tags = "1, 2, 3, 4, 5, 6")]
    #[validate]
    pub condition_one_of: ::core::option::Option<condition::ConditionOneOf>,
}
/// Nested message and enum types in `Condition`.
pub mod condition {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ConditionOneOf {
        #[prost(message, tag = "1")]
        Field(super::FieldCondition),
        #[prost(message, tag = "2")]
        IsEmpty(super::IsEmptyCondition),
        #[prost(message, tag = "3")]
        HasId(super::HasIdCondition),
        #[prost(message, tag = "4")]
        Filter(super::Filter),
        #[prost(message, tag = "5")]
        IsNull(super::IsNullCondition),
        #[prost(message, tag = "6")]
        Nested(super::NestedCondition),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsEmptyCondition {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNullCondition {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HasIdCondition {
    #[prost(message, repeated, tag = "1")]
    pub has_id: ::prost::alloc::vec::Vec<PointId>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NestedCondition {
    /// Path to nested object
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Filter condition
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldCondition {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    /// Check if point has field with a given value
    #[prost(message, optional, tag = "2")]
    pub r#match: ::core::option::Option<Match>,
    /// Check if points value lies in a given range
    #[prost(message, optional, tag = "3")]
    pub range: ::core::option::Option<Range>,
    /// Check if points geolocation lies in a given area
    #[prost(message, optional, tag = "4")]
    pub geo_bounding_box: ::core::option::Option<GeoBoundingBox>,
    /// Check if geo point is within a given radius
    #[prost(message, optional, tag = "5")]
    pub geo_radius: ::core::option::Option<GeoRadius>,
    /// Check number of values for a specific field
    #[prost(message, optional, tag = "6")]
    pub values_count: ::core::option::Option<ValuesCount>,
    /// Check if geo point is within a given polygon
    #[prost(message, optional, tag = "7")]
    pub geo_polygon: ::core::option::Option<GeoPolygon>,
    /// Check if datetime is within a given range
    #[prost(message, optional, tag = "8")]
    pub datetime_range: ::core::option::Option<DatetimeRange>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Match {
    #[prost(oneof = "r#match::MatchValue", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    pub match_value: ::core::option::Option<r#match::MatchValue>,
}
/// Nested message and enum types in `Match`.
pub mod r#match {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum MatchValue {
        /// Match string keyword
        #[prost(string, tag = "1")]
        Keyword(::prost::alloc::string::String),
        /// Match integer
        #[prost(int64, tag = "2")]
        Integer(i64),
        /// Match boolean
        #[prost(bool, tag = "3")]
        Boolean(bool),
        /// Match text
        #[prost(string, tag = "4")]
        Text(::prost::alloc::string::String),
        /// Match multiple keywords
        #[prost(message, tag = "5")]
        Keywords(super::RepeatedStrings),
        /// Match multiple integers
        #[prost(message, tag = "6")]
        Integers(super::RepeatedIntegers),
        /// Match any other value except those integers
        #[prost(message, tag = "7")]
        ExceptIntegers(super::RepeatedIntegers),
        /// Match any other value except those keywords
        #[prost(message, tag = "8")]
        ExceptKeywords(super::RepeatedStrings),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepeatedStrings {
    #[prost(string, repeated, tag = "1")]
    pub strings: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepeatedIntegers {
    #[prost(int64, repeated, tag = "1")]
    pub integers: ::prost::alloc::vec::Vec<i64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Range {
    #[prost(double, optional, tag = "1")]
    pub lt: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "2")]
    pub gt: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "3")]
    pub gte: ::core::option::Option<f64>,
    #[prost(double, optional, tag = "4")]
    pub lte: ::core::option::Option<f64>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatetimeRange {
    #[prost(message, optional, tag = "1")]
    #[validate(custom = "crate::grpc::validate::validate_timestamp")]
    pub lt: ::core::option::Option<::prost_wkt_types::Timestamp>,
    #[prost(message, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_timestamp")]
    pub gt: ::core::option::Option<::prost_wkt_types::Timestamp>,
    #[prost(message, optional, tag = "3")]
    #[validate(custom = "crate::grpc::validate::validate_timestamp")]
    pub gte: ::core::option::Option<::prost_wkt_types::Timestamp>,
    #[prost(message, optional, tag = "4")]
    #[validate(custom = "crate::grpc::validate::validate_timestamp")]
    pub lte: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoBoundingBox {
    /// north-west corner
    #[prost(message, optional, tag = "1")]
    pub top_left: ::core::option::Option<GeoPoint>,
    /// south-east corner
    #[prost(message, optional, tag = "2")]
    pub bottom_right: ::core::option::Option<GeoPoint>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoRadius {
    /// Center of the circle
    #[prost(message, optional, tag = "1")]
    pub center: ::core::option::Option<GeoPoint>,
    /// In meters
    #[prost(float, tag = "2")]
    pub radius: f32,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoLineString {
    /// Ordered sequence of GeoPoints representing the line
    #[prost(message, repeated, tag = "1")]
    pub points: ::prost::alloc::vec::Vec<GeoPoint>,
}
/// For a valid GeoPolygon, both the exterior and interior GeoLineStrings must consist of a minimum of 4 points.
/// Additionally, the first and last points of each GeoLineString must be the same.
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoPolygon {
    /// The exterior line bounds the surface
    #[prost(message, optional, tag = "1")]
    #[validate(custom = "crate::grpc::validate::validate_geo_polygon_exterior")]
    pub exterior: ::core::option::Option<GeoLineString>,
    /// Interior lines (if present) bound holes within the surface
    #[prost(message, repeated, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_geo_polygon_interiors")]
    pub interiors: ::prost::alloc::vec::Vec<GeoLineString>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValuesCount {
    #[prost(uint64, optional, tag = "1")]
    pub lt: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub gt: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub gte: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "4")]
    pub lte: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsSelector {
    #[prost(oneof = "points_selector::PointsSelectorOneOf", tags = "1, 2")]
    pub points_selector_one_of: ::core::option::Option<
        points_selector::PointsSelectorOneOf,
    >,
}
/// Nested message and enum types in `PointsSelector`.
pub mod points_selector {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PointsSelectorOneOf {
        #[prost(message, tag = "1")]
        Points(super::PointsIdsList),
        #[prost(message, tag = "2")]
        Filter(super::Filter),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsIdsList {
    #[prost(message, repeated, tag = "1")]
    pub ids: ::prost::alloc::vec::Vec<PointId>,
}
#[derive(validator::Validate)]
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointStruct {
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<PointId>,
    #[prost(map = "string, message", tag = "3")]
    pub payload: ::std::collections::HashMap<::prost::alloc::string::String, Value>,
    #[prost(message, optional, tag = "4")]
    #[validate]
    pub vectors: ::core::option::Option<Vectors>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GeoPoint {
    #[prost(double, tag = "1")]
    pub lon: f64,
    #[prost(double, tag = "2")]
    pub lat: f64,
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WriteOrderingType {
    /// Write operations may be reordered, works faster, default
    Weak = 0,
    /// Write operations go through dynamically selected leader, may be inconsistent for a short period of time in case of leader change
    Medium = 1,
    /// Write operations go through the permanent leader, consistent, but may be unavailable if leader is down
    Strong = 2,
}
impl WriteOrderingType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WriteOrderingType::Weak => "Weak",
            WriteOrderingType::Medium => "Medium",
            WriteOrderingType::Strong => "Strong",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Weak" => Some(Self::Weak),
            "Medium" => Some(Self::Medium),
            "Strong" => Some(Self::Strong),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ReadConsistencyType {
    /// Send request to all nodes and return points which are present on all of them
    All = 0,
    /// Send requests to all nodes and return points which are present on majority of them
    Majority = 1,
    /// Send requests to half + 1 nodes, return points which are present on all of them
    Quorum = 2,
}
impl ReadConsistencyType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ReadConsistencyType::All => "All",
            ReadConsistencyType::Majority => "Majority",
            ReadConsistencyType::Quorum => "Quorum",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "All" => Some(Self::All),
            "Majority" => Some(Self::Majority),
            "Quorum" => Some(Self::Quorum),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FieldType {
    Keyword = 0,
    Integer = 1,
    Float = 2,
    Geo = 3,
    Text = 4,
    Bool = 5,
    Datetime = 6,
}
impl FieldType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            FieldType::Keyword => "FieldTypeKeyword",
            FieldType::Integer => "FieldTypeInteger",
            FieldType::Float => "FieldTypeFloat",
            FieldType::Geo => "FieldTypeGeo",
            FieldType::Text => "FieldTypeText",
            FieldType::Bool => "FieldTypeBool",
            FieldType::Datetime => "FieldTypeDatetime",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "FieldTypeKeyword" => Some(Self::Keyword),
            "FieldTypeInteger" => Some(Self::Integer),
            "FieldTypeFloat" => Some(Self::Float),
            "FieldTypeGeo" => Some(Self::Geo),
            "FieldTypeText" => Some(Self::Text),
            "FieldTypeBool" => Some(Self::Bool),
            "FieldTypeDatetime" => Some(Self::Datetime),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Direction {
    Asc = 0,
    Desc = 1,
}
impl Direction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Direction::Asc => "Asc",
            Direction::Desc => "Desc",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Asc" => Some(Self::Asc),
            "Desc" => Some(Self::Desc),
            _ => None,
        }
    }
}
/// How to use positive and negative vectors to find the results, default is `AverageVector`:
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum RecommendStrategy {
    /// Average positive and negative vectors and create a single query with the formula
    /// `query = avg_pos + avg_pos - avg_neg`. Then performs normal search.
    AverageVector = 0,
    /// Uses custom search objective. Each candidate is compared against all
    /// examples, its score is then chosen from the `max(max_pos_score, max_neg_score)`.
    /// If the `max_neg_score` is chosen then it is squared and negated.
    BestScore = 1,
}
impl RecommendStrategy {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            RecommendStrategy::AverageVector => "AverageVector",
            RecommendStrategy::BestScore => "BestScore",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "AverageVector" => Some(Self::AverageVector),
            "BestScore" => Some(Self::BestScore),
            _ => None,
        }
    }
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UpdateStatus {
    UnknownUpdateStatus = 0,
    /// Update is received, but not processed yet
    Acknowledged = 1,
    /// Update is applied and ready for search
    Completed = 2,
    /// Internal: update is rejected due to an outdated clock
    ClockRejected = 3,
}
impl UpdateStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UpdateStatus::UnknownUpdateStatus => "UnknownUpdateStatus",
            UpdateStatus::Acknowledged => "Acknowledged",
            UpdateStatus::Completed => "Completed",
            UpdateStatus::ClockRejected => "ClockRejected",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UnknownUpdateStatus" => Some(Self::UnknownUpdateStatus),
            "Acknowledged" => Some(Self::Acknowledged),
            "Completed" => Some(Self::Completed),
            "ClockRejected" => Some(Self::ClockRejected),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod points_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct PointsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PointsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PointsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Perform insert + updates on points. If a point with a given ID already exists - it will be overwritten.
        pub async fn upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::UpsertPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Upsert"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete points
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Delete"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Retrieve points
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetPoints>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Get"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Update named vectors for point
        pub async fn update_vectors(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdatePointVectors>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/UpdateVectors",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "UpdateVectors"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete named vectors for points
        pub async fn delete_vectors(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePointVectors>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/DeleteVectors",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "DeleteVectors"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Set payload for points
        pub async fn set_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "SetPayload"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Overwrite payload for points
        pub async fn overwrite_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/OverwritePayload",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "OverwritePayload"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete specified key payload for points
        pub async fn delete_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "DeletePayload"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Remove all payload for specified points
        pub async fn clear_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::ClearPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "ClearPayload"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Create index for field in collection
        pub async fn create_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateFieldIndexCollection>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "CreateFieldIndex"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete field index for collection
        pub async fn delete_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteFieldIndexCollection>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "DeleteFieldIndex"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions
        pub async fn search(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchPoints>,
        ) -> std::result::Result<tonic::Response<super::SearchResponse>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Search"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions
        pub async fn search_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::SearchBatchResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/SearchBatch",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "SearchBatch"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions, grouped by a given field
        pub async fn search_groups(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchPointGroups>,
        ) -> std::result::Result<
            tonic::Response<super::SearchGroupsResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/SearchGroups",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "SearchGroups"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Iterate over all or filtered points
        pub async fn scroll(
            &mut self,
            request: impl tonic::IntoRequest<super::ScrollPoints>,
        ) -> std::result::Result<tonic::Response<super::ScrollResponse>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Scroll"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        pub async fn recommend(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendPoints>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Recommend"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        pub async fn recommend_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendBatchResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/RecommendBatch",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "RecommendBatch"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples, grouped by a given field
        pub async fn recommend_groups(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendPointGroups>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendGroupsResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/RecommendGroups",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "RecommendGroups"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Use context and a target to find the most similar points to the target, constrained by the context.
        ///
        /// When using only the context (without a target), a special search - called context search - is performed where
        /// pairs of points are used to generate a loss that guides the search towards the zone where
        /// most positive examples overlap. This means that the score minimizes the scenario of
        /// finding a point closer to a negative than to a positive part of a pair.
        ///
        /// Since the score of a context relates to loss, the maximum score a point can get is 0.0,
        /// and it becomes normal that many points can have a score of 0.0.
        ///
        /// When using target (with or without context), the score behaves a little different: The
        /// integer part of the score represents the rank with respect to the context, while the
        /// decimal part of the score relates to the distance to the target. The context part of the score for
        /// each pair is calculated +1 if the point is closer to a positive than to a negative part of a pair,
        /// and -1 otherwise.
        pub async fn discover(
            &mut self,
            request: impl tonic::IntoRequest<super::DiscoverPoints>,
        ) -> std::result::Result<
            tonic::Response<super::DiscoverResponse>,
            tonic::Status,
        > {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Discover");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Discover"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Batch request points based on { positive, negative } pairs of examples, and/or a target
        pub async fn discover_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::DiscoverBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::DiscoverBatchResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/DiscoverBatch",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Points", "DiscoverBatch"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Count points in collection with given filtering conditions
        pub async fn count(
            &mut self,
            request: impl tonic::IntoRequest<super::CountPoints>,
        ) -> std::result::Result<tonic::Response<super::CountResponse>, tonic::Status> {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Points/Count");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "Count"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Perform multiple update operations in one request
        pub async fn update_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateBatchResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Points/UpdateBatch",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Points", "UpdateBatch"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod points_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with PointsServer.
    #[async_trait]
    pub trait Points: Send + Sync + 'static {
        ///
        /// Perform insert + updates on points. If a point with a given ID already exists - it will be overwritten.
        async fn upsert(
            &self,
            request: tonic::Request<super::UpsertPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Delete points
        async fn delete(
            &self,
            request: tonic::Request<super::DeletePoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Retrieve points
        async fn get(
            &self,
            request: tonic::Request<super::GetPoints>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status>;
        ///
        /// Update named vectors for point
        async fn update_vectors(
            &self,
            request: tonic::Request<super::UpdatePointVectors>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Delete named vectors for points
        async fn delete_vectors(
            &self,
            request: tonic::Request<super::DeletePointVectors>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Set payload for points
        async fn set_payload(
            &self,
            request: tonic::Request<super::SetPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Overwrite payload for points
        async fn overwrite_payload(
            &self,
            request: tonic::Request<super::SetPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Delete specified key payload for points
        async fn delete_payload(
            &self,
            request: tonic::Request<super::DeletePayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Remove all payload for specified points
        async fn clear_payload(
            &self,
            request: tonic::Request<super::ClearPayloadPoints>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Create index for field in collection
        async fn create_field_index(
            &self,
            request: tonic::Request<super::CreateFieldIndexCollection>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Delete field index for collection
        async fn delete_field_index(
            &self,
            request: tonic::Request<super::DeleteFieldIndexCollection>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponse>,
            tonic::Status,
        >;
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions
        async fn search(
            &self,
            request: tonic::Request<super::SearchPoints>,
        ) -> std::result::Result<tonic::Response<super::SearchResponse>, tonic::Status>;
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions
        async fn search_batch(
            &self,
            request: tonic::Request<super::SearchBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::SearchBatchResponse>,
            tonic::Status,
        >;
        ///
        /// Retrieve closest points based on vector similarity and given filtering conditions, grouped by a given field
        async fn search_groups(
            &self,
            request: tonic::Request<super::SearchPointGroups>,
        ) -> std::result::Result<
            tonic::Response<super::SearchGroupsResponse>,
            tonic::Status,
        >;
        ///
        /// Iterate over all or filtered points
        async fn scroll(
            &self,
            request: tonic::Request<super::ScrollPoints>,
        ) -> std::result::Result<tonic::Response<super::ScrollResponse>, tonic::Status>;
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        async fn recommend(
            &self,
            request: tonic::Request<super::RecommendPoints>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendResponse>,
            tonic::Status,
        >;
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples.
        async fn recommend_batch(
            &self,
            request: tonic::Request<super::RecommendBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendBatchResponse>,
            tonic::Status,
        >;
        ///
        /// Look for the points which are closer to stored positive examples and at the same time further to negative examples, grouped by a given field
        async fn recommend_groups(
            &self,
            request: tonic::Request<super::RecommendPointGroups>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendGroupsResponse>,
            tonic::Status,
        >;
        ///
        /// Use context and a target to find the most similar points to the target, constrained by the context.
        ///
        /// When using only the context (without a target), a special search - called context search - is performed where
        /// pairs of points are used to generate a loss that guides the search towards the zone where
        /// most positive examples overlap. This means that the score minimizes the scenario of
        /// finding a point closer to a negative than to a positive part of a pair.
        ///
        /// Since the score of a context relates to loss, the maximum score a point can get is 0.0,
        /// and it becomes normal that many points can have a score of 0.0.
        ///
        /// When using target (with or without context), the score behaves a little different: The
        /// integer part of the score represents the rank with respect to the context, while the
        /// decimal part of the score relates to the distance to the target. The context part of the score for
        /// each pair is calculated +1 if the point is closer to a positive than to a negative part of a pair,
        /// and -1 otherwise.
        async fn discover(
            &self,
            request: tonic::Request<super::DiscoverPoints>,
        ) -> std::result::Result<
            tonic::Response<super::DiscoverResponse>,
            tonic::Status,
        >;
        ///
        /// Batch request points based on { positive, negative } pairs of examples, and/or a target
        async fn discover_batch(
            &self,
            request: tonic::Request<super::DiscoverBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::DiscoverBatchResponse>,
            tonic::Status,
        >;
        ///
        /// Count points in collection with given filtering conditions
        async fn count(
            &self,
            request: tonic::Request<super::CountPoints>,
        ) -> std::result::Result<tonic::Response<super::CountResponse>, tonic::Status>;
        ///
        /// Perform multiple update operations in one request
        async fn update_batch(
            &self,
            request: tonic::Request<super::UpdateBatchPoints>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateBatchResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct PointsServer<T: Points> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::upsert(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::delete(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::get(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/UpdateVectors" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateVectorsSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::UpdatePointVectors>
                    for UpdateVectorsSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdatePointVectors>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::update_vectors(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateVectorsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/DeleteVectors" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteVectorsSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::DeletePointVectors>
                    for DeleteVectorsSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePointVectors>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::delete_vectors(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteVectorsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::set_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/OverwritePayload" => {
                    #[allow(non_camel_case_types)]
                    struct OverwritePayloadSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::SetPayloadPoints>
                    for OverwritePayloadSvc<T> {
                        type Response = super::PointsOperationResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetPayloadPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::overwrite_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = OverwritePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::delete_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeletePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::clear_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ClearPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::create_field_index(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::delete_field_index(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::search(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/SearchBatch" => {
                    #[allow(non_camel_case_types)]
                    struct SearchBatchSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::SearchBatchPoints>
                    for SearchBatchSvc<T> {
                        type Response = super::SearchBatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchBatchPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::search_batch(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/SearchGroups" => {
                    #[allow(non_camel_case_types)]
                    struct SearchGroupsSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::SearchPointGroups>
                    for SearchGroupsSvc<T> {
                        type Response = super::SearchGroupsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SearchPointGroups>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::search_groups(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SearchGroupsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::scroll(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ScrollSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::recommend(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/RecommendBatch" => {
                    #[allow(non_camel_case_types)]
                    struct RecommendBatchSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::RecommendBatchPoints>
                    for RecommendBatchSvc<T> {
                        type Response = super::RecommendBatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecommendBatchPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::recommend_batch(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/RecommendGroups" => {
                    #[allow(non_camel_case_types)]
                    struct RecommendGroupsSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::RecommendPointGroups>
                    for RecommendGroupsSvc<T> {
                        type Response = super::RecommendGroupsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecommendPointGroups>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::recommend_groups(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendGroupsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Discover" => {
                    #[allow(non_camel_case_types)]
                    struct DiscoverSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::DiscoverPoints>
                    for DiscoverSvc<T> {
                        type Response = super::DiscoverResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DiscoverPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::discover(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DiscoverSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/DiscoverBatch" => {
                    #[allow(non_camel_case_types)]
                    struct DiscoverBatchSvc<T: Points>(pub Arc<T>);
                    impl<
                        T: Points,
                    > tonic::server::UnaryService<super::DiscoverBatchPoints>
                    for DiscoverBatchSvc<T> {
                        type Response = super::DiscoverBatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DiscoverBatchPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::discover_batch(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DiscoverBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/Count" => {
                    #[allow(non_camel_case_types)]
                    struct CountSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::CountPoints>
                    for CountSvc<T> {
                        type Response = super::CountResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CountPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::count(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CountSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Points/UpdateBatch" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateBatchSvc<T: Points>(pub Arc<T>);
                    impl<T: Points> tonic::server::UnaryService<super::UpdateBatchPoints>
                    for UpdateBatchSvc<T> {
                        type Response = super::UpdateBatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateBatchPoints>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Points>::update_batch(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Points> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Points> tonic::server::NamedService for PointsServer<T> {
        const NAME: &'static str = "qdrant.Points";
    }
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncPoints {
    /// name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Wait until the changes have been applied?
    #[prost(bool, optional, tag = "2")]
    pub wait: ::core::option::Option<bool>,
    #[prost(message, repeated, tag = "3")]
    pub points: ::prost::alloc::vec::Vec<PointStruct>,
    /// Start of the sync range
    #[prost(message, optional, tag = "4")]
    pub from_id: ::core::option::Option<PointId>,
    /// End of the sync range
    #[prost(message, optional, tag = "5")]
    pub to_id: ::core::option::Option<PointId>,
    #[prost(message, optional, tag = "6")]
    pub ordering: ::core::option::Option<WriteOrdering>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SyncPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub sync_points: ::core::option::Option<SyncPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpsertPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub upsert_points: ::core::option::Option<UpsertPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub delete_points: ::core::option::Option<DeletePoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateVectorsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub update_vectors: ::core::option::Option<UpdatePointVectors>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteVectorsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub delete_vectors: ::core::option::Option<DeletePointVectors>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SetPayloadPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub set_payload_points: ::core::option::Option<SetPayloadPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletePayloadPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub delete_payload_points: ::core::option::Option<DeletePayloadPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClearPayloadPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub clear_payload_points: ::core::option::Option<ClearPayloadPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateFieldIndexCollectionInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub create_field_index_collection: ::core::option::Option<
        CreateFieldIndexCollection,
    >,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteFieldIndexCollectionInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub delete_field_index_collection: ::core::option::Option<
        DeleteFieldIndexCollection,
    >,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
/// Has to be backward compatible with `PointsOperationResponse`!
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PointsOperationResponseInternal {
    #[prost(message, optional, tag = "1")]
    pub result: ::core::option::Option<UpdateResultInternal>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
/// Has to be backward compatible with `UpdateResult`!
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateResultInternal {
    /// Number of operation
    #[prost(uint64, optional, tag = "1")]
    pub operation_id: ::core::option::Option<u64>,
    /// Operation status
    #[prost(enumeration = "UpdateStatus", tag = "2")]
    pub status: i32,
    #[prost(message, optional, tag = "3")]
    pub clock_tag: ::core::option::Option<ClockTag>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClockTag {
    #[prost(uint64, tag = "1")]
    pub peer_id: u64,
    #[prost(uint32, tag = "2")]
    pub clock_id: u32,
    #[prost(uint64, tag = "3")]
    pub clock_tick: u64,
    #[prost(uint64, tag = "4")]
    pub token: u64,
    #[prost(bool, tag = "5")]
    pub force: bool,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub search_points: ::core::option::Option<SearchPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SearchBatchPointsInternal {
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub search_points: ::prost::alloc::vec::Vec<SearchPoints>,
    #[prost(uint32, optional, tag = "3")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(uint64, optional, tag = "4")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoQuery {
    #[prost(message, repeated, tag = "1")]
    #[validate]
    pub positives: ::prost::alloc::vec::Vec<Vector>,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub negatives: ::prost::alloc::vec::Vec<Vector>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContextPair {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub positive: ::core::option::Option<Vector>,
    #[prost(message, optional, tag = "2")]
    #[validate]
    pub negative: ::core::option::Option<Vector>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DiscoveryQuery {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub target: ::core::option::Option<Vector>,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub context: ::prost::alloc::vec::Vec<ContextPair>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContextQuery {
    #[prost(message, repeated, tag = "1")]
    #[validate]
    pub context: ::prost::alloc::vec::Vec<ContextPair>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryEnum {
    #[prost(oneof = "query_enum::Query", tags = "1, 2, 3, 4")]
    pub query: ::core::option::Option<query_enum::Query>,
}
/// Nested message and enum types in `QueryEnum`.
pub mod query_enum {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Query {
        /// ANN
        #[prost(message, tag = "1")]
        NearestNeighbors(super::Vector),
        /// Recommend points with higher similarity to positive examples
        #[prost(message, tag = "2")]
        RecommendBestScore(super::RecoQuery),
        /// Search for points that get closer to a target, constrained by a context of positive and negative pairs
        #[prost(message, tag = "3")]
        Discover(super::DiscoveryQuery),
        /// Use only the context to find points that minimize loss against negative examples
        #[prost(message, tag = "4")]
        Context(super::ContextQuery),
    }
}
/// This is only used internally, so it makes more sense to add it here rather than in points.proto
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoreSearchPoints {
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub query: ::core::option::Option<QueryEnum>,
    #[prost(message, optional, tag = "3")]
    #[validate]
    pub filter: ::core::option::Option<Filter>,
    #[prost(uint64, tag = "4")]
    #[validate(range(min = 1))]
    pub limit: u64,
    #[prost(message, optional, tag = "5")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    #[prost(message, optional, tag = "6")]
    #[validate]
    pub params: ::core::option::Option<SearchParams>,
    #[prost(float, optional, tag = "7")]
    pub score_threshold: ::core::option::Option<f32>,
    #[prost(uint64, optional, tag = "8")]
    pub offset: ::core::option::Option<u64>,
    #[prost(string, optional, tag = "9")]
    pub vector_name: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "10")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
    #[prost(message, optional, tag = "11")]
    pub read_consistency: ::core::option::Option<ReadConsistency>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoreSearchBatchPointsInternal {
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    #[validate]
    pub search_points: ::prost::alloc::vec::Vec<CoreSearchPoints>,
    #[prost(uint32, optional, tag = "3")]
    pub shard_id: ::core::option::Option<u32>,
    #[prost(uint64, optional, tag = "4")]
    pub timeout: ::core::option::Option<u64>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScrollPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub scroll_points: ::core::option::Option<ScrollPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecommendPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub recommend_points: ::core::option::Option<RecommendPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub get_points: ::core::option::Option<GetPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CountPointsInternal {
    #[prost(message, optional, tag = "1")]
    #[validate]
    pub count_points: ::core::option::Option<CountPoints>,
    #[prost(uint32, optional, tag = "2")]
    pub shard_id: ::core::option::Option<u32>,
}
/// A bare vector. No id reference here.
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RawVector {
    #[prost(oneof = "raw_vector::Variant", tags = "1, 2, 3")]
    pub variant: ::core::option::Option<raw_vector::Variant>,
}
/// Nested message and enum types in `RawVector`.
pub mod raw_vector {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Variant {
        #[prost(message, tag = "1")]
        Dense(super::DenseVector),
        #[prost(message, tag = "2")]
        Sparse(super::SparseVector),
        #[prost(message, tag = "3")]
        MultiDense(super::MultiDenseVector),
    }
}
/// Query variants for raw vectors (ids have been substituted with vectors)
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RawQuery {
    #[prost(oneof = "raw_query::Variant", tags = "1, 2, 3, 4")]
    pub variant: ::core::option::Option<raw_query::Variant>,
}
/// Nested message and enum types in `RawQuery`.
pub mod raw_query {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Recommend {
        #[prost(message, repeated, tag = "1")]
        pub positives: ::prost::alloc::vec::Vec<super::RawVector>,
        #[prost(message, repeated, tag = "2")]
        pub negatives: ::prost::alloc::vec::Vec<super::RawVector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct RawContextPair {
        #[prost(message, optional, tag = "1")]
        pub positive: ::core::option::Option<super::RawVector>,
        #[prost(message, optional, tag = "2")]
        pub negative: ::core::option::Option<super::RawVector>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Discovery {
        #[prost(message, optional, tag = "1")]
        pub target: ::core::option::Option<super::RawVector>,
        #[prost(message, repeated, tag = "2")]
        pub context: ::prost::alloc::vec::Vec<RawContextPair>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Context {
        #[prost(message, repeated, tag = "1")]
        pub context: ::prost::alloc::vec::Vec<RawContextPair>,
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Variant {
        /// ANN
        #[prost(message, tag = "1")]
        Nearest(super::RawVector),
        /// Recommend points with highest similarity to positive examples, or lowest to negative examples
        #[prost(message, tag = "2")]
        RecommendBestScore(Recommend),
        /// Search for points that get closer to a target, constrained by a context of positive and negative pairs
        #[prost(message, tag = "3")]
        Discover(Discovery),
        /// Use only the context to find points that minimize loss against negative examples
        #[prost(message, tag = "4")]
        Context(Context),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryShardPoints {
    #[prost(message, repeated, tag = "1")]
    pub prefetch: ::prost::alloc::vec::Vec<query_shard_points::Prefetch>,
    #[prost(message, optional, tag = "2")]
    pub query: ::core::option::Option<query_shard_points::Query>,
    #[prost(string, optional, tag = "3")]
    pub using: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub filter: ::core::option::Option<Filter>,
    #[prost(uint64, tag = "5")]
    pub limit: u64,
    #[prost(message, optional, tag = "6")]
    pub params: ::core::option::Option<SearchParams>,
    #[prost(float, optional, tag = "7")]
    pub score_threshold: ::core::option::Option<f32>,
    #[prost(uint64, tag = "8")]
    pub offset: u64,
    #[prost(message, optional, tag = "9")]
    pub with_payload: ::core::option::Option<WithPayloadSelector>,
    #[prost(message, optional, tag = "10")]
    pub with_vectors: ::core::option::Option<WithVectorsSelector>,
}
/// Nested message and enum types in `QueryShardPoints`.
pub mod query_shard_points {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Query {
        #[prost(oneof = "query::Score", tags = "1, 2")]
        pub score: ::core::option::Option<query::Score>,
    }
    /// Nested message and enum types in `Query`.
    pub mod query {
        #[derive(serde::Serialize)]
        #[allow(clippy::derive_partial_eq_without_eq)]
        #[derive(Clone, PartialEq, ::prost::Oneof)]
        pub enum Score {
            /// (re)score against a vector query
            #[prost(message, tag = "1")]
            Vector(super::super::RawQuery),
            /// Reciprocal Rank Fusion
            #[prost(bool, tag = "2")]
            Rrf(bool),
        }
    }
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Prefetch {
        #[prost(message, repeated, tag = "1")]
        pub prefetch: ::prost::alloc::vec::Vec<Prefetch>,
        #[prost(message, optional, tag = "2")]
        pub query: ::core::option::Option<Query>,
        #[prost(string, optional, tag = "3")]
        pub using: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(message, optional, tag = "4")]
        pub filter: ::core::option::Option<super::Filter>,
        #[prost(uint64, tag = "5")]
        pub limit: u64,
        #[prost(message, optional, tag = "6")]
        pub params: ::core::option::Option<super::SearchParams>,
        #[prost(float, optional, tag = "7")]
        pub score_threshold: ::core::option::Option<f32>,
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryPointsInternal {
    #[prost(string, tag = "1")]
    pub collection_name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub query_points: ::core::option::Option<QueryShardPoints>,
    #[prost(uint32, optional, tag = "3")]
    pub shard_id: ::core::option::Option<u32>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntermediateResult {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<ScoredPoint>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResponse {
    #[prost(message, repeated, tag = "1")]
    pub result: ::prost::alloc::vec::Vec<IntermediateResult>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
/// Generated client implementations.
pub mod points_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct PointsInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl PointsInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> PointsInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn upsert(
            &mut self,
            request: impl tonic::IntoRequest<super::UpsertPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Upsert"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn sync(
            &mut self,
            request: impl tonic::IntoRequest<super::SyncPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
                "/qdrant.PointsInternal/Sync",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Sync"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Delete"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn update_vectors(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateVectorsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
                "/qdrant.PointsInternal/UpdateVectors",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "UpdateVectors"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_vectors(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteVectorsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
                "/qdrant.PointsInternal/DeleteVectors",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "DeleteVectors"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn set_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "SetPayload"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn overwrite_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::SetPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
                "/qdrant.PointsInternal/OverwritePayload",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "OverwritePayload"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "DeletePayload"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn clear_payload(
            &mut self,
            request: impl tonic::IntoRequest<super::ClearPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "ClearPayload"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateFieldIndexCollectionInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "CreateFieldIndex"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn delete_field_index(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteFieldIndexCollectionInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "DeleteFieldIndex"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn core_search_batch(
            &mut self,
            request: impl tonic::IntoRequest<super::CoreSearchBatchPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::SearchBatchResponse>,
            tonic::Status,
        > {
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
                "/qdrant.PointsInternal/CoreSearchBatch",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "CoreSearchBatch"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn scroll(
            &mut self,
            request: impl tonic::IntoRequest<super::ScrollPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::ScrollResponse>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Scroll"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn count(
            &mut self,
            request: impl tonic::IntoRequest<super::CountPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::CountResponse>, tonic::Status> {
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
                "/qdrant.PointsInternal/Count",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Count"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn recommend(
            &mut self,
            request: impl tonic::IntoRequest<super::RecommendPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendResponse>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Recommend"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get(
            &mut self,
            request: impl tonic::IntoRequest<super::GetPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.PointsInternal", "Get"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn query(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::QueryResponse>, tonic::Status> {
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
                "/qdrant.PointsInternal/Query",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.PointsInternal", "Query"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod points_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with PointsInternalServer.
    #[async_trait]
    pub trait PointsInternal: Send + Sync + 'static {
        async fn upsert(
            &self,
            request: tonic::Request<super::UpsertPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn sync(
            &self,
            request: tonic::Request<super::SyncPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn delete(
            &self,
            request: tonic::Request<super::DeletePointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn update_vectors(
            &self,
            request: tonic::Request<super::UpdateVectorsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn delete_vectors(
            &self,
            request: tonic::Request<super::DeleteVectorsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn set_payload(
            &self,
            request: tonic::Request<super::SetPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn overwrite_payload(
            &self,
            request: tonic::Request<super::SetPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn delete_payload(
            &self,
            request: tonic::Request<super::DeletePayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn clear_payload(
            &self,
            request: tonic::Request<super::ClearPayloadPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn create_field_index(
            &self,
            request: tonic::Request<super::CreateFieldIndexCollectionInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn delete_field_index(
            &self,
            request: tonic::Request<super::DeleteFieldIndexCollectionInternal>,
        ) -> std::result::Result<
            tonic::Response<super::PointsOperationResponseInternal>,
            tonic::Status,
        >;
        async fn core_search_batch(
            &self,
            request: tonic::Request<super::CoreSearchBatchPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::SearchBatchResponse>,
            tonic::Status,
        >;
        async fn scroll(
            &self,
            request: tonic::Request<super::ScrollPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::ScrollResponse>, tonic::Status>;
        async fn count(
            &self,
            request: tonic::Request<super::CountPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::CountResponse>, tonic::Status>;
        async fn recommend(
            &self,
            request: tonic::Request<super::RecommendPointsInternal>,
        ) -> std::result::Result<
            tonic::Response<super::RecommendResponse>,
            tonic::Status,
        >;
        async fn get(
            &self,
            request: tonic::Request<super::GetPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::GetResponse>, tonic::Status>;
        async fn query(
            &self,
            request: tonic::Request<super::QueryPointsInternal>,
        ) -> std::result::Result<tonic::Response<super::QueryResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct PointsInternalServer<T: PointsInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpsertPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::upsert(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpsertSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Sync" => {
                    #[allow(non_camel_case_types)]
                    struct SyncSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::SyncPointsInternal>
                    for SyncSvc<T> {
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SyncPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::sync(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SyncSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::delete(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/UpdateVectors" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateVectorsSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::UpdateVectorsInternal>
                    for UpdateVectorsSvc<T> {
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateVectorsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::update_vectors(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateVectorsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/DeleteVectors" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteVectorsSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::DeleteVectorsInternal>
                    for DeleteVectorsSvc<T> {
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteVectorsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::delete_vectors(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteVectorsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetPayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::set_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SetPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/OverwritePayload" => {
                    #[allow(non_camel_case_types)]
                    struct OverwritePayloadSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::SetPayloadPointsInternal>
                    for OverwritePayloadSvc<T> {
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SetPayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::overwrite_payload(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = OverwritePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::delete_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeletePayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ClearPayloadPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::clear_payload(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ClearPayloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::create_field_index(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                        type Response = super::PointsOperationResponseInternal;
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::delete_field_index(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteFieldIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/CoreSearchBatch" => {
                    #[allow(non_camel_case_types)]
                    struct CoreSearchBatchSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::CoreSearchBatchPointsInternal>
                    for CoreSearchBatchSvc<T> {
                        type Response = super::SearchBatchResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CoreSearchBatchPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::core_search_batch(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CoreSearchBatchSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::scroll(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ScrollSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Count" => {
                    #[allow(non_camel_case_types)]
                    struct CountSvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::CountPointsInternal>
                    for CountSvc<T> {
                        type Response = super::CountResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CountPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::count(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CountSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::recommend(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecommendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::get(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.PointsInternal/Query" => {
                    #[allow(non_camel_case_types)]
                    struct QuerySvc<T: PointsInternal>(pub Arc<T>);
                    impl<
                        T: PointsInternal,
                    > tonic::server::UnaryService<super::QueryPointsInternal>
                    for QuerySvc<T> {
                        type Response = super::QueryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::QueryPointsInternal>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as PointsInternal>::query(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = QuerySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: PointsInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: PointsInternal> tonic::server::NamedService for PointsInternalServer<T> {
        const NAME: &'static str = "qdrant.PointsInternal";
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetConsensusCommitRequest {}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetConsensusCommitResponse {
    /// Raft commit as u64
    #[prost(int64, tag = "1")]
    pub commit: i64,
    /// Raft term as u64
    #[prost(int64, tag = "2")]
    pub term: i64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitOnConsensusCommitRequest {
    /// Raft commit as u64
    #[prost(int64, tag = "1")]
    pub commit: i64,
    /// Raft term as u64
    #[prost(int64, tag = "2")]
    pub term: i64,
    /// Timeout in seconds
    #[prost(int64, tag = "3")]
    pub timeout: i64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitOnConsensusCommitResponse {
    /// False if commit/term is diverged and never reached or if timed out.
    #[prost(bool, tag = "1")]
    pub ok: bool,
}
/// Generated client implementations.
pub mod qdrant_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct QdrantInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl QdrantInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> QdrantInternalClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> QdrantInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
            QdrantInternalClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Get current commit and term on the target node.
        pub async fn get_consensus_commit(
            &mut self,
            request: impl tonic::IntoRequest<super::GetConsensusCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetConsensusCommitResponse>,
            tonic::Status,
        > {
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
                "/qdrant.QdrantInternal/GetConsensusCommit",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.QdrantInternal", "GetConsensusCommit"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Wait until the target node reached the given commit ID.
        pub async fn wait_on_consensus_commit(
            &mut self,
            request: impl tonic::IntoRequest<super::WaitOnConsensusCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WaitOnConsensusCommitResponse>,
            tonic::Status,
        > {
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
                "/qdrant.QdrantInternal/WaitOnConsensusCommit",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("qdrant.QdrantInternal", "WaitOnConsensusCommit"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod qdrant_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with QdrantInternalServer.
    #[async_trait]
    pub trait QdrantInternal: Send + Sync + 'static {
        ///
        /// Get current commit and term on the target node.
        async fn get_consensus_commit(
            &self,
            request: tonic::Request<super::GetConsensusCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetConsensusCommitResponse>,
            tonic::Status,
        >;
        ///
        /// Wait until the target node reached the given commit ID.
        async fn wait_on_consensus_commit(
            &self,
            request: tonic::Request<super::WaitOnConsensusCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::WaitOnConsensusCommitResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct QdrantInternalServer<T: QdrantInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: QdrantInternal> QdrantInternalServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for QdrantInternalServer<T>
    where
        T: QdrantInternal,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.QdrantInternal/GetConsensusCommit" => {
                    #[allow(non_camel_case_types)]
                    struct GetConsensusCommitSvc<T: QdrantInternal>(pub Arc<T>);
                    impl<
                        T: QdrantInternal,
                    > tonic::server::UnaryService<super::GetConsensusCommitRequest>
                    for GetConsensusCommitSvc<T> {
                        type Response = super::GetConsensusCommitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetConsensusCommitRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as QdrantInternal>::get_consensus_commit(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetConsensusCommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.QdrantInternal/WaitOnConsensusCommit" => {
                    #[allow(non_camel_case_types)]
                    struct WaitOnConsensusCommitSvc<T: QdrantInternal>(pub Arc<T>);
                    impl<
                        T: QdrantInternal,
                    > tonic::server::UnaryService<super::WaitOnConsensusCommitRequest>
                    for WaitOnConsensusCommitSvc<T> {
                        type Response = super::WaitOnConsensusCommitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WaitOnConsensusCommitRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as QdrantInternal>::wait_on_consensus_commit(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WaitOnConsensusCommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
    impl<T: QdrantInternal> Clone for QdrantInternalServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: QdrantInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: QdrantInternal> tonic::server::NamedService for QdrantInternalServer<T> {
        const NAME: &'static str = "qdrant.QdrantInternal";
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RaftMessage {
    #[prost(bytes = "vec", tag = "1")]
    pub message: ::prost::alloc::vec::Vec<u8>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllPeers {
    #[prost(message, repeated, tag = "1")]
    pub all_peers: ::prost::alloc::vec::Vec<Peer>,
    #[prost(uint64, tag = "2")]
    pub first_peer_id: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Peer {
    #[prost(string, tag = "1")]
    pub uri: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub id: u64,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddPeerToKnownMessage {
    #[prost(string, optional, tag = "1")]
    #[validate(custom = "common::validation::validate_not_empty")]
    pub uri: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint32, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_u32_range_min_1")]
    pub port: ::core::option::Option<u32>,
    #[prost(uint64, tag = "3")]
    pub id: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerId {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Uri {
    #[prost(string, tag = "1")]
    pub uri: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod raft_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> RaftClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Send Raft message to another peer
        pub async fn send(
            &mut self,
            request: impl tonic::IntoRequest<super::RaftMessage>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Raft", "Send"));
            self.inner.unary(req, path, codec).await
        }
        /// Send to bootstrap peer
        /// Returns uri by id if bootstrap knows this peer
        pub async fn who_is(
            &mut self,
            request: impl tonic::IntoRequest<super::PeerId>,
        ) -> std::result::Result<tonic::Response<super::Uri>, tonic::Status> {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Raft/WhoIs");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Raft", "WhoIs"));
            self.inner.unary(req, path, codec).await
        }
        /// Send to bootstrap peer
        /// Adds peer to the network
        /// Returns all peers
        pub async fn add_peer_to_known(
            &mut self,
            request: impl tonic::IntoRequest<super::AddPeerToKnownMessage>,
        ) -> std::result::Result<tonic::Response<super::AllPeers>, tonic::Status> {
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
                "/qdrant.Raft/AddPeerToKnown",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Raft", "AddPeerToKnown"));
            self.inner.unary(req, path, codec).await
        }
        /// DEPRECATED
        /// Its functionality is now included in `AddPeerToKnown`
        ///
        /// Send to bootstrap peer
        /// Proposes to add this peer as participant of consensus
        pub async fn add_peer_as_participant(
            &mut self,
            request: impl tonic::IntoRequest<super::PeerId>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
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
                "/qdrant.Raft/AddPeerAsParticipant",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Raft", "AddPeerAsParticipant"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod raft_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with RaftServer.
    #[async_trait]
    pub trait Raft: Send + Sync + 'static {
        /// Send Raft message to another peer
        async fn send(
            &self,
            request: tonic::Request<super::RaftMessage>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Send to bootstrap peer
        /// Returns uri by id if bootstrap knows this peer
        async fn who_is(
            &self,
            request: tonic::Request<super::PeerId>,
        ) -> std::result::Result<tonic::Response<super::Uri>, tonic::Status>;
        /// Send to bootstrap peer
        /// Adds peer to the network
        /// Returns all peers
        async fn add_peer_to_known(
            &self,
            request: tonic::Request<super::AddPeerToKnownMessage>,
        ) -> std::result::Result<tonic::Response<super::AllPeers>, tonic::Status>;
        /// DEPRECATED
        /// Its functionality is now included in `AddPeerToKnown`
        ///
        /// Send to bootstrap peer
        /// Proposes to add this peer as participant of consensus
        async fn add_peer_as_participant(
            &self,
            request: tonic::Request<super::PeerId>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RaftServer<T: Raft> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Raft>::send(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SendSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Raft/WhoIs" => {
                    #[allow(non_camel_case_types)]
                    struct WhoIsSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::PeerId>
                    for WhoIsSvc<T> {
                        type Response = super::Uri;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PeerId>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Raft>::who_is(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WhoIsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Raft/AddPeerToKnown" => {
                    #[allow(non_camel_case_types)]
                    struct AddPeerToKnownSvc<T: Raft>(pub Arc<T>);
                    impl<
                        T: Raft,
                    > tonic::server::UnaryService<super::AddPeerToKnownMessage>
                    for AddPeerToKnownSvc<T> {
                        type Response = super::AllPeers;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddPeerToKnownMessage>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Raft>::add_peer_to_known(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddPeerToKnownSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Raft/AddPeerAsParticipant" => {
                    #[allow(non_camel_case_types)]
                    struct AddPeerAsParticipantSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::PeerId>
                    for AddPeerAsParticipantSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PeerId>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Raft>::add_peer_as_participant(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddPeerAsParticipantSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Raft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Raft> tonic::server::NamedService for RaftServer<T> {
        const NAME: &'static str = "qdrant.Raft";
    }
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateFullSnapshotRequest {}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListFullSnapshotsRequest {}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteFullSnapshotRequest {
    /// Name of the full snapshot
    #[prost(string, tag = "1")]
    #[validate(length(min = 1))]
    pub snapshot_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSnapshotsRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Name of the collection snapshot
    #[prost(string, tag = "2")]
    #[validate(length(min = 1))]
    pub snapshot_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SnapshotDescription {
    /// Name of the snapshot
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Creation time of the snapshot
    #[prost(message, optional, tag = "2")]
    #[validate(custom = "crate::grpc::validate::validate_timestamp")]
    pub creation_time: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Size of the snapshot in bytes
    #[prost(int64, tag = "3")]
    pub size: i64,
    /// SHA256 digest of the snapshot file
    #[prost(string, optional, tag = "4")]
    pub checksum: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateSnapshotResponse {
    #[prost(message, optional, tag = "1")]
    pub snapshot_description: ::core::option::Option<SnapshotDescription>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSnapshotsResponse {
    #[prost(message, repeated, tag = "1")]
    pub snapshot_descriptions: ::prost::alloc::vec::Vec<SnapshotDescription>,
    /// Time spent to process
    #[prost(double, tag = "2")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteSnapshotResponse {
    /// Time spent to process
    #[prost(double, tag = "1")]
    pub time: f64,
}
/// Generated client implementations.
pub mod snapshots_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct SnapshotsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SnapshotsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> SnapshotsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> SnapshotsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
            SnapshotsClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Create collection snapshot
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        > {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Snapshots/Create");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Snapshots", "Create"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// List collection snapshots
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        > {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Snapshots/List");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Snapshots", "List"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete collection snapshot
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        > {
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
            let path = http::uri::PathAndQuery::from_static("/qdrant.Snapshots/Delete");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Snapshots", "Delete"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Create full storage snapshot
        pub async fn create_full(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateFullSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Snapshots/CreateFull",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Snapshots", "CreateFull"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// List full storage snapshots
        pub async fn list_full(
            &mut self,
            request: impl tonic::IntoRequest<super::ListFullSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Snapshots/ListFull",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Snapshots", "ListFull"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete full storage snapshot
        pub async fn delete_full(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteFullSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        > {
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
                "/qdrant.Snapshots/DeleteFull",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.Snapshots", "DeleteFull"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod snapshots_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with SnapshotsServer.
    #[async_trait]
    pub trait Snapshots: Send + Sync + 'static {
        ///
        /// Create collection snapshot
        async fn create(
            &self,
            request: tonic::Request<super::CreateSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        >;
        ///
        /// List collection snapshots
        async fn list(
            &self,
            request: tonic::Request<super::ListSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        >;
        ///
        /// Delete collection snapshot
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        >;
        ///
        /// Create full storage snapshot
        async fn create_full(
            &self,
            request: tonic::Request<super::CreateFullSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        >;
        ///
        /// List full storage snapshots
        async fn list_full(
            &self,
            request: tonic::Request<super::ListFullSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        >;
        ///
        /// Delete full storage snapshot
        async fn delete_full(
            &self,
            request: tonic::Request<super::DeleteFullSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct SnapshotsServer<T: Snapshots> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Snapshots> SnapshotsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for SnapshotsServer<T>
    where
        T: Snapshots,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.Snapshots/Create" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::CreateSnapshotRequest>
                    for CreateSvc<T> {
                        type Response = super::CreateSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::create(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Snapshots/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::ListSnapshotsRequest>
                    for ListSvc<T> {
                        type Response = super::ListSnapshotsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSnapshotsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::list(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Snapshots/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::DeleteSnapshotRequest>
                    for DeleteSvc<T> {
                        type Response = super::DeleteSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::delete(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Snapshots/CreateFull" => {
                    #[allow(non_camel_case_types)]
                    struct CreateFullSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::CreateFullSnapshotRequest>
                    for CreateFullSvc<T> {
                        type Response = super::CreateSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateFullSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::create_full(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateFullSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Snapshots/ListFull" => {
                    #[allow(non_camel_case_types)]
                    struct ListFullSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::ListFullSnapshotsRequest>
                    for ListFullSvc<T> {
                        type Response = super::ListSnapshotsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListFullSnapshotsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::list_full(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListFullSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.Snapshots/DeleteFull" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteFullSvc<T: Snapshots>(pub Arc<T>);
                    impl<
                        T: Snapshots,
                    > tonic::server::UnaryService<super::DeleteFullSnapshotRequest>
                    for DeleteFullSvc<T> {
                        type Response = super::DeleteSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteFullSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Snapshots>::delete_full(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteFullSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
    impl<T: Snapshots> Clone for SnapshotsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Snapshots> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Snapshots> tonic::server::NamedService for SnapshotsServer<T> {
        const NAME: &'static str = "qdrant.Snapshots";
    }
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateShardSnapshotRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListShardSnapshotsRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteShardSnapshotRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
    /// Name of the shard snapshot
    #[prost(string, tag = "3")]
    #[validate(length(min = 1))]
    pub snapshot_name: ::prost::alloc::string::String,
}
#[derive(serde::Serialize)]
#[derive(validator::Validate)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverShardSnapshotRequest {
    /// Name of the collection
    #[prost(string, tag = "1")]
    #[validate(length(min = 1, max = 255))]
    pub collection_name: ::prost::alloc::string::String,
    /// Id of the shard
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
    /// Location of the shard snapshot
    #[prost(message, optional, tag = "3")]
    pub snapshot_location: ::core::option::Option<ShardSnapshotLocation>,
    /// Priority of the shard snapshot
    #[prost(enumeration = "ShardSnapshotPriority", tag = "4")]
    pub snapshot_priority: i32,
    /// SHA256 checksum for verifying snapshot integrity
    #[prost(string, optional, tag = "5")]
    #[validate(custom = "common::validation::validate_sha256_hash_option")]
    pub checksum: ::core::option::Option<::prost::alloc::string::String>,
    /// Optional API key used when fetching the snapshot from a remote URL
    #[prost(string, optional, tag = "6")]
    pub api_key: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShardSnapshotLocation {
    #[prost(oneof = "shard_snapshot_location::Location", tags = "1, 2")]
    pub location: ::core::option::Option<shard_snapshot_location::Location>,
}
/// Nested message and enum types in `ShardSnapshotLocation`.
pub mod shard_snapshot_location {
    #[derive(serde::Serialize)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Location {
        /// URL of the remote shard snapshot
        #[prost(string, tag = "1")]
        Url(::prost::alloc::string::String),
        /// Path of the local shard snapshot
        #[prost(string, tag = "2")]
        Path(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverSnapshotResponse {
    /// Time in seconds spent to process request
    #[prost(double, tag = "1")]
    pub time: f64,
}
#[derive(serde::Serialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ShardSnapshotPriority {
    /// Restore snapshot without *any* additional synchronization
    NoSync = 0,
    /// Prefer snapshot data over the current state
    Snapshot = 1,
    /// Prefer existing data over the snapshot
    Replica = 2,
    /// Internal priority to use during snapshot shard transfer
    ShardTransfer = 3,
}
impl ShardSnapshotPriority {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ShardSnapshotPriority::NoSync => "ShardSnapshotPriorityNoSync",
            ShardSnapshotPriority::Snapshot => "ShardSnapshotPrioritySnapshot",
            ShardSnapshotPriority::Replica => "ShardSnapshotPriorityReplica",
            ShardSnapshotPriority::ShardTransfer => "ShardSnapshotPriorityShardTransfer",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ShardSnapshotPriorityNoSync" => Some(Self::NoSync),
            "ShardSnapshotPrioritySnapshot" => Some(Self::Snapshot),
            "ShardSnapshotPriorityReplica" => Some(Self::Replica),
            "ShardSnapshotPriorityShardTransfer" => Some(Self::ShardTransfer),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod shard_snapshots_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ShardSnapshotsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShardSnapshotsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShardSnapshotsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShardSnapshotsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
            ShardSnapshotsClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        ///
        /// Create shard snapshot
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        > {
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
                "/qdrant.ShardSnapshots/Create",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.ShardSnapshots", "Create"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// List shard snapshots
        pub async fn list(
            &mut self,
            request: impl tonic::IntoRequest<super::ListShardSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        > {
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
                "/qdrant.ShardSnapshots/List",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.ShardSnapshots", "List"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Delete shard snapshot
        pub async fn delete(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        > {
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
                "/qdrant.ShardSnapshots/Delete",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.ShardSnapshots", "Delete"));
            self.inner.unary(req, path, codec).await
        }
        ///
        /// Recover shard snapshot
        pub async fn recover(
            &mut self,
            request: impl tonic::IntoRequest<super::RecoverShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RecoverSnapshotResponse>,
            tonic::Status,
        > {
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
                "/qdrant.ShardSnapshots/Recover",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("qdrant.ShardSnapshots", "Recover"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod shard_snapshots_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ShardSnapshotsServer.
    #[async_trait]
    pub trait ShardSnapshots: Send + Sync + 'static {
        ///
        /// Create shard snapshot
        async fn create(
            &self,
            request: tonic::Request<super::CreateShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateSnapshotResponse>,
            tonic::Status,
        >;
        ///
        /// List shard snapshots
        async fn list(
            &self,
            request: tonic::Request<super::ListShardSnapshotsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSnapshotsResponse>,
            tonic::Status,
        >;
        ///
        /// Delete shard snapshot
        async fn delete(
            &self,
            request: tonic::Request<super::DeleteShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DeleteSnapshotResponse>,
            tonic::Status,
        >;
        ///
        /// Recover shard snapshot
        async fn recover(
            &self,
            request: tonic::Request<super::RecoverShardSnapshotRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RecoverSnapshotResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ShardSnapshotsServer<T: ShardSnapshots> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ShardSnapshots> ShardSnapshotsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ShardSnapshotsServer<T>
    where
        T: ShardSnapshots,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/qdrant.ShardSnapshots/Create" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSvc<T: ShardSnapshots>(pub Arc<T>);
                    impl<
                        T: ShardSnapshots,
                    > tonic::server::UnaryService<super::CreateShardSnapshotRequest>
                    for CreateSvc<T> {
                        type Response = super::CreateSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateShardSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ShardSnapshots>::create(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.ShardSnapshots/List" => {
                    #[allow(non_camel_case_types)]
                    struct ListSvc<T: ShardSnapshots>(pub Arc<T>);
                    impl<
                        T: ShardSnapshots,
                    > tonic::server::UnaryService<super::ListShardSnapshotsRequest>
                    for ListSvc<T> {
                        type Response = super::ListSnapshotsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListShardSnapshotsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ShardSnapshots>::list(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.ShardSnapshots/Delete" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSvc<T: ShardSnapshots>(pub Arc<T>);
                    impl<
                        T: ShardSnapshots,
                    > tonic::server::UnaryService<super::DeleteShardSnapshotRequest>
                    for DeleteSvc<T> {
                        type Response = super::DeleteSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteShardSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ShardSnapshots>::delete(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/qdrant.ShardSnapshots/Recover" => {
                    #[allow(non_camel_case_types)]
                    struct RecoverSvc<T: ShardSnapshots>(pub Arc<T>);
                    impl<
                        T: ShardSnapshots,
                    > tonic::server::UnaryService<super::RecoverShardSnapshotRequest>
                    for RecoverSvc<T> {
                        type Response = super::RecoverSnapshotResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecoverShardSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ShardSnapshots>::recover(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecoverSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
    impl<T: ShardSnapshots> Clone for ShardSnapshotsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ShardSnapshots> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ShardSnapshots> tonic::server::NamedService for ShardSnapshotsServer<T> {
        const NAME: &'static str = "qdrant.ShardSnapshots";
    }
}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckRequest {}
#[derive(serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HealthCheckReply {
    #[prost(string, tag = "1")]
    pub title: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub version: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "3")]
    pub commit: ::core::option::Option<::prost::alloc::string::String>,
}
/// Generated client implementations.
pub mod qdrant_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct QdrantClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl QdrantClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
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
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> QdrantClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
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
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn health_check(
            &mut self,
            request: impl tonic::IntoRequest<super::HealthCheckRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HealthCheckReply>,
            tonic::Status,
        > {
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
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("qdrant.Qdrant", "HealthCheck"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod qdrant_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with QdrantServer.
    #[async_trait]
    pub trait Qdrant: Send + Sync + 'static {
        async fn health_check(
            &self,
            request: tonic::Request<super::HealthCheckRequest>,
        ) -> std::result::Result<
            tonic::Response<super::HealthCheckReply>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct QdrantServer<T: Qdrant> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
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
                max_decoding_message_size: None,
                max_encoding_message_size: None,
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
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
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
        ) -> Poll<std::result::Result<(), Self::Error>> {
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
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Qdrant>::health_check(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HealthCheckSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
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
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Qdrant> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Qdrant> tonic::server::NamedService for QdrantServer<T> {
        const NAME: &'static str = "qdrant.Qdrant";
    }
}
use super::validate::ValidateExt;
