use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use api::conversions::json::{json_path_from_proto, payload_to_proto};
use api::grpc::conversions::{
    convert_shard_key_from_grpc, convert_shard_key_from_grpc_opt, convert_shard_key_to_grpc,
    from_grpc_dist,
};
use api::grpc::qdrant::quantization_config_diff::Quantization;
use api::grpc::qdrant::update_collection_cluster_setup_request::{
    Operation as ClusterOperationsPb, Operation,
};
use api::grpc::qdrant::{CreateShardKey, Vectors};
use api::rest::schema::ShardKeySelector;
use api::rest::{BaseGroupRequest, MaxOptimizationThreads};
use common::types::ScoreType;
use itertools::Itertools;
use segment::common::operation_error::OperationError;
use segment::data_types::vectors::{
    BatchVectorStructInternal, NamedQuery, VectorInternal, VectorStructInternal,
};
use segment::types::{
    Distance, HnswConfig, MultiVectorConfig, QuantizationConfig, StrictModeConfig,
};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use sparse::common::sparse_vector::{validate_sparse_vector_impl, SparseVector};
use tonic::Status;

use super::consistency_params::ReadConsistency;
use super::types::{
    CollectionConfig, ContextExamplePair, CoreSearchRequest, Datatype, DiscoverRequestInternal,
    GroupsResult, Modifier, PointGroup, RecommendExample, RecommendGroupsRequestInternal,
    ReshardingInfo, SparseIndexParams, SparseVectorParams, SparseVectorsConfig, VectorParamsDiff,
    VectorsConfigDiff,
};
use crate::config::{
    default_replication_factor, default_write_consistency_factor, CollectionParams, ShardingMethod,
    WalConfig,
};
use crate::lookup::types::WithLookupInterface;
use crate::lookup::WithLookup;
use crate::operations::cluster_ops::{
    AbortShardTransfer, AbortTransferOperation, ClusterOperations, CreateShardingKey,
    CreateShardingKeyOperation, DropReplicaOperation, DropShardingKey, DropShardingKeyOperation,
    MoveShard, MoveShardOperation, Replica, ReplicateShard, ReplicateShardOperation,
    RestartTransfer, RestartTransferOperation,
};
use crate::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
    WalConfigDiff,
};
use crate::operations::point_ops::PointsSelector::PointIdsSelector;
use crate::operations::point_ops::{
    BatchPersisted, FilterSelector, PointIdsList, PointStructPersisted, PointsSelector,
    WriteOrdering,
};
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    AliasDescription, CollectionClusterInfo, CollectionInfo, CollectionStatus, CountResult,
    LocalShardInfo, OptimizersStatus, RecommendRequestInternal, RecordInternal, RemoteShardInfo,
    ShardTransferInfo, UpdateResult, UpdateStatus, VectorParams, VectorsConfig,
};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::remote_shard::CollectionCoreSearchRequest;
use crate::shards::replica_set::ReplicaState;
use crate::shards::transfer::ShardTransferMethod;

pub fn sharding_method_to_proto(sharding_method: ShardingMethod) -> i32 {
    match sharding_method {
        ShardingMethod::Auto => api::grpc::qdrant::ShardingMethod::Auto as i32,
        ShardingMethod::Custom => api::grpc::qdrant::ShardingMethod::Custom as i32,
    }
}

pub fn sharding_method_from_proto(sharding_method: i32) -> Result<ShardingMethod, Status> {
    let sharding_method_grpc = api::grpc::qdrant::ShardingMethod::try_from(sharding_method);

    match sharding_method_grpc {
        Ok(api::grpc::qdrant::ShardingMethod::Auto) => Ok(ShardingMethod::Auto),
        Ok(api::grpc::qdrant::ShardingMethod::Custom) => Ok(ShardingMethod::Custom),
        Err(err) => Err(Status::invalid_argument(format!(
            "Cannot convert ShardingMethod: {sharding_method}, error: {err}"
        ))),
    }
}

pub fn write_ordering_to_proto(ordering: WriteOrdering) -> api::grpc::qdrant::WriteOrdering {
    api::grpc::qdrant::WriteOrdering {
        r#type: match ordering {
            WriteOrdering::Weak => api::grpc::qdrant::WriteOrderingType::Weak as i32,
            WriteOrdering::Medium => api::grpc::qdrant::WriteOrderingType::Medium as i32,
            WriteOrdering::Strong => api::grpc::qdrant::WriteOrderingType::Strong as i32,
        },
    }
}

pub fn write_ordering_from_proto(
    ordering: Option<api::grpc::qdrant::WriteOrdering>,
) -> Result<WriteOrdering, Status> {
    let ordering_parsed = match ordering {
        None => api::grpc::qdrant::WriteOrderingType::Weak,
        Some(write_ordering) => {
            match api::grpc::qdrant::WriteOrderingType::try_from(write_ordering.r#type) {
                Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "cannot convert ordering: {}",
                        write_ordering.r#type
                    )))
                }
                Ok(res) => res,
            }
        }
    };

    Ok(match ordering_parsed {
        api::grpc::qdrant::WriteOrderingType::Weak => WriteOrdering::Weak,
        api::grpc::qdrant::WriteOrderingType::Medium => WriteOrdering::Medium,
        api::grpc::qdrant::WriteOrderingType::Strong => WriteOrdering::Strong,
    })
}

pub fn try_record_from_grpc(
    point: api::grpc::qdrant::RetrievedPoint,
    with_payload: bool,
) -> Result<RecordInternal, tonic::Status> {
    let id = point
        .id
        .ok_or_else(|| tonic::Status::invalid_argument("retrieved point does not have an ID"))?
        .try_into()?;

    let payload = if with_payload {
        Some(api::conversions::json::proto_to_payloads(point.payload)?)
    } else {
        debug_assert!(point.payload.is_empty());
        None
    };

    let vector: Option<_> = point
        .vectors
        .map(VectorStructInternal::try_from)
        .transpose()
        .map_err(|e| tonic::Status::invalid_argument(format!("Cannot convert vectors: {e}")))?;

    let order_value = point.order_value.map(TryFrom::try_from).transpose()?;

    Ok(RecordInternal {
        id,
        payload,
        vector,
        shard_key: convert_shard_key_from_grpc_opt(point.shard_key),
        order_value,
    })
}

#[allow(clippy::type_complexity)]
pub fn try_discover_request_from_grpc(
    value: api::grpc::qdrant::DiscoverPoints,
) -> Result<
    (
        DiscoverRequestInternal,
        String,
        Option<ReadConsistency>,
        Option<Duration>,
        Option<api::grpc::qdrant::ShardKeySelector>,
    ),
    Status,
> {
    let api::grpc::qdrant::DiscoverPoints {
        collection_name,
        target,
        context,
        filter,
        limit,
        offset,
        with_payload,
        params,
        using,
        with_vectors,
        lookup_from,
        read_consistency,
        timeout,
        shard_key_selector,
    } = value;

    let target = target.map(TryInto::try_into).transpose()?;

    let context = context
        .into_iter()
        .map(|pair| {
            match (
                pair.positive.map(|p| p.try_into()),
                pair.negative.map(|n| n.try_into()),
            ) {
                (Some(Ok(positive)), Some(Ok(negative))) => {
                    Ok(ContextExamplePair { positive, negative })
                }
                (Some(Err(e)), _) | (_, Some(Err(e))) => Err(e),
                (None, _) | (_, None) => Err(Status::invalid_argument(
                    "Both positive and negative are required in a context pair",
                )),
            }
        })
        .try_collect()?;

    let request = DiscoverRequestInternal {
        target,
        context: Some(context),
        filter: filter.map(|f| f.try_into()).transpose()?,
        params: params.map(|p| p.into()),
        limit: limit as usize,
        offset: offset.map(|x| x as usize),
        with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
        with_vector: Some(
            with_vectors
                .map(|selector| selector.into())
                .unwrap_or_default(),
        ),
        using: using.map(|u| u.into()),
        lookup_from: lookup_from.map(|l| l.into()),
    };

    let read_consistency = ReadConsistency::try_from_optional(read_consistency)?;

    let timeout = timeout.map(Duration::from_secs);

    Ok((
        request,
        collection_name,
        read_consistency,
        timeout,
        shard_key_selector,
    ))
}

impl From<api::grpc::qdrant::HnswConfigDiff> for HnswConfigDiff {
    fn from(value: api::grpc::qdrant::HnswConfigDiff) -> Self {
        Self {
            m: value.m.map(|v| v as usize),
            ef_construct: value.ef_construct.map(|v| v as usize),
            full_scan_threshold: value.full_scan_threshold.map(|v| v as usize),
            max_indexing_threads: value.max_indexing_threads.map(|v| v as usize),
            on_disk: value.on_disk,
            payload_m: value.payload_m.map(|v| v as usize),
        }
    }
}

impl From<HnswConfigDiff> for api::grpc::qdrant::HnswConfigDiff {
    fn from(value: HnswConfigDiff) -> Self {
        Self {
            m: value.m.map(|v| v as u64),
            ef_construct: value.ef_construct.map(|v| v as u64),
            full_scan_threshold: value.full_scan_threshold.map(|v| v as u64),
            max_indexing_threads: value.max_indexing_threads.map(|v| v as u64),
            on_disk: value.on_disk,
            payload_m: value.payload_m.map(|v| v as u64),
        }
    }
}

impl From<api::grpc::qdrant::WalConfigDiff> for WalConfigDiff {
    fn from(value: api::grpc::qdrant::WalConfigDiff) -> Self {
        Self {
            wal_capacity_mb: value.wal_capacity_mb.map(|v| v as usize),
            wal_segments_ahead: value.wal_segments_ahead.map(|v| v as usize),
        }
    }
}

impl TryFrom<api::grpc::qdrant::CollectionParamsDiff> for CollectionParamsDiff {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CollectionParamsDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            replication_factor: value
                .replication_factor
                .map(|factor| {
                    NonZeroU32::new(factor)
                        .ok_or_else(|| Status::invalid_argument("`replication_factor` cannot be 0"))
                })
                .transpose()?,
            write_consistency_factor: value
                .write_consistency_factor
                .map(|factor| {
                    NonZeroU32::new(factor).ok_or_else(|| {
                        Status::invalid_argument("`write_consistency_factor` cannot be 0")
                    })
                })
                .transpose()?,
            read_fan_out_factor: value.read_fan_out_factor,
            on_disk_payload: value.on_disk_payload,
        })
    }
}

impl TryFrom<api::grpc::qdrant::OptimizersConfigDiff> for OptimizersConfigDiff {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::OptimizersConfigDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            deleted_threshold: value.deleted_threshold,
            vacuum_min_vector_number: value.vacuum_min_vector_number.map(|v| v as usize),
            default_segment_number: value.default_segment_number.map(|v| v as usize),
            max_segment_size: value.max_segment_size.map(|v| v as usize),
            memmap_threshold: value.memmap_threshold.map(|v| v as usize),
            indexing_threshold: value.indexing_threshold.map(|v| v as usize),
            flush_interval_sec: value.flush_interval_sec,
            // TODO: remove deprecated field in a later version
            max_optimization_threads: value
                .deprecated_max_optimization_threads
                .map(|v| MaxOptimizationThreads::Threads(v as usize))
                .or(value
                    .max_optimization_threads
                    .map(TryFrom::try_from)
                    .transpose()?),
        })
    }
}

impl TryFrom<api::grpc::qdrant::QuantizationConfigDiff> for QuantizationConfigDiff {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::QuantizationConfigDiff) -> Result<Self, Self::Error> {
        match value.quantization {
            None => Err(Status::invalid_argument(
                "Quantization type is not specified",
            )),
            Some(quantization) => match quantization {
                Quantization::Scalar(scalar) => Ok(Self::Scalar(scalar.try_into()?)),
                Quantization::Product(product) => Ok(Self::Product(product.try_into()?)),
                Quantization::Binary(binary) => Ok(Self::Binary(binary.try_into()?)),
                Quantization::Disabled(_) => Ok(Self::new_disabled()),
            },
        }
    }
}

impl From<CollectionInfo> for api::grpc::qdrant::CollectionInfo {
    fn from(value: CollectionInfo) -> Self {
        let CollectionInfo {
            status,
            optimizer_status,
            vectors_count,
            indexed_vectors_count,
            points_count,
            segments_count,
            config,
            payload_schema,
        } = value;

        api::grpc::qdrant::CollectionInfo {
            status: match status {
                CollectionStatus::Green => api::grpc::qdrant::CollectionStatus::Green,
                CollectionStatus::Yellow => api::grpc::qdrant::CollectionStatus::Yellow,
                CollectionStatus::Red => api::grpc::qdrant::CollectionStatus::Red,
                CollectionStatus::Grey => api::grpc::qdrant::CollectionStatus::Grey,
            }
            .into(),
            optimizer_status: Some(match optimizer_status {
                OptimizersStatus::Ok => api::grpc::qdrant::OptimizerStatus {
                    ok: true,
                    error: "".to_string(),
                },
                OptimizersStatus::Error(error) => {
                    api::grpc::qdrant::OptimizerStatus { ok: false, error }
                }
            }),
            vectors_count: vectors_count.map(|count| count as u64),
            indexed_vectors_count: indexed_vectors_count.map(|count| count as u64),
            points_count: points_count.map(|count| count as u64),
            segments_count: segments_count as u64,
            config: Some(api::grpc::qdrant::CollectionConfig {
                params: Some(api::grpc::qdrant::CollectionParams {
                    vectors_config: {
                        let config = match config.params.vectors {
                            VectorsConfig::Single(vector_params) => {
                                Some(api::grpc::qdrant::vectors_config::Config::Params(
                                    vector_params.into(),
                                ))
                            }
                            VectorsConfig::Multi(vectors_params) => {
                                Some(api::grpc::qdrant::vectors_config::Config::ParamsMap(
                                    api::grpc::qdrant::VectorParamsMap {
                                        map: vectors_params
                                            .iter()
                                            .map(|(vector_name, vector_param)| {
                                                (vector_name.clone(), vector_param.clone().into())
                                            })
                                            .collect(),
                                    },
                                ))
                            }
                        };
                        Some(api::grpc::qdrant::VectorsConfig { config })
                    },
                    shard_number: config.params.shard_number.get(),
                    replication_factor: Some(config.params.replication_factor.get()),
                    on_disk_payload: config.params.on_disk_payload,
                    write_consistency_factor: Some(config.params.write_consistency_factor.get()),
                    read_fan_out_factor: config.params.read_fan_out_factor,
                    sharding_method: config.params.sharding_method.map(sharding_method_to_proto),
                    sparse_vectors_config: config.params.sparse_vectors.map(|sparse_vectors| {
                        api::grpc::qdrant::SparseVectorConfig {
                            map: sparse_vectors
                                .into_iter()
                                .map(|(name, sparse_vector_params)| {
                                    (name, sparse_vector_params.into())
                                })
                                .collect(),
                        }
                    }),
                }),
                hnsw_config: Some(api::grpc::qdrant::HnswConfigDiff {
                    m: Some(config.hnsw_config.m as u64),
                    ef_construct: Some(config.hnsw_config.ef_construct as u64),
                    full_scan_threshold: Some(config.hnsw_config.full_scan_threshold as u64),
                    max_indexing_threads: Some(config.hnsw_config.max_indexing_threads as u64),
                    on_disk: config.hnsw_config.on_disk,
                    payload_m: config.hnsw_config.payload_m.map(|v| v as u64),
                }),
                optimizer_config: Some(api::grpc::qdrant::OptimizersConfigDiff {
                    deleted_threshold: Some(config.optimizer_config.deleted_threshold),
                    vacuum_min_vector_number: Some(
                        config.optimizer_config.vacuum_min_vector_number as u64,
                    ),
                    default_segment_number: Some(
                        config.optimizer_config.default_segment_number as u64,
                    ),
                    max_segment_size: config.optimizer_config.max_segment_size.map(|x| x as u64),
                    memmap_threshold: config.optimizer_config.memmap_threshold.map(|x| x as u64),
                    indexing_threshold: config
                        .optimizer_config
                        .indexing_threshold
                        .map(|x| x as u64),
                    flush_interval_sec: Some(config.optimizer_config.flush_interval_sec),
                    deprecated_max_optimization_threads: config
                        .optimizer_config
                        .max_optimization_threads
                        .map(|x| x as u64),
                    max_optimization_threads: Some(From::from(
                        config.optimizer_config.max_optimization_threads,
                    )),
                }),
                wal_config: config
                    .wal_config
                    .map(|wal_config| api::grpc::qdrant::WalConfigDiff {
                        wal_capacity_mb: Some(wal_config.wal_capacity_mb as u64),
                        wal_segments_ahead: Some(wal_config.wal_segments_ahead as u64),
                    }),
                quantization_config: config.quantization_config.map(|x| x.into()),
                strict_mode_config: config
                    .strict_mode_config
                    .map(api::grpc::qdrant::StrictModeConfig::from),
            }),
            payload_schema: payload_schema
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.into()))
                .collect(),
        }
    }
}

impl From<RecordInternal> for api::grpc::qdrant::RetrievedPoint {
    fn from(record: RecordInternal) -> Self {
        let vectors = record.vector.map(VectorStructInternal::from);

        Self {
            id: Some(record.id.into()),
            payload: record.payload.map(payload_to_proto).unwrap_or_default(),
            vectors: vectors.map(api::grpc::qdrant::VectorsOutput::from),
            shard_key: record.shard_key.map(convert_shard_key_to_grpc),
            order_value: record.order_value.map(From::from),
        }
    }
}

impl TryFrom<i32> for CollectionStatus {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let status_grpc = api::grpc::qdrant::CollectionStatus::try_from(value);
        match status_grpc {
            Ok(api::grpc::qdrant::CollectionStatus::Green) => Ok(CollectionStatus::Green),
            Ok(api::grpc::qdrant::CollectionStatus::Yellow) => Ok(CollectionStatus::Yellow),
            Ok(api::grpc::qdrant::CollectionStatus::Red) => Ok(CollectionStatus::Red),
            Ok(api::grpc::qdrant::CollectionStatus::Grey) => Ok(CollectionStatus::Grey),
            Ok(api::grpc::qdrant::CollectionStatus::UnknownCollectionStatus) => Err(
                Status::invalid_argument(format!("Unknown CollectionStatus: {value}")),
            ),
            Err(err) => Err(Status::invalid_argument(format!(
                "Cannot convert CollectionStatus: {value}, error: {err}"
            ))),
        }
    }
}

impl TryFrom<api::grpc::qdrant::OptimizersConfigDiff> for OptimizersConfig {
    type Error = Status;

    fn try_from(
        optimizer_config: api::grpc::qdrant::OptimizersConfigDiff,
    ) -> Result<Self, Self::Error> {
        debug_assert!(
            optimizer_config.max_optimization_threads.is_some(),
            "This conversion is for CollectionInfo, max_optimization_threads should always have a value"
        );

        Ok(Self {
            deleted_threshold: optimizer_config.deleted_threshold.unwrap_or_default(),
            vacuum_min_vector_number: optimizer_config
                .vacuum_min_vector_number
                .unwrap_or_default() as usize,
            default_segment_number: optimizer_config.default_segment_number.unwrap_or_default()
                as usize,
            max_segment_size: optimizer_config.max_segment_size.map(|x| x as usize),
            memmap_threshold: optimizer_config.memmap_threshold.map(|x| x as usize),
            indexing_threshold: optimizer_config.indexing_threshold.map(|x| x as usize),
            flush_interval_sec: optimizer_config.flush_interval_sec.unwrap_or_default(),
            max_optimization_threads: match optimizer_config.max_optimization_threads {
                None => return Err(Status::invalid_argument("Malformed OptimizersConfig")),
                Some(max_optimization_threads) => TryFrom::try_from(max_optimization_threads)?,
            },
        })
    }
}

impl From<api::grpc::qdrant::WalConfigDiff> for WalConfig {
    fn from(wal_config: api::grpc::qdrant::WalConfigDiff) -> Self {
        Self {
            wal_capacity_mb: wal_config.wal_capacity_mb.unwrap_or_default() as usize,
            wal_segments_ahead: wal_config.wal_segments_ahead.unwrap_or_default() as usize,
        }
    }
}

impl TryFrom<api::grpc::qdrant::vectors_config::Config> for VectorsConfig {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::vectors_config::Config) -> Result<Self, Self::Error> {
        Ok(match value {
            api::grpc::qdrant::vectors_config::Config::Params(vector_params) => {
                VectorsConfig::Single(vector_params.try_into()?)
            }
            api::grpc::qdrant::vectors_config::Config::ParamsMap(vectors_params) => {
                let mut params_map = BTreeMap::new();
                for (name, params) in vectors_params.map {
                    params_map.insert(name, params.try_into()?);
                }
                VectorsConfig::Multi(params_map)
            }
        })
    }
}

impl TryFrom<api::grpc::qdrant::vectors_config_diff::Config> for VectorsConfigDiff {
    type Error = Status;

    fn try_from(
        value: api::grpc::qdrant::vectors_config_diff::Config,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            api::grpc::qdrant::vectors_config_diff::Config::Params(vector_params) => {
                let diff: VectorParamsDiff = vector_params.try_into()?;
                VectorsConfigDiff::from(diff)
            }
            api::grpc::qdrant::vectors_config_diff::Config::ParamsMap(vectors_params) => {
                let mut params_map = BTreeMap::new();
                for (name, params) in vectors_params.map {
                    params_map.insert(name, params.try_into()?);
                }
                VectorsConfigDiff(params_map)
            }
        })
    }
}

impl TryFrom<api::grpc::qdrant::VectorParams> for VectorParams {
    type Error = Status;

    fn try_from(vector_params: api::grpc::qdrant::VectorParams) -> Result<Self, Self::Error> {
        Ok(Self {
            size: NonZeroU64::new(vector_params.size).ok_or_else(|| {
                Status::invalid_argument("VectorParams size must be greater than zero")
            })?,
            distance: from_grpc_dist(vector_params.distance)?,
            hnsw_config: vector_params.hnsw_config.map(Into::into),
            quantization_config: vector_params
                .quantization_config
                .map(grpc_to_segment_quantization_config)
                .transpose()?,
            on_disk: vector_params.on_disk,
            datatype: convert_datatype_from_proto(vector_params.datatype)?,
            multivector_config: vector_params
                .multivector_config
                .map(MultiVectorConfig::try_from)
                .transpose()?,
        })
    }
}

fn convert_datatype_from_proto(datatype: Option<i32>) -> Result<Option<Datatype>, Status> {
    if let Some(datatype_int) = datatype {
        let grpc_datatype = api::grpc::qdrant::Datatype::try_from(datatype_int);
        if let Ok(grpc_datatype) = grpc_datatype {
            match grpc_datatype {
                api::grpc::qdrant::Datatype::Uint8 => Ok(Some(Datatype::Uint8)),
                api::grpc::qdrant::Datatype::Float32 => Ok(Some(Datatype::Float32)),
                api::grpc::qdrant::Datatype::Float16 => Ok(Some(Datatype::Float16)),
                api::grpc::qdrant::Datatype::Default => Ok(None),
            }
        } else {
            Err(Status::invalid_argument(format!(
                "Cannot convert datatype: {datatype_int}"
            )))
        }
    } else {
        Ok(None)
    }
}

impl TryFrom<api::grpc::qdrant::VectorParamsDiff> for VectorParamsDiff {
    type Error = Status;

    fn try_from(vector_params: api::grpc::qdrant::VectorParamsDiff) -> Result<Self, Self::Error> {
        Ok(Self {
            hnsw_config: vector_params.hnsw_config.map(Into::into),
            quantization_config: vector_params
                .quantization_config
                .map(TryInto::try_into)
                .transpose()?,
            on_disk: vector_params.on_disk,
        })
    }
}

impl From<api::grpc::qdrant::Modifier> for Modifier {
    fn from(value: api::grpc::qdrant::Modifier) -> Self {
        match value {
            api::grpc::qdrant::Modifier::None => Modifier::None,
            api::grpc::qdrant::Modifier::Idf => Modifier::Idf,
        }
    }
}

impl TryFrom<api::grpc::qdrant::SparseVectorParams> for SparseVectorParams {
    type Error = Status;

    fn try_from(
        sparse_vector_params: api::grpc::qdrant::SparseVectorParams,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            index: sparse_vector_params
                .index
                .map(|index_config| -> Result<_, Status> {
                    Ok(SparseIndexParams {
                        full_scan_threshold: index_config.full_scan_threshold.map(|v| v as usize),
                        on_disk: index_config.on_disk,
                        datatype: convert_datatype_from_proto(index_config.datatype)?,
                    })
                })
                .transpose()?,
            modifier: sparse_vector_params
                .modifier
                .and_then(|x|
                    // XXX: Invalid values silently converted to None
                    api::grpc::qdrant::Modifier::try_from(x).ok())
                .map(Modifier::from),
        })
    }
}

impl From<Modifier> for api::grpc::qdrant::Modifier {
    fn from(value: Modifier) -> Self {
        match value {
            Modifier::None => api::grpc::qdrant::Modifier::None,
            Modifier::Idf => api::grpc::qdrant::Modifier::Idf,
        }
    }
}

impl From<SparseVectorParams> for api::grpc::qdrant::SparseVectorParams {
    fn from(sparse_vector_params: SparseVectorParams) -> Self {
        Self {
            index: sparse_vector_params.index.map(|index_config| {
                api::grpc::qdrant::SparseIndexConfig {
                    full_scan_threshold: index_config.full_scan_threshold.map(|v| v as u64),
                    on_disk: index_config.on_disk,
                    datatype: index_config
                        .datatype
                        .map(|dt| api::grpc::qdrant::Datatype::from(dt).into()),
                }
            }),
            modifier: sparse_vector_params
                .modifier
                .map(|modifier| api::grpc::qdrant::Modifier::from(modifier) as i32),
        }
    }
}

fn grpc_to_segment_quantization_config(
    value: api::grpc::qdrant::QuantizationConfig,
) -> Result<QuantizationConfig, Status> {
    let quantization = value
        .quantization
        .ok_or_else(|| Status::invalid_argument("QuantizationConfig must contain quantization"))?;
    match quantization {
        api::grpc::qdrant::quantization_config::Quantization::Scalar(config) => {
            Ok(QuantizationConfig::Scalar(config.try_into()?))
        }
        api::grpc::qdrant::quantization_config::Quantization::Product(config) => {
            Ok(QuantizationConfig::Product(config.try_into()?))
        }
        api::grpc::qdrant::quantization_config::Quantization::Binary(config) => {
            Ok(QuantizationConfig::Binary(config.try_into()?))
        }
    }
}

impl TryFrom<api::grpc::qdrant::GetCollectionInfoResponse> for CollectionInfo {
    type Error = Status;

    fn try_from(
        collection_info_response: api::grpc::qdrant::GetCollectionInfoResponse,
    ) -> Result<Self, Self::Error> {
        match collection_info_response.result {
            None => Err(Status::invalid_argument("Malformed CollectionInfo type")),
            Some(collection_info_response) => Ok(Self {
                status: CollectionStatus::try_from(collection_info_response.status)?,
                optimizer_status: match collection_info_response.optimizer_status {
                    None => return Err(Status::invalid_argument("Malformed OptimizerStatus type")),
                    Some(api::grpc::qdrant::OptimizerStatus { ok, error }) => {
                        if ok {
                            OptimizersStatus::Ok
                        } else {
                            OptimizersStatus::Error(error)
                        }
                    }
                },
                vectors_count: collection_info_response
                    .vectors_count
                    .map(|count| count as usize),
                indexed_vectors_count: collection_info_response
                    .indexed_vectors_count
                    .map(|count| count as usize),
                points_count: collection_info_response
                    .points_count
                    .map(|count| count as usize),
                segments_count: collection_info_response.segments_count as usize,
                config: match collection_info_response.config {
                    None => {
                        return Err(Status::invalid_argument("Malformed CollectionConfig type"))
                    }
                    Some(config) => CollectionConfig::try_from(config)?,
                },
                payload_schema: collection_info_response
                    .payload_schema
                    .into_iter()
                    .map(|(k, v)| Ok::<_, Status>((json_path_from_proto(&k)?, v.try_into()?)))
                    .try_collect()?,
            }),
        }
    }
}

impl TryFrom<PointStructPersisted> for api::grpc::qdrant::PointStruct {
    type Error = Status;

    fn try_from(value: PointStructPersisted) -> Result<Self, Self::Error> {
        let PointStructPersisted {
            id,
            vector,
            payload,
        } = value;

        let vectors_internal = VectorStructInternal::try_from(vector)
            .map_err(|e| Status::invalid_argument(format!("Failed to convert vectors: {e}")))?;

        let vectors = api::grpc::qdrant::Vectors::from(vectors_internal);
        let converted_payload = match payload {
            None => HashMap::new(),
            Some(payload) => payload_to_proto(payload),
        };

        Ok(Self {
            id: Some(id.into()),
            vectors: Some(vectors),
            payload: converted_payload,
        })
    }
}

impl TryFrom<BatchPersisted> for Vec<api::grpc::qdrant::PointStruct> {
    type Error = Status;

    fn try_from(batch: BatchPersisted) -> Result<Self, Self::Error> {
        let mut points = Vec::new();
        let batch_vectors = BatchVectorStructInternal::from(batch.vectors);
        let all_vectors = batch_vectors.into_all_vectors(batch.ids.len());
        for (i, p_id) in batch.ids.into_iter().enumerate() {
            let id = Some(p_id.into());
            let vector = all_vectors.get(i).cloned();
            let payload = batch.payloads.as_ref().and_then(|payloads| {
                payloads.get(i).map(|payload| match payload {
                    None => HashMap::new(),
                    Some(payload) => payload_to_proto(payload.clone()),
                })
            });
            let vectors: Option<VectorStructInternal> = vector.map(|v| v.into());

            let point = api::grpc::qdrant::PointStruct {
                id,
                vectors: vectors.map(Vectors::from),
                payload: payload.unwrap_or_default(),
            };
            points.push(point);
        }

        Ok(points)
    }
}

pub fn try_points_selector_from_grpc(
    value: api::grpc::qdrant::PointsSelector,
    shard_key_selector: Option<api::grpc::qdrant::ShardKeySelector>,
) -> Result<PointsSelector, Status> {
    match value.points_selector_one_of {
        Some(api::grpc::qdrant::points_selector::PointsSelectorOneOf::Points(points)) => {
            Ok(PointIdsSelector(PointIdsList {
                points: points
                    .ids
                    .into_iter()
                    .map(|p| p.try_into())
                    .collect::<Result<_, _>>()?,
                shard_key: shard_key_selector.map(ShardKeySelector::from),
            }))
        }
        Some(api::grpc::qdrant::points_selector::PointsSelectorOneOf::Filter(f)) => {
            Ok(PointsSelector::FilterSelector(FilterSelector {
                filter: f.try_into()?,
                shard_key: shard_key_selector.map(ShardKeySelector::from),
            }))
        }
        _ => Err(Status::invalid_argument("Malformed PointsSelector type")),
    }
}

impl From<UpdateResult> for api::grpc::qdrant::UpdateResultInternal {
    fn from(res: UpdateResult) -> Self {
        Self {
            operation_id: res.operation_id,
            status: res.status.into(),
            clock_tag: res.clock_tag.map(Into::into),
        }
    }
}

impl From<UpdateResult> for api::grpc::qdrant::UpdateResult {
    fn from(res: UpdateResult) -> Self {
        api::grpc::qdrant::UpdateResultInternal::from(res).into()
    }
}

impl TryFrom<api::grpc::qdrant::UpdateResultInternal> for UpdateResult {
    type Error = Status;

    fn try_from(res: api::grpc::qdrant::UpdateResultInternal) -> Result<Self, Self::Error> {
        let res = Self {
            operation_id: res.operation_id,
            status: res.status.try_into()?,
            clock_tag: res.clock_tag.map(Into::into),
        };

        Ok(res)
    }
}

impl TryFrom<api::grpc::qdrant::UpdateResult> for UpdateResult {
    type Error = Status;

    fn try_from(res: api::grpc::qdrant::UpdateResult) -> Result<Self, Self::Error> {
        api::grpc::qdrant::UpdateResultInternal::from(res).try_into()
    }
}

impl From<UpdateStatus> for i32 {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::Acknowledged => api::grpc::qdrant::UpdateStatus::Acknowledged as i32,
            UpdateStatus::Completed => api::grpc::qdrant::UpdateStatus::Completed as i32,
            UpdateStatus::ClockRejected => api::grpc::qdrant::UpdateStatus::ClockRejected as i32,
        }
    }
}

impl TryFrom<i32> for UpdateStatus {
    type Error = Status;

    fn try_from(status: i32) -> Result<Self, Self::Error> {
        let status = api::grpc::qdrant::UpdateStatus::try_from(status)
            .map_err(|_| Status::invalid_argument("Malformed UpdateStatus type"))?;

        let status = match status {
            api::grpc::qdrant::UpdateStatus::Acknowledged => Self::Acknowledged,
            api::grpc::qdrant::UpdateStatus::Completed => Self::Completed,
            api::grpc::qdrant::UpdateStatus::ClockRejected => Self::ClockRejected,

            api::grpc::qdrant::UpdateStatus::UnknownUpdateStatus => {
                return Err(Status::invalid_argument(
                    "Malformed UpdateStatus type: update status is unknown",
                ));
            }
        };

        Ok(status)
    }
}

impl From<api::grpc::qdrant::CountResult> for CountResult {
    fn from(value: api::grpc::qdrant::CountResult) -> Self {
        Self {
            count: value.count as usize,
        }
    }
}

impl From<CountResult> for api::grpc::qdrant::CountResult {
    fn from(value: CountResult) -> Self {
        Self {
            count: value.count as u64,
        }
    }
}

impl TryFrom<api::grpc::qdrant::SearchPoints> for CoreSearchRequest {
    type Error = Status;
    fn try_from(value: api::grpc::qdrant::SearchPoints) -> Result<Self, Self::Error> {
        let api::grpc::qdrant::SearchPoints {
            collection_name: _,
            vector,
            filter,
            limit,
            with_payload,
            params,
            score_threshold,
            offset,
            vector_name,
            with_vectors,
            read_consistency: _,
            timeout: _,
            shard_key_selector: _,
            sparse_indices,
        } = value;

        if let Some(sparse_indices) = &sparse_indices {
            validate_sparse_vector_impl(&sparse_indices.data, &vector).map_err(|e| {
                Status::invalid_argument(format!(
                    "Sparse indices does not match sparse vector conditions: {e}"
                ))
            })?;
        }

        let vector_struct =
            api::grpc::conversions::into_named_vector_struct(vector_name, vector, sparse_indices)?;

        Ok(Self {
            query: QueryEnum::Nearest(vector_struct),
            filter: filter.map(TryInto::try_into).transpose()?,
            params: params.map(Into::into),
            limit: limit as usize,
            offset: offset.map(|v| v as usize).unwrap_or_default(),
            with_payload: with_payload.map(TryInto::try_into).transpose()?,
            with_vector: with_vectors.map(Into::into),
            score_threshold: score_threshold.map(|s| s as ScoreType),
        })
    }
}

impl From<QueryEnum> for api::grpc::qdrant::QueryEnum {
    fn from(value: QueryEnum) -> Self {
        match value {
            QueryEnum::Nearest(vector) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::NearestNeighbors(
                    vector.to_vector().into(),
                )),
            },
            QueryEnum::RecommendBestScore(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::RecommendBestScore(
                    api::grpc::qdrant::RecoQuery {
                        positives: named.query.positives.into_iter().map_into().collect(),
                        negatives: named.query.negatives.into_iter().map_into().collect(),
                    },
                )),
            },
            QueryEnum::Discover(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::Discover(
                    api::grpc::qdrant::DiscoveryQuery {
                        target: Some(named.query.target.into()),
                        context: named
                            .query
                            .pairs
                            .into_iter()
                            .map(|pair| api::grpc::qdrant::ContextPair {
                                positive: { Some(pair.positive.into()) },
                                negative: { Some(pair.negative.into()) },
                            })
                            .collect(),
                    },
                )),
            },
            QueryEnum::Context(named) => api::grpc::qdrant::QueryEnum {
                query: Some(api::grpc::qdrant::query_enum::Query::Context(
                    api::grpc::qdrant::ContextQuery {
                        context: named
                            .query
                            .pairs
                            .into_iter()
                            .map(|pair| api::grpc::qdrant::ContextPair {
                                positive: { Some(pair.positive.into()) },
                                negative: { Some(pair.negative.into()) },
                            })
                            .collect(),
                    },
                )),
            },
        }
    }
}

impl<'a> From<CollectionCoreSearchRequest<'a>> for api::grpc::qdrant::CoreSearchPoints {
    fn from(value: CollectionCoreSearchRequest<'a>) -> Self {
        let (collection_id, request) = value.0;

        Self {
            collection_name: collection_id,
            query: Some(request.query.clone().into()),
            filter: request.filter.clone().map(|f| f.into()),
            limit: request.limit as u64,
            with_vectors: request.with_vector.clone().map(|wv| wv.into()),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            params: request.params.map(|sp| sp.into()),
            score_threshold: request.score_threshold,
            offset: Some(request.offset as u64),
            vector_name: Some(request.query.get_vector_name().to_owned()),
            read_consistency: None,
        }
    }
}

impl TryFrom<api::grpc::qdrant::WithLookup> for WithLookup {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::WithLookup) -> Result<Self, Self::Error> {
        let with_default_payload = || Some(segment::types::WithPayloadInterface::Bool(true));

        Ok(Self {
            collection_name: value.collection,
            with_payload: value
                .with_payload
                .map(|wp| wp.try_into())
                .transpose()?
                .or_else(with_default_payload),
            with_vectors: value.with_vectors.map(|wv| wv.into()),
        })
    }
}

impl TryFrom<api::grpc::qdrant::WithLookup> for WithLookupInterface {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::WithLookup) -> Result<Self, Self::Error> {
        Ok(Self::WithLookup(value.try_into()?))
    }
}

impl TryFrom<api::grpc::qdrant::TargetVector> for RecommendExample {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::TargetVector) -> Result<Self, Self::Error> {
        value
            .target
            .ok_or_else(|| Status::invalid_argument("Target vector is malformed"))
            .and_then(|target| match target {
                api::grpc::qdrant::target_vector::Target::Single(vector_example) => {
                    Ok(vector_example.try_into()?)
                }
            })
    }
}

fn try_context_pair_from_grpc(
    pair: api::grpc::qdrant::ContextPair,
) -> Result<ContextPair<VectorInternal>, Status> {
    match (pair.positive, pair.negative) {
        (Some(positive), Some(negative)) => Ok(ContextPair {
            positive: positive.try_into()?,
            negative: negative.try_into()?,
        }),
        _ => Err(Status::invalid_argument(
            "All context pairs must have both positive and negative parts",
        )),
    }
}

impl TryFrom<api::grpc::qdrant::CoreSearchPoints> for CoreSearchRequest {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CoreSearchPoints) -> Result<Self, Self::Error> {
        let query = value
            .query
            .and_then(|query| query.query)
            .map(|query| {
                Ok(match query {
                    api::grpc::qdrant::query_enum::Query::NearestNeighbors(vector) => {
                        QueryEnum::Nearest(api::grpc::conversions::into_named_vector_struct(
                            value.vector_name,
                            vector.data,
                            vector.indices,
                        )?)
                    }
                    api::grpc::qdrant::query_enum::Query::RecommendBestScore(query) => {
                        QueryEnum::RecommendBestScore(NamedQuery {
                            query: RecoQuery::new(
                                query
                                    .positives
                                    .into_iter()
                                    .map(TryInto::try_into)
                                    .collect::<Result<_, _>>()?,
                                query
                                    .negatives
                                    .into_iter()
                                    .map(TryInto::try_into)
                                    .collect::<Result<_, _>>()?,
                            ),
                            using: value.vector_name,
                        })
                    }
                    api::grpc::qdrant::query_enum::Query::Discover(query) => {
                        let Some(target) = query.target else {
                            return Err(Status::invalid_argument("Target is not specified"));
                        };

                        let pairs = query
                            .context
                            .into_iter()
                            .map(try_context_pair_from_grpc)
                            .try_collect()?;

                        QueryEnum::Discover(NamedQuery {
                            query: DiscoveryQuery::new(target.try_into()?, pairs),
                            using: value.vector_name,
                        })
                    }
                    api::grpc::qdrant::query_enum::Query::Context(query) => {
                        let pairs = query
                            .context
                            .into_iter()
                            .map(try_context_pair_from_grpc)
                            .try_collect()?;

                        QueryEnum::Context(NamedQuery {
                            query: ContextQuery::new(pairs),
                            using: value.vector_name,
                        })
                    }
                })
            })
            .transpose()?
            .ok_or_else(|| Status::invalid_argument("Query is not specified"))?;

        Ok(Self {
            query,
            filter: value.filter.map(|f| f.try_into()).transpose()?,
            params: value.params.map(|p| p.into()),
            limit: value.limit as usize,
            offset: value.offset.unwrap_or_default() as usize,
            with_payload: value.with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(
                value
                    .with_vectors
                    .map(|with_vectors| with_vectors.into())
                    .unwrap_or_default(),
            ),
            score_threshold: value.score_threshold,
        })
    }
}

impl TryFrom<PointGroup> for api::grpc::qdrant::PointGroup {
    type Error = OperationError;
    fn try_from(group: PointGroup) -> Result<Self, Self::Error> {
        let hits: Result<_, _> = group
            .hits
            .into_iter()
            .map(api::grpc::qdrant::ScoredPoint::try_from)
            .collect();

        Ok(Self {
            hits: hits?,
            id: Some(group.id.into()),
            lookup: group
                .lookup
                .map(api::grpc::qdrant::RetrievedPoint::try_from)
                .transpose()?,
        })
    }
}

impl TryFrom<i32> for ReplicaState {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let replica_state = api::grpc::qdrant::ReplicaState::try_from(value)
            .map_err(|_| Status::invalid_argument(format!("Unknown replica state: {value}")))?;
        Ok(replica_state.into())
    }
}

impl From<api::grpc::qdrant::ReplicaState> for ReplicaState {
    fn from(value: api::grpc::qdrant::ReplicaState) -> Self {
        match value {
            api::grpc::qdrant::ReplicaState::Active => Self::Active,
            api::grpc::qdrant::ReplicaState::Dead => Self::Dead,
            api::grpc::qdrant::ReplicaState::Partial => Self::Partial,
            api::grpc::qdrant::ReplicaState::Initializing => Self::Initializing,
            api::grpc::qdrant::ReplicaState::Listener => Self::Listener,
            api::grpc::qdrant::ReplicaState::PartialSnapshot => Self::PartialSnapshot,
            api::grpc::qdrant::ReplicaState::Recovery => Self::Recovery,
            api::grpc::qdrant::ReplicaState::Resharding => Self::Resharding,
            api::grpc::qdrant::ReplicaState::ReshardingScaleDown => Self::ReshardingScaleDown,
        }
    }
}

impl From<ReplicaState> for api::grpc::qdrant::ReplicaState {
    fn from(value: ReplicaState) -> Self {
        match value {
            ReplicaState::Active => Self::Active,
            ReplicaState::Dead => Self::Dead,
            ReplicaState::Partial => Self::Partial,
            ReplicaState::Initializing => Self::Initializing,
            ReplicaState::Listener => Self::Listener,
            ReplicaState::PartialSnapshot => Self::PartialSnapshot,
            ReplicaState::Recovery => Self::Recovery,
            ReplicaState::Resharding => Self::Resharding,
            ReplicaState::ReshardingScaleDown => Self::ReshardingScaleDown,
        }
    }
}

impl TryFrom<api::grpc::qdrant::PointId> for RecommendExample {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::PointId) -> Result<Self, Self::Error> {
        Ok(Self::PointId(value.try_into()?))
    }
}

impl TryFrom<api::grpc::qdrant::Vector> for RecommendExample {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::Vector) -> Result<Self, Self::Error> {
        let vector: VectorInternal = value.try_into()?;
        match vector {
            VectorInternal::Dense(vector) => Ok(Self::Dense(vector)),
            VectorInternal::Sparse(vector) => Ok(Self::Sparse(vector)),
            VectorInternal::MultiDense(_vector) => Err(Status::invalid_argument(
                "MultiDense vector is not supported in search request",
            )),
        }
    }
}

impl TryFrom<api::grpc::qdrant::VectorExample> for RecommendExample {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::VectorExample) -> Result<Self, Self::Error> {
        value
            .example
            .ok_or_else(|| {
                Status::invalid_argument(
                    "Vector example, which can be id or bare vector, is malformed",
                )
            })
            .and_then(|example| match example {
                api::grpc::qdrant::vector_example::Example::Id(id) => {
                    Ok(Self::PointId(id.try_into()?))
                }
                api::grpc::qdrant::vector_example::Example::Vector(vector) => {
                    match vector.indices {
                        Some(indices) => {
                            validate_sparse_vector_impl(&indices.data, &vector.data).map_err(
                                |e| {
                                    Status::invalid_argument(
                                        format!("Sparse indices does not match sparse vector conditions: {e}"),
                                    )
                                },
                            )?;
                            Ok(Self::Sparse(SparseVector {
                                indices: indices.data,
                                values: vector.data,
                            }))
                        }
                        None => Ok(Self::Dense(vector.data)),
                    }
                }
            })
    }
}

impl TryFrom<api::grpc::qdrant::RecommendPoints> for RecommendRequestInternal {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::RecommendPoints) -> Result<Self, Self::Error> {
        let positive_ids = value
            .positive
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<RecommendExample>, Self::Error>>()?;

        let positive_vectors = value
            .positive_vectors
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        let positive = [positive_ids, positive_vectors].concat();

        let negative_ids = value
            .negative
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<RecommendExample>, Self::Error>>()?;

        let negative_vectors = value
            .negative_vectors
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        let negative = [negative_ids, negative_vectors].concat();

        Ok(RecommendRequestInternal {
            positive,
            negative,
            strategy: value.strategy.map(|s| s.try_into()).transpose()?,
            filter: value.filter.map(|f| f.try_into()).transpose()?,
            params: value.params.map(|p| p.into()),
            limit: value.limit as usize,
            offset: value.offset.map(|x| x as usize),
            with_payload: value.with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: Some(
                value
                    .with_vectors
                    .map(|with_vectors| with_vectors.into())
                    .unwrap_or_default(),
            ),
            score_threshold: value.score_threshold,
            using: value.using.map(|name| name.into()),
            lookup_from: value.lookup_from.map(|x| x.into()),
        })
    }
}

impl TryFrom<api::grpc::qdrant::RecommendPointGroups> for RecommendGroupsRequestInternal {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::RecommendPointGroups) -> Result<Self, Self::Error> {
        let recommend_points = api::grpc::qdrant::RecommendPoints {
            positive: value.positive,
            negative: value.negative,
            strategy: value.strategy,
            using: value.using,
            lookup_from: value.lookup_from,
            filter: value.filter,
            params: value.params,
            with_payload: value.with_payload,
            with_vectors: value.with_vectors,
            score_threshold: value.score_threshold,
            read_consistency: None,
            limit: 0,     // Will be calculated from group_size
            offset: None, // Not enabled for groups
            collection_name: String::new(),
            positive_vectors: value.positive_vectors,
            negative_vectors: value.negative_vectors,
            timeout: None, // Passed as query param
            shard_key_selector: None,
        };

        let RecommendRequestInternal {
            positive,
            negative,
            strategy,
            using,
            lookup_from,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            limit: _,
            offset: _,
        } = recommend_points.try_into()?;

        Ok(RecommendGroupsRequestInternal {
            positive,
            negative,
            strategy,
            using,
            lookup_from,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            group_request: BaseGroupRequest {
                group_by: json_path_from_proto(&value.group_by)?,
                limit: value.limit,
                group_size: value.group_size,
                with_lookup: value.with_lookup.map(|l| l.try_into()).transpose()?,
            },
        })
    }
}

impl TryFrom<GroupsResult> for api::grpc::qdrant::GroupsResult {
    type Error = OperationError;

    fn try_from(value: GroupsResult) -> Result<Self, Self::Error> {
        let groups: Result<_, _> = value
            .groups
            .into_iter()
            .map(api::grpc::qdrant::PointGroup::try_from)
            .collect();

        Ok(Self { groups: groups? })
    }
}

impl From<VectorParams> for api::grpc::qdrant::VectorParams {
    fn from(value: VectorParams) -> Self {
        api::grpc::qdrant::VectorParams {
            size: value.size.get(),
            distance: match value.distance {
                Distance::Cosine => api::grpc::qdrant::Distance::Cosine,
                Distance::Euclid => api::grpc::qdrant::Distance::Euclid,
                Distance::Dot => api::grpc::qdrant::Distance::Dot,
                Distance::Manhattan => api::grpc::qdrant::Distance::Manhattan,
            }
            .into(),
            hnsw_config: value.hnsw_config.map(Into::into),
            quantization_config: value.quantization_config.map(Into::into),
            on_disk: value.on_disk,
            datatype: value
                .datatype
                .map(|dt| api::grpc::qdrant::Datatype::from(dt).into()),
            multivector_config: value
                .multivector_config
                .map(api::grpc::qdrant::MultiVectorConfig::from),
        }
    }
}

impl From<Datatype> for api::grpc::qdrant::Datatype {
    fn from(value: Datatype) -> Self {
        match value {
            Datatype::Float32 => api::grpc::qdrant::Datatype::Float32,
            Datatype::Uint8 => api::grpc::qdrant::Datatype::Uint8,
            Datatype::Float16 => api::grpc::qdrant::Datatype::Float16,
        }
    }
}

impl From<AliasDescription> for api::grpc::qdrant::AliasDescription {
    fn from(value: AliasDescription) -> Self {
        api::grpc::qdrant::AliasDescription {
            alias_name: value.alias_name,
            collection_name: value.collection_name,
        }
    }
}

impl From<LocalShardInfo> for api::grpc::qdrant::LocalShardInfo {
    fn from(value: LocalShardInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            points_count: value.points_count as u64,
            state: value.state as i32,
            shard_key: value.shard_key.map(convert_shard_key_to_grpc),
        }
    }
}

impl From<RemoteShardInfo> for api::grpc::qdrant::RemoteShardInfo {
    fn from(value: RemoteShardInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            peer_id: value.peer_id,
            state: value.state as i32,
            shard_key: value.shard_key.map(convert_shard_key_to_grpc),
        }
    }
}

impl From<ReshardingInfo> for api::grpc::qdrant::ReshardingInfo {
    fn from(value: ReshardingInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            peer_id: value.peer_id,
            shard_key: value.shard_key.map(convert_shard_key_to_grpc),
        }
    }
}

impl From<ShardTransferInfo> for api::grpc::qdrant::ShardTransferInfo {
    fn from(value: ShardTransferInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            to_shard_id: value.to_shard_id,
            from: value.from,
            to: value.to,
            sync: value.sync,
        }
    }
}

impl From<CollectionClusterInfo> for api::grpc::qdrant::CollectionClusterInfoResponse {
    fn from(value: CollectionClusterInfo) -> Self {
        Self {
            peer_id: value.peer_id,
            shard_count: value.shard_count as u64,
            local_shards: value
                .local_shards
                .into_iter()
                .map(|shard| shard.into())
                .collect(),
            remote_shards: value
                .remote_shards
                .into_iter()
                .map(|shard| shard.into())
                .collect(),
            shard_transfers: value
                .shard_transfers
                .into_iter()
                .map(|shard| shard.into())
                .collect(),
            resharding_operations: value
                .resharding_operations
                .into_iter()
                .map(|info| info.into())
                .collect(),
        }
    }
}

impl TryFrom<api::grpc::qdrant::ReplicateShard> for ReplicateShard {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::ReplicateShard) -> Result<Self, Self::Error> {
        let method = value.method.map(TryInto::try_into).transpose()?;
        Ok(Self {
            shard_id: value.shard_id,
            to_shard_id: value.to_shard_id,
            from_peer_id: value.from_peer_id,
            to_peer_id: value.to_peer_id,
            method,
        })
    }
}

impl TryFrom<api::grpc::qdrant::MoveShard> for MoveShard {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::MoveShard) -> Result<Self, Self::Error> {
        let method = value.method.map(TryInto::try_into).transpose()?;
        Ok(Self {
            shard_id: value.shard_id,
            to_shard_id: value.to_shard_id,
            from_peer_id: value.from_peer_id,
            to_peer_id: value.to_peer_id,
            method,
        })
    }
}

impl TryFrom<api::grpc::qdrant::AbortShardTransfer> for AbortShardTransfer {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::AbortShardTransfer) -> Result<Self, Self::Error> {
        Ok(Self {
            shard_id: value.shard_id,
            to_shard_id: value.to_shard_id,
            from_peer_id: value.from_peer_id,
            to_peer_id: value.to_peer_id,
        })
    }
}

impl TryFrom<i32> for ShardTransferMethod {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        api::grpc::qdrant::ShardTransferMethod::try_from(value)
            .map(Into::into)
            .map_err(|_| {
                Status::invalid_argument(format!("Unknown shard transfer method: {value}"))
            })
    }
}

impl From<api::grpc::qdrant::ShardTransferMethod> for ShardTransferMethod {
    fn from(value: api::grpc::qdrant::ShardTransferMethod) -> Self {
        match value {
            api::grpc::qdrant::ShardTransferMethod::StreamRecords => {
                ShardTransferMethod::StreamRecords
            }
            api::grpc::qdrant::ShardTransferMethod::Snapshot => ShardTransferMethod::Snapshot,
            api::grpc::qdrant::ShardTransferMethod::WalDelta => ShardTransferMethod::WalDelta,
            api::grpc::qdrant::ShardTransferMethod::ReshardingStreamRecords => {
                ShardTransferMethod::ReshardingStreamRecords
            }
        }
    }
}

impl TryFrom<api::grpc::qdrant::CreateShardKey> for CreateShardingKey {
    type Error = Status;

    fn try_from(op: CreateShardKey) -> Result<Self, Self::Error> {
        let res = CreateShardingKey {
            shard_key: op
                .shard_key
                .and_then(convert_shard_key_from_grpc)
                .ok_or_else(|| Status::invalid_argument("Shard key is not specified"))?,
            shards_number: op
                .shards_number
                .map(NonZeroU32::try_from)
                .transpose()
                .map_err(|err| {
                    Status::invalid_argument(format!("Shard number cannot be zero: {err}"))
                })?,
            replication_factor: op
                .replication_factor
                .map(NonZeroU32::try_from)
                .transpose()
                .map_err(|err| {
                    Status::invalid_argument(format!("Replication factor cannot be zero: {err}"))
                })?,
            placement: (!op.placement.is_empty()).then_some(op.placement),
        };
        Ok(res)
    }
}

impl TryFrom<api::grpc::qdrant::DeleteShardKey> for DropShardingKey {
    type Error = Status;

    fn try_from(op: api::grpc::qdrant::DeleteShardKey) -> Result<Self, Self::Error> {
        Ok(DropShardingKey {
            shard_key: op
                .shard_key
                .and_then(convert_shard_key_from_grpc)
                .ok_or_else(|| Status::invalid_argument("Shard key is not specified"))?,
        })
    }
}

impl TryFrom<ClusterOperationsPb> for ClusterOperations {
    type Error = Status;

    fn try_from(value: ClusterOperationsPb) -> Result<Self, Self::Error> {
        Ok(match value {
            ClusterOperationsPb::MoveShard(op) => {
                ClusterOperations::MoveShard(MoveShardOperation {
                    move_shard: op.try_into()?,
                })
            }
            ClusterOperationsPb::ReplicateShard(op) => {
                ClusterOperations::ReplicateShard(ReplicateShardOperation {
                    replicate_shard: op.try_into()?,
                })
            }
            ClusterOperationsPb::AbortTransfer(op) => {
                ClusterOperations::AbortTransfer(AbortTransferOperation {
                    abort_transfer: op.try_into()?,
                })
            }
            ClusterOperationsPb::DropReplica(op) => {
                ClusterOperations::DropReplica(DropReplicaOperation {
                    drop_replica: Replica {
                        shard_id: op.shard_id,
                        peer_id: op.peer_id,
                    },
                })
            }
            Operation::CreateShardKey(op) => {
                ClusterOperations::CreateShardingKey(CreateShardingKeyOperation {
                    create_sharding_key: op.try_into()?,
                })
            }
            Operation::DeleteShardKey(op) => {
                ClusterOperations::DropShardingKey(DropShardingKeyOperation {
                    drop_sharding_key: op.try_into()?,
                })
            }
            Operation::RestartTransfer(op) => {
                ClusterOperations::RestartTransfer(RestartTransferOperation {
                    restart_transfer: RestartTransfer {
                        shard_id: op.shard_id,
                        to_shard_id: op.to_shard_id,
                        from_peer_id: op.from_peer_id,
                        to_peer_id: op.to_peer_id,
                        method: op.method.try_into()?,
                    },
                })
            }
        })
    }
}

impl From<api::grpc::qdrant::ShardKeySelector> for ShardSelectorInternal {
    fn from(value: api::grpc::qdrant::ShardKeySelector) -> Self {
        let shard_keys: Vec<_> = value
            .shard_keys
            .into_iter()
            .filter_map(convert_shard_key_from_grpc)
            .collect();

        if shard_keys.len() == 1 {
            ShardSelectorInternal::ShardKey(shard_keys.into_iter().next().unwrap())
        } else {
            ShardSelectorInternal::ShardKeys(shard_keys)
        }
    }
}

impl TryFrom<api::grpc::qdrant::SparseVectorConfig> for SparseVectorsConfig {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::SparseVectorConfig) -> Result<Self, Self::Error> {
        let api::grpc::qdrant::SparseVectorConfig { map } = value;
        map.into_iter()
            .map(|(k, v)| Ok((k, v.try_into()?)))
            .collect::<Result<_, Status>>()
            .map(SparseVectorsConfig)
    }
}

impl TryFrom<api::grpc::qdrant::CollectionConfig> for CollectionConfig {
    type Error = Status;

    fn try_from(config: api::grpc::qdrant::CollectionConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            params: match config.params {
                None => return Err(Status::invalid_argument("Malformed CollectionParams type")),
                Some(params) => CollectionParams {
                    vectors: match params.vectors_config {
                        None => {
                            return Err(Status::invalid_argument(
                                "Expected `vectors` - configuration for vector storage",
                            ))
                        }
                        Some(vector_config) => match vector_config.config {
                            None => {
                                return Err(Status::invalid_argument(
                                    "Expected `vectors` - configuration for vector storage",
                                ))
                            }
                            Some(api::grpc::qdrant::vectors_config::Config::Params(params)) => {
                                VectorsConfig::Single(params.try_into()?)
                            }
                            Some(api::grpc::qdrant::vectors_config::Config::ParamsMap(
                                params_map,
                            )) => VectorsConfig::Multi(
                                params_map
                                    .map
                                    .into_iter()
                                    .map(|(k, v)| Ok((k, v.try_into()?)))
                                    .collect::<Result<BTreeMap<String, VectorParams>, Status>>()?,
                            ),
                        },
                    },
                    sparse_vectors: params
                        .sparse_vectors_config
                        .map(|v| SparseVectorsConfig::try_from(v).map(|SparseVectorsConfig(x)| x))
                        .transpose()?,
                    shard_number: NonZeroU32::new(params.shard_number)
                        .ok_or_else(|| Status::invalid_argument("`shard_number` cannot be zero"))?,
                    on_disk_payload: params.on_disk_payload,
                    replication_factor: NonZeroU32::new(
                        params
                            .replication_factor
                            .unwrap_or_else(|| default_replication_factor().get()),
                    )
                    .ok_or_else(|| {
                        Status::invalid_argument("`replication_factor` cannot be zero")
                    })?,
                    write_consistency_factor: NonZeroU32::new(
                        params
                            .write_consistency_factor
                            .unwrap_or_else(|| default_write_consistency_factor().get()),
                    )
                    .ok_or_else(|| {
                        Status::invalid_argument("`write_consistency_factor` cannot be zero")
                    })?,

                    read_fan_out_factor: params.read_fan_out_factor,
                    sharding_method: params
                        .sharding_method
                        .map(sharding_method_from_proto)
                        .transpose()?,
                    on_disk_payload_uses_mmap: false,
                    on_disk_sparse_vectors_uses_mmap: false,
                },
            },
            hnsw_config: match config.hnsw_config {
                None => return Err(Status::invalid_argument("Malformed HnswConfig type")),
                Some(hnsw_config) => HnswConfig::from(hnsw_config),
            },
            optimizer_config: match config.optimizer_config {
                None => return Err(Status::invalid_argument("Malformed OptimizerConfig type")),
                Some(optimizer_config) => OptimizersConfig::try_from(optimizer_config)?,
            },
            wal_config: match config.wal_config {
                None => return Err(Status::invalid_argument("Malformed WalConfig type")),
                Some(wal_config) => Some(WalConfig::from(wal_config)),
            },
            quantization_config: {
                if let Some(config) = config.quantization_config {
                    Some(QuantizationConfig::try_from(config)?)
                } else {
                    None
                }
            },
            strict_mode_config: config.strict_mode_config.map(StrictModeConfig::from),
        })
    }
}
