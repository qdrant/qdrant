use std::collections::{BTreeMap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};

use api::grpc::conversions::{from_grpc_dist, payload_to_proto, proto_to_payloads};
use api::grpc::qdrant::quantization_config_diff::Quantization;
use api::grpc::qdrant::update_collection_cluster_setup_request::Operation as ClusterOperationsPb;
use itertools::Itertools;
use segment::data_types::vectors::{
    Named, NamedRecoQuery, NamedVector, VectorStruct, DEFAULT_VECTOR_NAME,
};
use segment::types::{Distance, QuantizationConfig};
use segment::vector_storage::query::reco_query::RecoQuery;
use tonic::Status;

use super::types::{
    BaseGroupRequest, CoreSearchRequest, GroupsResult, PointGroup, QueryEnum, RecommendExample,
    RecommendGroupsRequest, RecommendStrategy, SearchGroupsRequest, VectorParamsDiff,
    VectorsConfigDiff,
};
use crate::config::{
    default_replication_factor, default_write_consistency_factor, CollectionConfig,
    CollectionParams, WalConfig,
};
use crate::lookup::types::WithLookupInterface;
use crate::lookup::WithLookup;
use crate::operations::cluster_ops::{
    AbortTransferOperation, ClusterOperations, DropReplicaOperation, MoveShard, MoveShardOperation,
    Replica, ReplicateShardOperation,
};
use crate::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
    WalConfigDiff,
};
use crate::operations::point_ops::PointsSelector::PointIdsSelector;
use crate::operations::point_ops::{
    Batch, FilterSelector, PointIdsList, PointStruct, PointsSelector, WriteOrdering,
};
use crate::operations::types::{
    AliasDescription, CollectionClusterInfo, CollectionInfo, CollectionStatus, CountResult,
    LocalShardInfo, LookupLocation, OptimizersStatus, RecommendRequest, Record, RemoteShardInfo,
    SearchRequest, ShardTransferInfo, UpdateResult, UpdateStatus, VectorParams, VectorsConfig,
};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::remote_shard::{CollectionCoreSearchRequest, CollectionSearchRequest};
use crate::shards::shard::ShardKey;

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
            match api::grpc::qdrant::WriteOrderingType::from_i32(write_ordering.r#type) {
                None => {
                    return Err(Status::invalid_argument(format!(
                        "cannot convert ordering: {}",
                        write_ordering.r#type
                    )))
                }
                Some(res) => res,
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
) -> Result<Record, tonic::Status> {
    let id = point
        .id
        .ok_or_else(|| tonic::Status::invalid_argument("retrieved point does not have an ID"))?
        .try_into()?;

    let payload = if with_payload {
        Some(api::grpc::conversions::proto_to_payloads(point.payload)?)
    } else {
        debug_assert!(point.payload.is_empty());
        None
    };

    let vector = point
        .vectors
        .map(|vectors| vectors.try_into())
        .transpose()?;

    Ok(Record {
        id,
        payload,
        vector,
    })
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

impl From<api::grpc::qdrant::OptimizersConfigDiff> for OptimizersConfigDiff {
    fn from(value: api::grpc::qdrant::OptimizersConfigDiff) -> Self {
        Self {
            deleted_threshold: value.deleted_threshold,
            vacuum_min_vector_number: value.vacuum_min_vector_number.map(|v| v as usize),
            default_segment_number: value.default_segment_number.map(|v| v as usize),
            max_segment_size: value.max_segment_size.map(|v| v as usize),
            memmap_threshold: value.memmap_threshold.map(|v| v as usize),
            indexing_threshold: value.indexing_threshold.map(|v| v as usize),
            flush_interval_sec: value.flush_interval_sec,
            max_optimization_threads: value.max_optimization_threads.map(|v| v as usize),
        }
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
            vectors_count: vectors_count as u64,
            indexed_vectors_count: Some(indexed_vectors_count as u64),
            points_count: points_count as u64,
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
                    max_optimization_threads: Some(
                        config.optimizer_config.max_optimization_threads as u64,
                    ),
                }),
                wal_config: Some(api::grpc::qdrant::WalConfigDiff {
                    wal_capacity_mb: Some(config.wal_config.wal_capacity_mb as u64),
                    wal_segments_ahead: Some(config.wal_config.wal_segments_ahead as u64),
                }),
                quantization_config: config.quantization_config.map(|x| x.into()),
            }),
            payload_schema: payload_schema
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<Record> for api::grpc::qdrant::RetrievedPoint {
    fn from(record: Record) -> Self {
        let vectors = record.vector.map(|vector_struct| vector_struct.into());

        Self {
            id: Some(record.id.into()),
            payload: record.payload.map(payload_to_proto).unwrap_or_default(),
            vectors,
        }
    }
}

impl TryFrom<i32> for CollectionStatus {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(CollectionStatus::Green),
            2 => Ok(CollectionStatus::Yellow),
            3 => Ok(CollectionStatus::Red),
            _ => Err(Status::invalid_argument("Malformed CollectionStatus type")),
        }
    }
}

impl From<api::grpc::qdrant::OptimizersConfigDiff> for OptimizersConfig {
    fn from(optimizer_config: api::grpc::qdrant::OptimizersConfigDiff) -> Self {
        Self {
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
            max_optimization_threads: optimizer_config
                .max_optimization_threads
                .unwrap_or_default() as usize,
        }
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
        })
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
                },
            },
            hnsw_config: match config.hnsw_config {
                None => return Err(Status::invalid_argument("Malformed HnswConfig type")),
                Some(hnsw_config) => hnsw_config.into(),
            },
            optimizer_config: match config.optimizer_config {
                None => return Err(Status::invalid_argument("Malformed OptimizerConfig type")),
                Some(optimizer_config) => optimizer_config.into(),
            },
            wal_config: match config.wal_config {
                None => return Err(Status::invalid_argument("Malformed WalConfig type")),
                Some(wal_config) => wal_config.into(),
            },
            quantization_config: {
                if let Some(config) = config.quantization_config {
                    Some(config.try_into()?)
                } else {
                    None
                }
            },
        })
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
                status: collection_info_response.status.try_into()?,
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
                vectors_count: collection_info_response.vectors_count as usize,
                indexed_vectors_count: collection_info_response
                    .indexed_vectors_count
                    .unwrap_or_default() as usize,
                points_count: collection_info_response.points_count as usize,
                segments_count: collection_info_response.segments_count as usize,
                config: match collection_info_response.config {
                    None => {
                        return Err(Status::invalid_argument("Malformed CollectionConfig type"))
                    }
                    Some(config) => config.try_into()?,
                },
                payload_schema: collection_info_response
                    .payload_schema
                    .into_iter()
                    .map(|(k, v)| v.try_into().map(|v| (k, v)))
                    .try_collect()?,
            }),
        }
    }
}

impl TryFrom<api::grpc::qdrant::PointStruct> for PointStruct {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::PointStruct) -> Result<Self, Self::Error> {
        let api::grpc::qdrant::PointStruct {
            id,
            vectors,
            payload,
        } = value;

        let converted_payload = proto_to_payloads(payload)?;

        let vector_struct: VectorStruct = match vectors {
            None => return Err(Status::invalid_argument("Expected some vectors")),
            Some(vectors) => vectors.try_into()?,
        };

        Ok(Self {
            id: id
                .ok_or_else(|| Status::invalid_argument("Empty ID is not allowed"))?
                .try_into()?,
            vector: vector_struct,
            payload: Some(converted_payload),
        })
    }
}

impl TryFrom<PointStruct> for api::grpc::qdrant::PointStruct {
    type Error = Status;

    fn try_from(value: PointStruct) -> Result<Self, Self::Error> {
        let vectors: api::grpc::qdrant::Vectors = value.vector.into();

        let id = value.id;
        let payload = value.payload;

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

impl TryFrom<Batch> for Vec<api::grpc::qdrant::PointStruct> {
    type Error = Status;

    fn try_from(batch: Batch) -> Result<Self, Self::Error> {
        let mut points = Vec::new();
        let all_vectors = batch.vectors.into_all_vectors(batch.ids.len());
        for (i, p_id) in batch.ids.into_iter().enumerate() {
            let id = Some(p_id.into());
            let vector = all_vectors.get(i).cloned();
            let payload = batch.payloads.as_ref().and_then(|payloads| {
                payloads.get(i).map(|payload| match payload {
                    None => HashMap::new(),
                    Some(payload) => payload_to_proto(payload.clone()),
                })
            });
            let vectors: Option<VectorStruct> = vector.map(|v| v.into());

            let point = api::grpc::qdrant::PointStruct {
                id,
                vectors: vectors.map(|v| v.into()),
                payload: payload.unwrap_or_default(),
            };
            points.push(point);
        }

        Ok(points)
    }
}

impl TryFrom<api::grpc::qdrant::PointsSelector> for PointsSelector {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::PointsSelector) -> Result<Self, Self::Error> {
        match value.points_selector_one_of {
            Some(api::grpc::qdrant::points_selector::PointsSelectorOneOf::Points(points)) => {
                Ok(PointIdsSelector(PointIdsList {
                    points: points
                        .ids
                        .into_iter()
                        .map(|p| p.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                }))
            }
            Some(api::grpc::qdrant::points_selector::PointsSelectorOneOf::Filter(f)) => {
                Ok(PointsSelector::FilterSelector(FilterSelector {
                    filter: f.try_into()?,
                }))
            }
            _ => Err(Status::invalid_argument("Malformed PointsSelector type")),
        }
    }
}

impl From<UpdateResult> for api::grpc::qdrant::UpdateResult {
    fn from(value: UpdateResult) -> Self {
        Self {
            operation_id: value.operation_id,
            status: match value.status {
                UpdateStatus::Acknowledged => api::grpc::qdrant::UpdateStatus::Acknowledged as i32,
                UpdateStatus::Completed => api::grpc::qdrant::UpdateStatus::Completed as i32,
            },
        }
    }
}

impl TryFrom<api::grpc::qdrant::UpdateResult> for UpdateResult {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::UpdateResult) -> Result<Self, Self::Error> {
        Ok(Self {
            operation_id: value.operation_id,
            status: match value.status {
                status if status == api::grpc::qdrant::UpdateStatus::Acknowledged as i32 => {
                    UpdateStatus::Acknowledged
                }
                status if status == api::grpc::qdrant::UpdateStatus::Completed as i32 => {
                    UpdateStatus::Completed
                }
                _ => return Err(Status::invalid_argument("Malformed UpdateStatus type")),
            },
        })
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

// Use wrapper type to bundle CollectionId & SearchRequest
impl<'a> From<CollectionSearchRequest<'a>> for api::grpc::qdrant::SearchPoints {
    fn from(value: CollectionSearchRequest<'a>) -> Self {
        let (collection_id, request) = value.0;

        Self {
            collection_name: collection_id,
            vector: request.vector.get_vector().to_vec(),
            filter: request.filter.clone().map(|f| f.into()),
            limit: request.limit as u64,
            with_vectors: request.with_vector.clone().map(|wv| wv.into()),
            with_payload: request.with_payload.clone().map(|wp| wp.into()),
            params: request.params.map(|sp| sp.into()),
            score_threshold: request.score_threshold,
            offset: Some(request.offset as u64),
            vector_name: match request.vector.get_name() {
                DEFAULT_VECTOR_NAME => None,
                vector_name => Some(vector_name.to_string()),
            },
            read_consistency: None,
        }
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
                        positives: named
                            .query
                            .positives
                            .into_iter()
                            .map(|v| api::grpc::qdrant::Vector { data: v })
                            .collect(),
                        negatives: named
                            .query
                            .negatives
                            .into_iter()
                            .map(|v| api::grpc::qdrant::Vector { data: v })
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

impl TryFrom<api::grpc::qdrant::CoreSearchPoints> for CoreSearchRequest {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::CoreSearchPoints) -> Result<Self, Self::Error> {
        let query = value
            .query
            .and_then(|query| query.query)
            .map(|query| match query {
                api::grpc::qdrant::query_enum::Query::NearestNeighbors(vector) => {
                    QueryEnum::Nearest(match value.vector_name {
                        Some(name) => NamedVector {
                            name,
                            vector: vector.data,
                        }
                        .into(),
                        None => vector.data.into(),
                    })
                }
                api::grpc::qdrant::query_enum::Query::RecommendBestScore(query) => {
                    QueryEnum::RecommendBestScore(NamedRecoQuery {
                        query: RecoQuery::new(
                            query.positives.into_iter().map(|v| v.data).collect(),
                            query.negatives.into_iter().map(|v| v.data).collect(),
                        ),
                        using: value.vector_name,
                    })
                }
            })
            .ok_or(Status::invalid_argument("Query is not specified"))?;

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

impl TryFrom<api::grpc::qdrant::SearchPoints> for SearchRequest {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::SearchPoints) -> Result<Self, Self::Error> {
        Ok(SearchRequest {
            vector: match value.vector_name {
                Some(vector_name) => NamedVector {
                    name: vector_name,
                    vector: value.vector,
                }
                .into(),
                None => value.vector.into(),
            },
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

impl TryFrom<api::grpc::qdrant::SearchPointGroups> for SearchGroupsRequest {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::SearchPointGroups) -> Result<Self, Self::Error> {
        let search_points = api::grpc::qdrant::SearchPoints {
            vector: value.vector,
            filter: value.filter,
            params: value.params,
            with_payload: value.with_payload,
            with_vectors: value.with_vectors,
            score_threshold: value.score_threshold,
            vector_name: value.vector_name,
            limit: 0,
            offset: None,
            collection_name: String::new(),
            read_consistency: None,
        };

        let SearchRequest {
            vector,
            filter,
            params,
            limit: _,
            offset: _,
            with_payload,
            with_vector,
            score_threshold,
        } = search_points.try_into()?;

        Ok(SearchGroupsRequest {
            vector,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            group_request: BaseGroupRequest {
                group_by: value.group_by,
                limit: value.limit,
                group_size: value.group_size,
                with_lookup: value.with_lookup.map(|l| l.try_into()).transpose()?,
            },
        })
    }
}

impl From<PointGroup> for api::grpc::qdrant::PointGroup {
    fn from(group: PointGroup) -> Self {
        Self {
            hits: group.hits.into_iter().map_into().collect(),
            id: Some(group.id.into()),
            lookup: group.lookup.map(|record| record.into()),
        }
    }
}

impl From<api::grpc::qdrant::LookupLocation> for LookupLocation {
    fn from(value: api::grpc::qdrant::LookupLocation) -> Self {
        Self {
            collection: value.collection_name,
            vector: value.vector_name,
        }
    }
}

impl From<api::grpc::qdrant::RecommendStrategy> for RecommendStrategy {
    fn from(value: api::grpc::qdrant::RecommendStrategy) -> Self {
        match value {
            api::grpc::qdrant::RecommendStrategy::AverageVector => RecommendStrategy::AverageVector,
            api::grpc::qdrant::RecommendStrategy::BestScore => RecommendStrategy::BestScore,
        }
    }
}

impl TryFrom<i32> for RecommendStrategy {
    type Error = Status;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let strategy = api::grpc::qdrant::RecommendStrategy::from_i32(value).ok_or_else(|| {
            Status::invalid_argument(format!("Unknown recommend strategy: {}", value))
        })?;
        Ok(strategy.into())
    }
}

impl TryFrom<api::grpc::qdrant::PointId> for RecommendExample {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::PointId) -> Result<Self, Self::Error> {
        Ok(Self::PointId(value.try_into()?))
    }
}

impl From<api::grpc::qdrant::Vector> for RecommendExample {
    fn from(value: api::grpc::qdrant::Vector) -> Self {
        Self::Vector(value.data)
    }
}

impl TryFrom<api::grpc::qdrant::RecommendPoints> for RecommendRequest {
    type Error = Status;

    fn try_from(value: api::grpc::qdrant::RecommendPoints) -> Result<Self, Self::Error> {
        let positive_ids = value
            .positive
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<RecommendExample>, Self::Error>>()?;

        let positive_vectors = value.positive_vectors.into_iter().map(Into::into).collect();
        let positive = [positive_ids, positive_vectors].concat();

        let negative_ids = value
            .negative
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<RecommendExample>, Self::Error>>()?;

        let negative_vectors = value.negative_vectors.into_iter().map(Into::into).collect();
        let negative = [negative_ids, negative_vectors].concat();

        Ok(RecommendRequest {
            positive,
            negative,
            strategy: value.strategy.map(|s| s.try_into()).transpose()?,
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
            using: value.using.map(|name| name.into()),
            lookup_from: value.lookup_from.map(|x| x.into()),
        })
    }
}

impl TryFrom<api::grpc::qdrant::RecommendPointGroups> for RecommendGroupsRequest {
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
            limit: 0,
            offset: None,
            collection_name: String::new(),
            positive_vectors: value.positive_vectors,
            negative_vectors: value.negative_vectors,
        };

        let RecommendRequest {
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

        Ok(RecommendGroupsRequest {
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
                group_by: value.group_by,
                limit: value.limit,
                group_size: value.group_size,
                with_lookup: value.with_lookup.map(|l| l.try_into()).transpose()?,
            },
        })
    }
}

impl From<GroupsResult> for api::grpc::qdrant::GroupsResult {
    fn from(value: GroupsResult) -> Self {
        Self {
            groups: value.groups.into_iter().map(Into::into).collect(),
        }
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
            }
            .into(),
            hnsw_config: value.hnsw_config.map(Into::into),
            quantization_config: value.quantization_config.map(Into::into),
            on_disk: value.on_disk,
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

impl From<ShardKey> for api::grpc::qdrant::ShardKey {
    fn from(value: ShardKey) -> Self {
        match value {
            ShardKey::Keyword(keyword) => Self {
                key: Some(api::grpc::qdrant::shard_key::Key::Keyword(keyword)),
            },
            ShardKey::Number(number) => Self {
                key: Some(api::grpc::qdrant::shard_key::Key::Number(number)),
            },
        }
    }
}

impl From<LocalShardInfo> for api::grpc::qdrant::LocalShardInfo {
    fn from(value: LocalShardInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            points_count: value.points_count as u64,
            state: value.state as i32,
            shard_key: value.shard_key.map(Into::into),
        }
    }
}

impl From<RemoteShardInfo> for api::grpc::qdrant::RemoteShardInfo {
    fn from(value: RemoteShardInfo) -> Self {
        Self {
            shard_id: value.shard_id,
            peer_id: value.peer_id,
            state: value.state as i32,
            shard_key: value.shard_key.map(Into::into),
        }
    }
}

impl From<ShardTransferInfo> for api::grpc::qdrant::ShardTransferInfo {
    fn from(value: ShardTransferInfo) -> Self {
        Self {
            shard_id: value.shard_id,
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
        }
    }
}

impl From<api::grpc::qdrant::MoveShard> for MoveShard {
    fn from(value: api::grpc::qdrant::MoveShard) -> Self {
        Self {
            shard_id: value.shard_id,
            from_peer_id: value.from_peer_id,
            to_peer_id: value.to_peer_id,
        }
    }
}

impl From<ClusterOperationsPb> for ClusterOperations {
    fn from(value: ClusterOperationsPb) -> Self {
        match value {
            ClusterOperationsPb::MoveShard(op) => {
                ClusterOperations::MoveShard(MoveShardOperation {
                    move_shard: op.into(),
                })
            }
            ClusterOperationsPb::ReplicateShard(op) => {
                ClusterOperations::ReplicateShard(ReplicateShardOperation {
                    replicate_shard: op.into(),
                })
            }
            ClusterOperationsPb::AbortTransfer(op) => {
                ClusterOperations::AbortTransfer(AbortTransferOperation {
                    abort_transfer: op.into(),
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
        }
    }
}
