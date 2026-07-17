// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::AHashMap;
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig, CompressionRatio, Distance, MultiVectorConfig,
    PayloadFieldSchema, PayloadSchemaType, ProductQuantization, ProductQuantizationConfig,
    QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig, ScalarType,
    TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
};

use super::{ALL_CANDIDATES, COLLECTION_NAME, PEER_ID, QuantizationKind, VectorKind};
use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::config_diff::HnswConfigDiff;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{Datatype, SparseVectorParams, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};
use crate::shards::shard::{PeerId, ShardId};

/// Shared builder plumbing for dense and multi-dense fixture params. Only override when
/// enabled/present: leaving `on_disk` and `datatype` unset (None) preserves the engine's
/// default resolution, so no-flag runs stay byte-identical to before these overrides
/// existed (`None` and `Some(false)` aren't equivalent everywhere: the
/// config-mismatch optimizer, for one, only acts on `Some`).
fn dense_params_builder(
    dim: u64,
    distance: Distance,
    on_disk: bool,
    datatype: Option<Datatype>,
) -> VectorParamsBuilder {
    let mut builder = VectorParamsBuilder::new(dim, distance);
    if on_disk {
        builder = builder.with_on_disk(true);
    }
    if let Some(datatype) = datatype {
        builder = builder.with_datatype(datatype);
    }
    builder
}

/// Materialize the schema `QuantizationConfig` for a candidate's declared kind. All fields
/// are the engine defaults (no quantile / memory-placement / encoding overrides); Scalar
/// matches the config the inline-storage vector has always used, byte for byte.
fn quantization_config(kind: QuantizationKind) -> QuantizationConfig {
    match kind {
        QuantizationKind::Scalar => QuantizationConfig::Scalar(ScalarQuantization {
            scalar: ScalarQuantizationConfig {
                r#type: ScalarType::Int8,
                quantile: None,
                always_ram: None,
                memory: None,
            },
        }),
        QuantizationKind::Product => QuantizationConfig::Product(ProductQuantization {
            product: ProductQuantizationConfig {
                compression: CompressionRatio::X4,
                always_ram: None,
                memory: None,
            },
        }),
        QuantizationKind::Binary => QuantizationConfig::Binary(BinaryQuantization {
            binary: BinaryQuantizationConfig {
                always_ram: None,
                memory: None,
                encoding: None,
                query_encoding: None,
            },
        }),
        QuantizationKind::Turbo => QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: None,
                memory: None,
                bits: None,
            },
        }),
        QuantizationKind::TurboBits1_5 => QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: None,
                memory: None,
                bits: Some(TurboQuantBitSize::Bits1_5),
            },
        }),
    }
}

/// Build a fresh soak collection rooted at `storage_path`. Creates `collection/` and
/// `snapshots/` subdirs, wiping any pre-existing contents — each soak run starts from a
/// clean slate. If you need to preserve a crashed run's state for post-mortem, copy the
/// directory out before re-running.
pub(super) async fn fixture(
    shard_count: u32,
    storage_path: &Path,
    disable_optimizer: bool,
    max_segment_size_kb: usize,
    indexing_threshold_kb: usize,
    flush_interval_sec: u64,
    on_disk: bool,
) -> (PathBuf, PathBuf, Collection) {
    let collection_dir = storage_path.join("collection");
    let snapshots_dir = storage_path.join("snapshots");
    for dir in [&collection_dir, &snapshots_dir] {
        if dir.exists() {
            fs_err::remove_dir_all(dir)
                .unwrap_or_else(|e| panic!("failed to wipe {}: {e:?}", dir.display()));
        }
        fs_err::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("failed to create {}: {e:?}", dir.display()));
    }

    let wal_config = WalConfig::default();

    // Build the initial vector schema from the candidate pool. The remaining (initially
    // inactive) candidates are reachable through `Op::CreateVectorName` later in the run.
    let mut dense_vectors = BTreeMap::new();
    let mut sparse_vectors = BTreeMap::new();
    for candidate in ALL_CANDIDATES.iter().filter(|c| c.initially_active) {
        let name = candidate.name;
        match candidate.kind {
            VectorKind::Dense(dim) => {
                let mut builder =
                    dense_params_builder(dim, candidate.distance, on_disk, candidate.datatype);
                if let Some(kind) = candidate.quantization {
                    builder = builder.with_quantization_config(quantization_config(kind));
                }
                if candidate.inline_storage {
                    builder = builder.with_hnsw_config(HnswConfigDiff {
                        inline_storage: Some(true),
                        ..Default::default()
                    });
                }
                dense_vectors.insert(name.to_string(), builder.build());
            }
            VectorKind::Sparse => {
                sparse_vectors.insert(
                    name.to_string(),
                    SparseVectorParams {
                        index: None,
                        modifier: None,
                    },
                );
            }
            VectorKind::MultiDense(dim) => {
                // The builder has no `with_multivector_config`; set the field on the built
                // `VectorParams` directly to mark this dense slot as a ColBERT-style multi-vec.
                let mut params =
                    dense_params_builder(dim, candidate.distance, on_disk, candidate.datatype)
                        .build();
                params.multivector_config = Some(MultiVectorConfig::default());
                dense_vectors.insert(name.to_string(), params);
            }
        }
    }

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Multi(dense_vectors),
        sparse_vectors: Some(sparse_vectors),
        shard_number: NonZeroU32::new(shard_count).unwrap(),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        sharding_method: None,
        read_fan_out_factor: None,
        read_fan_out_delay_ms: None,
        on_disk_payload: false,
        payload: None,
    };

    // Optimizer config — `max_segment_size_kb` / `indexing_threshold_kb` are caller-supplied
    // so the binary can experiment with race surfaces under different cadences. Defaults
    // are scaled down from production (~200 MB / ~20 MB) because the soak collection holds
    // only a few MB and would never trip the optimizer at those thresholds.
    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.1,
        vacuum_min_vector_number: 100,
        default_segment_number: 2,
        max_segment_size: Some(max_segment_size_kb),
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(indexing_threshold_kb),
        flush_interval_sec,
        // `Some(0)` disables the optimizer worker entirely (per `OptimizersConfig::fixture`'s
        // comment); `None` lets it use its default thread count.
        max_optimization_threads: if disable_optimizer { Some(0) } else { Some(1) },
        prevent_unoptimized: Some(false),
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let shards: AHashMap<ShardId, HashSet<PeerId>> = (0..shard_count)
        .map(|i| (i, HashSet::from([PEER_ID])))
        .collect();

    let collection = Collection::new(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        &collection_dir,
        &snapshots_dir,
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();

    for shard_id in 0..shard_count {
        collection
            .set_shard_replica_state(shard_id, PEER_ID, ReplicaState::Active, None)
            .await
            .expect("failed to activate shard");
    }

    // Eagerly index all "always-on" payload fields so filter ops run through the index path.
    // `tag` and `url` are left out — `tag` is optional on points; `url` is toggled via
    // `Op::CreateIndex` / `Op::DropIndex` with `prefix: true`.
    let eager_indices: &[(&str, PayloadSchemaType)] = &[
        ("num", PayloadSchemaType::Integer),
        ("f", PayloadSchemaType::Float),
        ("b", PayloadSchemaType::Bool),
        ("d", PayloadSchemaType::Datetime),
        ("g", PayloadSchemaType::Geo),
        ("t", PayloadSchemaType::Text),
    ];
    for (field, schema) in eager_indices {
        collection
            .create_payload_index_with_wait(
                field.parse().unwrap(),
                PayloadFieldSchema::FieldType(*schema),
                true,
                HwMeasurementAcc::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("failed to create eager `{field}` index: {e:?}"));
    }

    (collection_dir, snapshots_dir, collection)
}

pub(super) async fn reopen_collection(collection_dir: &Path, snapshots_dir: &Path) -> Collection {
    Collection::load(
        COLLECTION_NAME.to_string(),
        PEER_ID,
        collection_dir,
        snapshots_dir,
        Arc::new(SharedStorageConfig::default()),
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
}

fn dummy_on_replica_failure() -> ChangePeerFromState {
    Arc::new(move |_peer_id, _shard_id, _from_state| {})
}

fn dummy_request_shard_transfer() -> RequestShardTransfer {
    Arc::new(move |_transfer| {})
}

fn dummy_abort_shard_transfer() -> AbortShardTransfer {
    Arc::new(|_transfer, _reason| {})
}
