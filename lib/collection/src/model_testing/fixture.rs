// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::{
    BoolIndexParams, DatetimeIndexParams, FloatIndexParams, GeoIndexParams, IntegerIndexParams,
    Language, Snowball, SnowballLanguage, SnowballParams, StemmingAlgorithm, StopwordsInterface,
    TextIndexParams, TextScoringParams, TokenizerType,
};
use segment::data_types::modifier::Modifier;
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig, CompressionRatio, Distance, HnswConfig, Memory,
    MultiVectorConfig, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
    ProductQuantization, ProductQuantizationConfig, QuantizationConfig, ScalarQuantization,
    ScalarQuantizationConfig, ScalarType, TurboQuantBitSize, TurboQuantQuantizationConfig,
    TurboQuantization,
};

use super::op::nested_indexes;
use super::{ALL_CANDIDATES, COLLECTION_NAME, PEER_ID, QuantizationKind, VectorKind};
use crate::collection::{Collection, RequestShardTransfer};
use crate::config::{CollectionConfigInternal, CollectionParams, IdTrackerParams, WalConfig};
use crate::operations::config_diff::HnswConfigDiff;
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::{Datatype, SparseIndexParams, SparseVectorParams, VectorsConfig};
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
        QuantizationKind::TurboBits8 => QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: None,
                memory: None,
                bits: Some(TurboQuantBitSize::Bits8),
            },
        }),
    }
}

/// Periodic flush worker interval the collection starts with, in seconds. Matches the shipped
/// default (see `config/config.yaml`); `Op::SetFlushInterval` moves it around during the run.
pub(super) const INITIAL_FLUSH_INTERVAL_SEC: u64 = 5;

/// Storage and index parameters of a run, drawn from its seed. With a single hard-coded choice
/// every run exercised the same variants: payload in memory, the default id tracker placement,
/// a WAL that never rolls over, searches that never reach the HNSW graph, in-memory payload and
/// sparse indexes.
#[derive(Debug, Clone, Copy)]
pub(super) struct FixtureVariation {
    /// Payload on disk (mmap) instead of in memory.
    on_disk_payload: bool,
    /// Placement of the id tracker of indexed segments, `None` for the engine default.
    id_tracker_memory: Option<Memory>,
    /// WAL segment size, `None` for the 32 MB default, which a short run never fills. At 1 MB
    /// the WAL rolls over segments and truncates acknowledged ones.
    wal_capacity_mb: Option<usize>,
    /// HNSW full scan threshold in KB, `None` for the default (10000), under which every search
    /// of this small collection is a plain scan. A low value sends them through the graph.
    hnsw_full_scan_threshold: Option<usize>,
    /// Sparse index on disk (mmap) instead of in memory, `None` for the engine default.
    sparse_index_on_disk: Option<bool>,
    /// IDF modifier on the sparse vectors.
    sparse_idf: bool,
    /// The always-on payload indexes (`num`, `f`, `b`, `d`, `g`) on disk instead of in memory.
    payload_index_on_disk: bool,
}

impl FixtureVariation {
    /// Drawn from a generator of its own, so the op stream of the seed is unchanged.
    pub(super) fn from_seed(seed: u64) -> Self {
        let rng = &mut SmallRng::seed_from_u64(seed ^ 0x0066_6978_7475_7265);
        let memories = [
            None,
            Some(Memory::Cold),
            Some(Memory::Cached),
            Some(Memory::Pinned),
        ];
        Self {
            on_disk_payload: rng.random_bool(0.5),
            id_tracker_memory: *memories.choose(rng).unwrap(),
            wal_capacity_mb: rng.random_bool(0.5).then_some(1),
            hnsw_full_scan_threshold: *[None, Some(1), Some(10)].choose(rng).unwrap(),
            sparse_index_on_disk: *[None, Some(false), Some(true)].choose(rng).unwrap(),
            sparse_idf: rng.random_bool(0.5),
            payload_index_on_disk: rng.random_bool(0.5),
        }
    }

    /// Back to the default full scan threshold. With append-only mutations, a search through
    /// the HNSW graph can return the same point twice from one segment, which trips a
    /// `debug_assert` in the search result processing (`segment/read_view/search.rs`).
    /// TODO(model tester): drop once that is fixed.
    pub(super) fn keep_searches_on_plain_scan(&mut self) {
        self.hnsw_full_scan_threshold = None;
    }
}

/// Schema of an always-on payload index: the plain field type, or its on-disk variant.
fn eager_schema(schema_type: PayloadSchemaType, on_disk: bool) -> PayloadFieldSchema {
    if !on_disk {
        return PayloadFieldSchema::FieldType(schema_type);
    }
    let on_disk = Some(true);
    let params = match schema_type {
        // `range` stays on for `ScrollOrdered`, `lookup` for the equality filters and facets.
        PayloadSchemaType::Integer => PayloadSchemaParams::Integer(IntegerIndexParams {
            lookup: Some(true),
            range: Some(true),
            on_disk,
            ..Default::default()
        }),
        PayloadSchemaType::Float => PayloadSchemaParams::Float(FloatIndexParams {
            on_disk,
            ..Default::default()
        }),
        PayloadSchemaType::Bool => PayloadSchemaParams::Bool(BoolIndexParams {
            on_disk,
            ..Default::default()
        }),
        PayloadSchemaType::Datetime => PayloadSchemaParams::Datetime(DatetimeIndexParams {
            on_disk,
            ..Default::default()
        }),
        PayloadSchemaType::Geo => PayloadSchemaParams::Geo(GeoIndexParams {
            on_disk,
            ..Default::default()
        }),
        PayloadSchemaType::Keyword | PayloadSchemaType::Text | PayloadSchemaType::Uuid => {
            unreachable!("not an always-on index type: {schema_type:?}")
        }
    };
    PayloadFieldSchema::FieldParams(params)
}

/// Build a fresh soak collection rooted at `storage_path`. Creates `collection/` and
/// `snapshots/` subdirs, wiping any pre-existing contents — each soak run starts from a
/// clean slate. If you need to preserve a crashed run's state for post-mortem, copy the
/// directory out before re-running.
#[expect(clippy::too_many_arguments)]
pub(super) async fn fixture(
    shard_count: u32,
    storage_path: &Path,
    disable_optimizer: bool,
    max_segment_size_kb: usize,
    indexing_threshold_kb: usize,
    on_disk: bool,
    text_params: TextIndexParams,
    nested_index_setup: NestedIndexSetup,
    variation: FixtureVariation,
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

    let wal_config = match variation.wal_capacity_mb {
        None => WalConfig::default(),
        Some(wal_capacity_mb) => WalConfig {
            wal_capacity_mb,
            wal_segments_ahead: 0,
            ..WalConfig::default()
        },
    };

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
                        index: variation
                            .sparse_index_on_disk
                            .map(|on_disk| SparseIndexParams {
                                on_disk: Some(on_disk),
                                ..Default::default()
                            }),
                        modifier: variation.sparse_idf.then_some(Modifier::Idf),
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
        on_disk_payload: Some(variation.on_disk_payload),
        payload: None,
        id_tracker: variation.id_tracker_memory.map(|memory| IdTrackerParams {
            memory: Some(memory),
        }),
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
        flush_interval_sec: INITIAL_FLUSH_INTERVAL_SEC,
        // `Some(0)` disables the optimizer worker entirely (per `OptimizersConfig::fixture`'s
        // comment); `None` lets it use its default thread count.
        max_optimization_threads: if disable_optimizer { Some(0) } else { Some(1) },
        prevent_unoptimized: Some(false),
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: match variation.hnsw_full_scan_threshold {
            None => HnswConfig::default(),
            Some(full_scan_threshold) => HnswConfig {
                full_scan_threshold,
                ..HnswConfig::default()
            },
        },
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
        created_at: None,
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
    // `Op::CreateIndex` / `Op::DropIndex` with `prefix: true`. The `offers` indexes are eager
    // only with `NestedIndexSetup::Eager`.
    let eager_indices: &[(&str, PayloadSchemaType)] = &[
        ("num", PayloadSchemaType::Integer),
        ("f", PayloadSchemaType::Float),
        ("b", PayloadSchemaType::Bool),
        ("d", PayloadSchemaType::Datetime),
        ("g", PayloadSchemaType::Geo),
    ];
    // `t` scores, for `QueryText`: it records document lengths, and scoring turns on the
    // positions (`phrase_matching`) term frequencies come from.
    let text_schema = PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
        scoring: Some(TextScoringParams::default()),
        ..text_params
    }));
    let nested_schemas = match nested_index_setup {
        NestedIndexSetup::Eager => nested_indexes().to_vec(),
        NestedIndexSetup::Toggled => Vec::new(),
    };
    let eager_schemas = eager_indices
        .iter()
        .map(|(field, schema)| {
            (
                field.parse().unwrap(),
                eager_schema(*schema, variation.payload_index_on_disk),
            )
        })
        .chain([("t".parse().unwrap(), text_schema)])
        .chain(nested_schemas);
    for (field, schema) in eager_schemas {
        collection
            .create_payload_index_with_wait(field.clone(), schema, true)
            .measured(AmbientContext::new())
            .await
            .unwrap_or_else(|e| panic!("failed to create eager `{field}` index: {e:?}"));
    }

    (collection_dir, snapshots_dir, collection)
}

/// Whether a run starts with the per-element indexes of `offers`.
#[derive(Debug, Clone, Copy)]
pub(super) enum NestedIndexSetup {
    /// Both created with the collection, so nested ops take the per-element path until a
    /// `DropIndex` removes one.
    Eager,
    /// Left to `CreateIndex` / `DropIndex`, so nested ops mostly fall back to the payload.
    Toggled,
}

/// The `offers` index setup of a run, keyed off the seed so the op stream is unchanged, on a bit
/// [`text_index_params`] does not read.
pub(super) fn nested_index_setup(seed: u64) -> NestedIndexSetup {
    if (seed / 12).is_multiple_of(2) {
        NestedIndexSetup::Eager
    } else {
        NestedIndexSetup::Toggled
    }
}

/// The `t` text index of a run, keyed off the seed so the op stream is unchanged. Its parity picks
/// where the index lives once a segment is optimized: in RAM (the immutable index) or on disk (the
/// mmap one, which reads lengths in batches). The rest picks the tokenizer, each variant changing
/// what the words of `random_text` become: case kept, stopwords dropped and stems merged, short
/// and long words filtered out, prefixes indexed, or segmentation by language.
pub(super) fn text_index_params(seed: u64) -> TextIndexParams {
    let memory = if seed.is_multiple_of(2) {
        Memory::Pinned
    } else {
        Memory::Cold
    };
    let base = TextIndexParams {
        memory: Some(memory),
        ..TextIndexParams::default()
    };
    match (seed / 2) % 6 {
        0 => base,
        1 => TextIndexParams {
            tokenizer: TokenizerType::Whitespace,
            lowercase: Some(false),
            ..base
        },
        2 => TextIndexParams {
            stopwords: Some(StopwordsInterface::Language(Language::English)),
            stemmer: Some(StemmingAlgorithm::Snowball(SnowballParams {
                r#type: Snowball::Snowball,
                language: SnowballLanguage::English,
            })),
            ascii_folding: Some(true),
            ..base
        },
        3 => TextIndexParams {
            min_token_len: Some(3),
            max_token_len: Some(7),
            ..base
        },
        4 => TextIndexParams {
            tokenizer: TokenizerType::Prefix,
            min_token_len: Some(2),
            max_token_len: Some(5),
            ..base
        },
        _ => TextIndexParams {
            tokenizer: TokenizerType::Multilingual,
            ..base
        },
    }
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
