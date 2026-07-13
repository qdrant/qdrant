//! Edge shard configuration: user-facing params and conversion to/from SegmentConfig.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use common::fs::{atomic_save_json, read_json};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{
    Distance, HnswConfig, PayloadStorageType, QuantizationConfig, SegmentConfig, VectorName,
    VectorNameBuf,
};
use serde::{Deserialize, Serialize};
use shard::operations::optimization::OptimizerThresholds;
use wal::WalOptions;

use super::optimizers::EdgeOptimizersConfig;
use super::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

/// File name for the persisted edge shard config.
pub(crate) const EDGE_CONFIG_FILE: &str = "edge_config.json";

/// Full configuration for an edge shard.
///
/// `vectors` and `sparse_vectors` define the stored data: when loading an existing shard they are
/// validated for compatibility against the segments if provided (non-empty), or taken from the
/// persisted config / the segments themselves if not.
///
/// Everything else is tunable and `None` means "not specified": when loading an existing shard
/// each parameter resolves through provided → persisted → derived from segments → default (see
/// [`EdgeConfig::fill_unspecified_from`]), so leaving a parameter unspecified keeps the shard as
/// it is, while a `Some` value explicitly overwrites it and existing segments converge to it
/// through the optimizers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EdgeConfig {
    /// If true, payload is stored on disk (mmap); otherwise in RAM. Same as `CollectionParams::on_disk_payload`.
    /// `None` defaults to on-disk, see [`EdgeConfig::on_disk_payload`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk_payload: Option<bool>,
    /// Dense vector params per vector name.
    #[serde(default)]
    pub vectors: HashMap<VectorNameBuf, EdgeVectorParams>,
    /// Sparse vector params per vector name.
    #[serde(default)]
    pub sparse_vectors: HashMap<VectorNameBuf, EdgeSparseVectorParams>,
    /// Global HNSW config; per-vector override is in `vectors[].hnsw_config`.
    /// `None` defaults to [`HnswConfig::default`], see [`EdgeConfig::hnsw_config`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
    /// Global quantization config for all vectors
    /// Per-vector override in in `vectors[].quantization_config`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
    /// `None` defaults to [`EdgeOptimizersConfig::default`], see [`EdgeConfig::optimizers`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimizers: Option<EdgeOptimizersConfig>,
    /// WAL options for the shard. `None` keeps the WAL crate's defaults
    /// (32 MiB segment capacity). Override for embedded/mobile deployments
    /// where the default segment size is too large.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_options: Option<WalOptions>,
    /// Number of threads in the shard's search thread pool. The pool executes per-segment reads
    /// (search, scroll, count, facet, ...) in parallel and loads segments in parallel. `None` (the
    /// default) derives the count from the number of CPUs, matching the core search runtime — see
    /// [`EdgeConfig::search_thread_count`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_search_threads: Option<usize>,
}

impl EdgeConfig {
    /// Start building an [`EdgeConfig`] with a fluent API.
    pub fn builder() -> crate::builders::EdgeConfigBuilder {
        crate::builders::EdgeConfigBuilder::new()
    }

    /// Effective payload storage location: on-disk unless explicitly set to `false`.
    pub fn on_disk_payload(&self) -> bool {
        self.on_disk_payload.unwrap_or(true)
    }

    /// Effective global HNSW config: [`HnswConfig::default`] unless explicitly set.
    pub fn hnsw_config(&self) -> HnswConfig {
        self.hnsw_config.unwrap_or_default()
    }

    /// Effective optimizers config: [`EdgeOptimizersConfig::default`] unless explicitly set.
    pub fn optimizers(&self) -> EdgeOptimizersConfig {
        self.optimizers.clone().unwrap_or_default()
    }

    /// Fill parameters left unspecified from `base`, keeping explicitly provided values.
    ///
    /// Chained over the fallback layers of [`EdgeShard::load`](crate::EdgeShard::load):
    /// provided → persisted → derived from segments → default.
    ///
    /// For tunables, unspecified means `None`. For `vectors` and `sparse_vectors` it means an
    /// empty map: a non-empty map is taken as-is (never merged element-wise) — those define the
    /// stored data, so the load path validates them against existing segments instead of
    /// converging via the optimizers like the tunables do.
    pub fn fill_unspecified_from(self, base: &EdgeConfig) -> Self {
        let Self {
            on_disk_payload,
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config,
            optimizers,
            wal_options,
            max_search_threads,
        } = self;
        Self {
            on_disk_payload: on_disk_payload.or(base.on_disk_payload),
            vectors: if vectors.is_empty() {
                base.vectors.clone()
            } else {
                vectors
            },
            sparse_vectors: if sparse_vectors.is_empty() {
                base.sparse_vectors.clone()
            } else {
                sparse_vectors
            },
            hnsw_config: hnsw_config.or(base.hnsw_config),
            quantization_config: quantization_config.or_else(|| base.quantization_config.clone()),
            optimizers: optimizers.or_else(|| base.optimizers.clone()),
            wal_options: wal_options.or_else(|| base.wal_options.clone()),
            max_search_threads: max_search_threads.or(base.max_search_threads),
        }
    }

    /// Accumulate the config derived from one more segment into `acc`.
    ///
    /// Building block for the "derived from segments" layer of the config fallback chain: fold
    /// this over *all* segments, so that a segment carrying no information about a parameter
    /// (e.g. a plain appendable segment says nothing about HNSW) never masks one that does (an
    /// indexed segment carries the actual build parameters). Fold in a deterministic segment
    /// order: when segments disagree on a parameter, the first one providing it wins.
    pub(crate) fn fold_from_segment_config(acc: Option<Self>, segment: &SegmentConfig) -> Self {
        let derived = Self::from_segment_config(segment);
        match acc {
            Some(acc) => acc.fill_unspecified_from(&derived),
            None => derived,
        }
    }

    /// Build from existing segment config. Fills all parameters that can be inferred.
    pub fn from_segment_config(segment: &SegmentConfig) -> Self {
        let SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        } = segment;

        let vectors = vector_data
            .iter()
            .map(|(name, v)| (name.clone(), EdgeVectorParams::from_vector_data_config(v)))
            .collect();

        let sparse_vectors = sparse_vector_data
            .iter()
            .map(|(name, s)| {
                (
                    name.clone(),
                    EdgeSparseVectorParams::from_sparse_vector_data_config(s),
                )
            })
            .collect();

        let on_disk_payload = payload_storage_type.is_on_disk();

        // Infer global hnsw_config from per-vector HNSW configs when all agree
        let hnsw_configs: Vec<HnswConfig> = vector_data
            .values()
            .filter_map(|v| match &v.index {
                segment::types::Indexes::Plain {} => None,
                segment::types::Indexes::Hnsw(h) => Some(*h),
            })
            .collect();
        let hnsw_config = hnsw_configs.first().and_then(|first| {
            if hnsw_configs.iter().all(|h| h == first) {
                Some(*first)
            } else {
                None
            }
        });

        Self {
            on_disk_payload: Some(on_disk_payload),
            vectors,
            sparse_vectors,
            hnsw_config,
            quantization_config: None,
            optimizers: None,
            wal_options: None,
            max_search_threads: None,
        }
    }

    /// Resolve the configured [`max_search_threads`](Self::max_search_threads) into a concrete
    /// thread count. `None` derives the count from the number of CPUs, matching the core search
    /// runtime (`common::defaults::search_thread_count`).
    pub fn search_thread_count(&self) -> usize {
        common::defaults::search_thread_count(self.max_search_threads.unwrap_or(0))
    }

    /// Check compatibility with a segment config (e.g. loaded segment).
    pub fn check_compatible_with_segment_config(
        &self,
        other: &SegmentConfig,
    ) -> Result<(), String> {
        self.plain_segment_config().check_compatible(other)
    }

    /// Segment config for creating appendable segments only.
    /// Does not contain any HNSW configuration (plain index only).
    pub fn plain_segment_config(&self) -> SegmentConfig {
        let payload_storage_type = PayloadStorageType::from_on_disk_payload(self.on_disk_payload());
        let vector_data = self
            .vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_plain_vector_data_config(self.quantization_config.as_ref()),
                )
            })
            .collect();

        let sparse_vector_data = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_plain_sparse_vector_data_config()))
            .collect();

        SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        }
    }

    /// All vector names (dense and sparse) currently present in this config.
    ///
    /// Must cover both kinds: a segment's `vector_data` holds dense and sparse vectors together,
    /// so the optimizer merge consults this set for both.
    pub fn vector_names(&self) -> HashSet<VectorNameBuf> {
        self.vectors
            .keys()
            .chain(self.sparse_vectors.keys())
            .cloned()
            .collect()
    }

    /// Build segment optimizer config from this config (for blocking optimizers).
    /// Use this instead of converting to SegmentConfig first.
    pub fn segment_optimizer_config(&self) -> shard::optimizers::config::SegmentOptimizerConfig {
        use shard::optimizers::config::SegmentOptimizerConfig;

        let SegmentConfig {
            vector_data: plain_dense_vector_config,
            sparse_vector_data: plain_sparse_vector_config,
            payload_storage_type,
        } = self.plain_segment_config();

        let hnsw_config = self.hnsw_config();
        let dense_vector = self
            .vectors
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    p.to_dense_vector_optimizer_config(
                        &hnsw_config,
                        self.quantization_config.as_ref(),
                    ),
                )
            })
            .collect();

        let sparse_vector = self
            .sparse_vectors
            .iter()
            .map(|(name, p)| (name.clone(), p.to_sparse_vector_optimizer_config()))
            .collect();

        SegmentOptimizerConfig {
            payload_storage_type,
            plain_dense_vector_config,
            plain_sparse_vector_config,
            dense_vector,
            sparse_vector,
            live_vector_names: None,
        }
    }

    /// Return vector data config for a named vector (for read-only use, e.g. query).
    /// Uses plain index; for optimizer/segment creation use segment_optimizer_config or plain_segment_config.
    pub fn vector_data_config(
        &self,
        name: &VectorNameBuf,
    ) -> Option<segment::types::VectorDataConfig> {
        self.vectors
            .get(name)
            .map(|p| p.to_plain_vector_data_config(self.quantization_config.as_ref()))
    }

    /// Distance of a named vector, mirroring `CollectionParams::get_distance`:
    /// sparse vectors always score with `Dot`.
    pub fn get_distance(&self, vector_name: &VectorName) -> OperationResult<Distance> {
        if let Some(params) = self.vectors.get(vector_name) {
            Ok(params.distance)
        } else if self.sparse_vectors.contains_key(vector_name) {
            Ok(Distance::Dot)
        } else {
            Err(OperationError::vector_name_not_exists(vector_name))
        }
    }

    pub fn optimizer_thresholds(&self, num_indexing_threads: usize) -> OptimizerThresholds {
        let optimizers = self.optimizers();
        OptimizerThresholds {
            memmap_threshold_kb: usize::MAX,
            indexing_threshold_kb: optimizers.get_indexing_threshold_kb(),
            max_segment_size_kb: optimizers.get_max_segment_size_kb(num_indexing_threads),
            deferred_internal_id: None,
        }
    }

    pub fn save(&self, path: &Path) -> OperationResult<()> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        atomic_save_json(&config_path, self).map_err(|e| {
            OperationError::service_error(format!(
                "failed to write {}: {}",
                config_path.display(),
                e
            ))
        })
    }

    pub fn load(path: &Path) -> Option<OperationResult<Self>> {
        let config_path = path.join(EDGE_CONFIG_FILE);
        match fs_err::exists(&config_path) {
            Ok(false) => return None,
            Err(e) => return Some(Err(OperationError::from(e))),
            Ok(true) => {}
        }
        Some(read_json(&config_path).map_err(OperationError::from))
    }

    pub fn set_hnsw_config(&mut self, hnsw_config: HnswConfig) {
        self.hnsw_config = Some(hnsw_config);
    }

    pub fn set_vector_hnsw_config(
        &mut self,
        vector_name: &str,
        hnsw_config: HnswConfig,
    ) -> OperationResult<()> {
        let name = VectorNameBuf::from(vector_name);
        let params = self
            .vectors
            .get_mut(&name)
            .ok_or_else(|| OperationError::vector_name_not_exists(vector_name))?;
        params.hnsw_config = Some(hnsw_config);
        Ok(())
    }

    pub fn set_optimizers_config(&mut self, optimizers: EdgeOptimizersConfig) {
        self.optimizers = Some(optimizers);
    }
}

#[cfg(test)]
mod tests {
    use segment::types::{Distance, Indexes, VectorDataConfig, VectorStorageType};

    use super::*;

    fn segment_config(index: Indexes) -> SegmentConfig {
        SegmentConfig {
            vector_data: HashMap::from([(
                "vec".to_string(),
                VectorDataConfig {
                    size: 4,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::ChunkedMmap,
                    index,
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: HashMap::new(),
            payload_storage_type: PayloadStorageType::from_on_disk_payload(true),
        }
    }

    /// A plain (appendable) segment carries no HNSW parameters; folding must not let it mask an
    /// indexed segment's actual build parameters, regardless of segment order.
    #[test]
    fn fold_derives_hnsw_from_indexed_segment_regardless_of_order() {
        let hnsw = HnswConfig {
            m: 32,
            ..HnswConfig::default()
        };
        let plain = segment_config(Indexes::Plain {});
        let indexed = segment_config(Indexes::Hnsw(hnsw));

        for segments in [[&plain, &indexed], [&indexed, &plain]] {
            let derived = segments
                .into_iter()
                .fold(None, |acc, segment| {
                    Some(EdgeConfig::fold_from_segment_config(acc, segment))
                })
                .unwrap();
            assert_eq!(derived.hnsw_config, Some(hnsw));
            assert!(derived.vectors.contains_key("vec"));
        }
    }
}
