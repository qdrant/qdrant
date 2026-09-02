use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use segment::common::BYTES_IN_KB;
use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::{
    Distance, HnswConfig, Indexes, Memory, MultiVectorConfig, PayloadStorageType,
    QuantizationConfig, SegmentConfig, SparseVectorDataConfig, SparseVectorStorageType,
    VectorDataConfig, VectorNameBuf, VectorStorageDatatype, VectorStorageType,
};

pub const TEMP_SEGMENTS_PATH: &str = "temp_segments";
pub const DEFAULT_MAX_SEGMENT_PER_CPU_KB: usize = 256_000;
pub const DEFAULT_INDEXING_THRESHOLD_KB: usize = 10_000;
pub const DEFAULT_DELETED_THRESHOLD: f64 = 0.2;
pub const DEFAULT_VACUUM_MIN_VECTOR_NUMBER: usize = 1000;

#[derive(Debug, Clone, PartialEq)]
pub struct DenseVectorOptimizerConfig {
    pub size: usize,
    pub distance: Distance,
    pub on_disk: Option<bool>,
    pub memory: Option<Memory>,
    pub hnsw_config: HnswConfig,
    pub quantization_config: Option<QuantizationConfig>,
    pub multivector_config: Option<MultiVectorConfig>,
    pub datatype: Option<VectorStorageDatatype>,
}

impl DenseVectorOptimizerConfig {
    /// Requested memory placement of the original vector storage, resolving the new `memory`
    /// parameter against the deprecated `on_disk` flag. `None` if neither is configured.
    pub fn memory_placement(&self) -> Option<Memory> {
        Memory::resolve(self.memory, self.on_disk.map(Memory::from_on_disk))
    }

    /// Config for a plain (appendable, unindexed) segment.
    pub fn plain(&self) -> VectorDataConfig {
        self.vector_data_config(
            Indexes::Plain {},
            QuantizationConfig::for_appendable_segment(self.quantization_config.as_ref()),
        )
    }

    /// Config for an indexed segment.
    pub fn indexed(&self) -> VectorDataConfig {
        self.vector_data_config(
            Indexes::Hnsw(self.hnsw_config),
            self.quantization_config.clone(),
        )
    }

    fn vector_data_config(
        &self,
        index: Indexes,
        quantization_config: Option<QuantizationConfig>,
    ) -> VectorDataConfig {
        let memory = self.memory_placement().unwrap_or(Memory::Cached);
        VectorDataConfig {
            size: self.size,
            distance: self.distance,
            index,
            storage_type: VectorStorageType::appendable_from_memory(memory),
            quantization_config,
            multivector_config: self.multivector_config,
            datatype: self.datatype,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SparseVectorOptimizerConfig {
    pub on_disk: Option<bool>,
    pub memory: Option<Memory>,
    pub full_scan_threshold: Option<usize>,
    pub index_datatype: Option<VectorStorageDatatype>,
    pub storage_type: SparseVectorStorageType,
    pub modifier: Option<Modifier>,
}

impl SparseVectorOptimizerConfig {
    /// Requested memory placement of the sparse index, resolving the new `memory` parameter
    /// against the deprecated `on_disk` flag. `None` if neither is configured.
    pub fn memory_placement(&self) -> Option<Memory> {
        Memory::resolve(self.memory, self.on_disk.map(Memory::from_on_disk_heap))
    }

    /// Config for a plain (appendable) segment.
    pub fn plain(&self) -> SparseVectorDataConfig {
        self.with_index_type(SparseIndexType::MutableRam)
    }

    pub fn with_index_type(&self, index_type: SparseIndexType) -> SparseVectorDataConfig {
        SparseVectorDataConfig {
            index: SparseIndexConfig {
                full_scan_threshold: self.full_scan_threshold,
                index_type,
                datatype: self.index_datatype,
                // Persist only the explicitly requested `memory` parameter: the structural
                // decision is carried by `index_type`, and only the cold/cached distinction
                // (reachable solely through the explicit parameter) needs the extra field.
                // Legacy-only configurations thus keep a byte-identical index config,
                // which older Qdrant versions can load without any unknown fields.
                memory: self.memory,
            },
            storage_type: self.storage_type,
            modifier: self.modifier,
        }
    }
}

/// Live read of the vector names currently present in the collection schema.
///
/// Unlike the rest of [`SegmentOptimizerConfig`], which is a frozen snapshot taken when the
/// optimizer was built, this reads the *current* schema each time it is called. Optimization needs
/// the live view to tell a vector name that was deleted from the collection (and should be pruned
/// when rebuilding old segments) from one that was just created but is not yet in this optimizer's
/// frozen target (the CreateVectorName race, which must cancel instead). Wrapped in a newtype so
/// `SegmentOptimizerConfig` can keep deriving `Debug`.
#[derive(Clone)]
pub struct LiveVectorNamesProvider(Arc<dyn Fn() -> HashSet<VectorNameBuf> + Send + Sync>);

impl LiveVectorNamesProvider {
    pub fn new(read: impl Fn() -> HashSet<VectorNameBuf> + Send + Sync + 'static) -> Self {
        Self(Arc::new(read))
    }

    pub fn get(&self) -> HashSet<VectorNameBuf> {
        self.0()
    }
}

impl fmt::Debug for LiveVectorNamesProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveVectorNamesProvider")
            .finish_non_exhaustive()
    }
}

/// This configuration contains all necessary information to build an optimized segment.
#[derive(Debug, Clone)]
pub struct SegmentOptimizerConfig {
    pub payload_storage_type: PayloadStorageType,
    pub dense_vectors: HashMap<VectorNameBuf, DenseVectorOptimizerConfig>,
    pub sparse_vectors: HashMap<VectorNameBuf, SparseVectorOptimizerConfig>,
    /// Live read of the collection's vector names, when wired in via
    /// [`SegmentOptimizerConfig::with_live_vector_names`]. `None` if no live source is available.
    pub live_vector_names: Option<LiveVectorNamesProvider>,
}

impl SegmentOptimizerConfig {
    pub fn plain_segment_config(&self) -> SegmentConfig {
        SegmentConfig {
            vector_data: self
                .dense_vectors
                .iter()
                .map(|(name, config)| (name.clone(), config.plain()))
                .collect(),
            sparse_vector_data: self
                .sparse_vectors
                .iter()
                .map(|(name, config)| (name.clone(), config.plain()))
                .collect(),
            payload_storage_type: self.payload_storage_type,
        }
    }

    /// Attach a live read of the collection's vector names (see [`LiveVectorNamesProvider`]).
    #[must_use]
    pub fn with_live_vector_names(mut self, provider: LiveVectorNamesProvider) -> Self {
        self.live_vector_names = Some(provider);
        self
    }

    /// The collection's current vector names, if a live source was wired in.
    pub fn live_vector_names(&self) -> Option<HashSet<VectorNameBuf>> {
        self.live_vector_names
            .as_ref()
            .map(LiveVectorNamesProvider::get)
    }
}

/// Target segment count for the merge optimizer.
pub fn default_segment_number() -> usize {
    // Configure 1 segment per 2 CPUs, as a middle ground between
    // latency and RPS.
    let expected_segments = common::cpu::get_num_cpus() / 2;
    // Do not configure less than 2 and more than 8 segments
    // until it is not explicitly requested
    expected_segments.clamp(2, 8)
}

// --- Shared optimizer threshold helpers (used by collection and edge) ---

/// Resolve number of segments: if `default_segment_number` is 0, use CPU-based default.
pub fn get_number_segments(requested_segment_number: usize) -> usize {
    if requested_segment_number == 0 {
        default_segment_number()
    } else {
        requested_segment_number
    }
}

/// Resolve indexing threshold in KB: `None` => default, `Some(0)` => disable (usize::MAX).
pub fn get_indexing_threshold_kb(indexing_threshold: Option<usize>) -> usize {
    match indexing_threshold {
        None => DEFAULT_INDEXING_THRESHOLD_KB,
        Some(0) => usize::MAX,
        Some(custom) => custom,
    }
}

/// Resolve max segment size in KB: custom value or per-thread default.
pub fn get_max_segment_size_kb(
    max_segment_size: Option<usize>,
    num_indexing_threads: usize,
) -> usize {
    if let Some(max) = max_segment_size {
        max
    } else {
        num_indexing_threads.saturating_mul(DEFAULT_MAX_SEGMENT_PER_CPU_KB)
    }
}

/// Build deferred points threshold in bytes when `prevent_unoptimized` is true.
///
/// The threshold is clamped to an explicitly configured `max_segment_size`, since a segment past
/// the threshold keeps deferring points until optimized and would otherwise be allowed to grow
/// beyond the size cap. Disabled indexing (`usize::MAX`) is left alone.
pub fn get_deferred_points_threshold_bytes(
    prevent_unoptimized: Option<bool>,
    indexing_threshold_kb: usize,
    max_segment_size_kb: Option<usize>,
) -> Option<NonZeroUsize> {
    if prevent_unoptimized != Some(true) {
        return None;
    }

    // A zero cap means uncapped, like everywhere else the cap is read.
    let mut threshold_kb = indexing_threshold_kb;
    if let Some(max_segment_size_kb) = max_segment_size_kb
        && max_segment_size_kb > 0
        && indexing_threshold_kb != usize::MAX
    {
        threshold_kb = threshold_kb.min(max_segment_size_kb);
    }

    NonZeroUsize::new(threshold_kb.saturating_mul(BYTES_IN_KB))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn deferred_threshold_is_clamped_to_max_segment_size() {
        let kb = |kb: usize| NonZeroUsize::new(kb * BYTES_IN_KB);
        let disabled_indexing = NonZeroUsize::new(usize::MAX.saturating_mul(BYTES_IN_KB));

        // (prevent_unoptimized, indexing_threshold_kb, max_segment_size_kb, expected)
        let cases = [
            (None, 10_000, Some(1_000), None),
            (Some(false), 10_000, Some(1_000), None),
            (Some(true), 10_000, Some(256_000), kb(10_000)),
            (Some(true), 100_000, Some(1_000), kb(1_000)),
            // `None` and zero cap both mean uncapped
            (Some(true), 100_000, None, kb(100_000)),
            (Some(true), 100_000, Some(0), kb(100_000)),
            // disabled indexing is not clamped
            (Some(true), usize::MAX, Some(1_000), disabled_indexing),
        ];

        for (prevent_unoptimized, indexing_threshold_kb, max_segment_size_kb, expected) in cases {
            assert_eq!(
                get_deferred_points_threshold_bytes(
                    prevent_unoptimized,
                    indexing_threshold_kb,
                    max_segment_size_kb,
                ),
                expected,
                "{prevent_unoptimized:?} / {indexing_threshold_kb} / {max_segment_size_kb:?}",
            );
        }
    }

    #[test]
    fn live_vector_names_provider_reads_current_state() {
        // The provider must re-read the live source on every call rather than snapshot it once,
        // otherwise a vector deleted (or created) after optimizer construction would be missed and
        // the merge would make the wrong cancel/prune decision.
        let source = Arc::new(Mutex::new(HashSet::from(["a".to_owned(), "b".to_owned()])));
        let provider = {
            let source = source.clone();
            LiveVectorNamesProvider::new(move || source.lock().unwrap().clone())
        };

        assert_eq!(
            provider.get(),
            HashSet::from(["a".to_owned(), "b".to_owned()])
        );

        // Delete "b" from the live source: the provider must reflect it on the next read.
        source.lock().unwrap().remove("b");
        assert_eq!(provider.get(), HashSet::from(["a".to_owned()]));
    }
}
