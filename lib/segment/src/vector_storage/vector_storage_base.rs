use std::cmp::Ordering;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use ordered_float::OrderedFloat;
use rand::Rng;

use super::memmap_vector_storage::MemmapVectorStorage;
use super::simple_vector_storage::SimpleVectorStorage;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{PointOffsetType, QuantizationConfig, ScoreType};

#[derive(Copy, Clone, PartialEq, Debug, Default)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType,
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Optimized scorer for multiple scoring requests comparing with a single query
/// Holds current query and params, receives only subset of points to score
pub trait RawScorer {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize;

    /// Return true if point satisfies current search context (exists and not deleted)
    fn check_point(&self, point: PointOffsetType) -> bool;
    /// Score stored vector with vector under the given index
    fn score_point(&self, point: PointOffsetType) -> ScoreType;

    /// Return distance between stored points selected by ids
    /// Panics if any id is out of range
    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}

pub trait ScorerBuilder {
    /// Generate a `RawScorer` object which contains all required context for searching similar vector
    fn raw_scorer(&self, vector: Vec<VectorElementType>) -> Box<dyn RawScorer + '_>;

    // Generate RawScorer on quantized vectors if present
    fn quantized_raw_scorer(&self, vector: &[VectorElementType])
        -> Option<Box<dyn RawScorer + '_>>;

    // Try peek top nearest points from quantized vectors. If quantized vectors are not present, do it on raw vectors
    fn score_quantized_points(
        &self,
        vector: &[VectorElementType],
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn score_points(
        &self,
        vector: &[VectorElementType],
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn score_all(&self, vector: &[VectorElementType], top: usize) -> Vec<ScoredPointOffset>;
}

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn vector_dim(&self) -> usize;
    fn vector_count(&self) -> usize;
    /// Number of searchable vectors (not deleted)
    fn deleted_count(&self) -> usize;
    /// Number of vectors, marked as deleted but still stored
    fn total_vector_count(&self) -> usize;
    /// Number of all stored vectors including deleted
    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>>;
    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType>;
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<()>;
    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;
    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()>;
    fn is_deleted(&self, key: PointOffsetType) -> bool;
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;
    fn flusher(&self) -> Flusher;

    // Generate quantized vectors and store them on disk
    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()>;

    // Load quantized vectors from disk
    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()>;

    fn files(&self) -> Vec<PathBuf>;

    /// Iterator over `n` random ids which are not deleted
    fn sample_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let total = self.total_vector_count() as PointOffsetType;
        let mut rng = rand::thread_rng();
        Box::new(
            (0..total)
                .map(move |_| rng.gen_range(0..total))
                .filter(move |x| !self.is_deleted(*x)),
        )
    }

    fn scorer_builder(&self) -> Box<dyn ScorerBuilder + Sync + Send + '_>;
}

pub enum VectorStorageEnum {
    Simple(SimpleVectorStorage),
    Memmap(Box<MemmapVectorStorage>),
}

impl VectorStorage for VectorStorageEnum {
    fn vector_dim(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.vector_dim(),
            VectorStorageEnum::Memmap(v) => v.vector_dim(),
        }
    }

    fn vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.vector_count(),
            VectorStorageEnum::Memmap(v) => v.vector_count(),
        }
    }

    fn deleted_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.deleted_count(),
            VectorStorageEnum::Memmap(v) => v.deleted_count(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.total_vector_count(),
            VectorStorageEnum::Memmap(v) => v.total_vector_count(),
        }
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        match self {
            VectorStorageEnum::Simple(v) => v.get_vector(key),
            VectorStorageEnum::Memmap(v) => v.get_vector(key),
        }
    }

    fn put_vector(&mut self, vector: Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        match self {
            VectorStorageEnum::Simple(v) => v.put_vector(vector),
            VectorStorageEnum::Memmap(v) => v.put_vector(vector),
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: Vec<VectorElementType>,
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::Memmap(v) => v.insert_vector(key, vector),
        }
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::Simple(v) => v.update_from(other, stopped),
            VectorStorageEnum::Memmap(v) => v.update_from(other, stopped),
        }
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.delete(key),
            VectorStorageEnum::Memmap(v) => v.delete(key),
        }
    }

    fn is_deleted(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::Simple(v) => v.is_deleted(key),
            VectorStorageEnum::Memmap(v) => v.is_deleted(key),
        }
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            VectorStorageEnum::Simple(v) => v.iter_ids(),
            VectorStorageEnum::Memmap(v) => v.iter_ids(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::Simple(v) => v.flusher(),
            VectorStorageEnum::Memmap(v) => v.flusher(),
        }
    }

    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.quantize(data_path, quantization_config),
            VectorStorageEnum::Memmap(v) => v.quantize(data_path, quantization_config),
        }
    }

    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.load_quantization(data_path),
            VectorStorageEnum::Memmap(v) => v.load_quantization(data_path),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::Simple(v) => v.files(),
            VectorStorageEnum::Memmap(v) => v.files(),
        }
    }

    fn scorer_builder(&self) -> Box<dyn ScorerBuilder + Sync + Send + '_> {
        match self {
            VectorStorageEnum::Simple(v) => v.scorer_builder(),
            VectorStorageEnum::Memmap(v) => v.scorer_builder(),
        }
    }
}
