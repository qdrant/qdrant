use std::cmp::Ordering;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use ordered_float::OrderedFloat;

use super::memmap_vector_storage::MemmapVectorStorage;
use super::quantized::quantized_vectors::QuantizedVectors;
use super::simple_vector_storage::SimpleVectorStorage;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{Distance, PointOffsetType, QuantizationConfig, ScoreType};
use crate::vector_storage::appendable_mmap_vector_storage::AppendableMmapVectorStorage;

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

/// Trait for vector storage
/// El - type of vector element, expected numerical type
/// Storage operates with internal IDs (`PointOffsetType`), which always starts with zero and have no skips
pub trait VectorStorage {
    fn dim(&self) -> usize;

    fn preprocessed_dim(&self) -> usize;

    fn distance(&self) -> Distance;

    /// Number of vectors
    ///
    /// - includes soft deleted vectors, as they are still stored
    fn total_vector_count(&self) -> usize;

    /// Get the number of available vectors, considering deleted points and vectors
    ///
    /// This uses [`VectorStorage::total_vector_count`] and [`VectorStorage::deleted_vector_count`] internally.
    ///
    /// # Warning
    ///
    /// This number may not always be accurate. See warning in [`VectorStorage::deleted_vector_count`] documentation.
    fn available_vector_count(&self) -> usize {
        self.total_vector_count()
            .saturating_sub(self.deleted_vector_count())
    }

    /// Number of all stored vectors including deleted
    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType];

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()>;

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>>;

    fn flusher(&self) -> Flusher;

    // Generate quantized vectors and store them on disk
    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<()>;

    // Load quantized vectors from disk
    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()>;

    fn quantized_storage(&self) -> Option<&QuantizedVectors>;

    fn files(&self) -> Vec<PathBuf>;

    /// Flag the vector by the given key as deleted
    ///
    /// Returns true if the vector was not deleted before and is now deleted
    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool>;

    /// Check whether the vector at the given key is flagged as deleted
    fn is_deleted_vector(&self, key: PointOffsetType) -> bool;

    /// Get the number of deleted vectors, considering deleted points and vectors
    ///
    /// Vectors may be deleted at two levels, as point or as vector. Deleted points should
    /// propagate to deleting the vectors. That means that the deleted vector count includes the
    /// number of deleted points as well.
    ///
    /// This includes any vectors that were deleted at creation.
    ///
    /// # Warning
    ///
    /// In some very exceptional cases it is possible for this count not to include some deleted
    /// points. That may happen when flushing a segment to disk fails. This should be recovered
    /// when loading/recovering the segment, but that isn't guaranteed. You should therefore use
    /// the deleted count with care.
    fn deleted_vector_count(&self) -> usize;

    /// Get [`BitSlice`] representation for deleted vectors with deletion flags
    ///
    /// The size of this slice is not guaranteed. It may be smaller/larger than the number of
    /// vectors in this segment.
    fn deleted_vector_bitslice(&self) -> &BitSlice;

    /// Whether this vector storage type support appending.
    fn is_appendable(&self) -> bool;
}

pub enum VectorStorageEnum {
    Simple(SimpleVectorStorage),
    Memmap(Box<MemmapVectorStorage>),
    AppendableMemmap(Box<AppendableMmapVectorStorage>),
}

impl VectorStorage for VectorStorageEnum {
    fn dim(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.dim(),
            VectorStorageEnum::Memmap(v) => v.dim(),
            VectorStorageEnum::AppendableMemmap(v) => v.dim(),
        }
    }

    fn preprocessed_dim(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.preprocessed_dim(),
            VectorStorageEnum::Memmap(v) => v.preprocessed_dim(),
            VectorStorageEnum::AppendableMemmap(v) => v.preprocessed_dim(),
        }
    }

    fn distance(&self) -> Distance {
        match self {
            VectorStorageEnum::Simple(v) => v.distance(),
            VectorStorageEnum::Memmap(v) => v.distance(),
            VectorStorageEnum::AppendableMemmap(v) => v.distance(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.total_vector_count(),
            VectorStorageEnum::Memmap(v) => v.total_vector_count(),
            VectorStorageEnum::AppendableMemmap(v) => v.total_vector_count(),
        }
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        match self {
            VectorStorageEnum::Simple(v) => v.get_vector(key),
            VectorStorageEnum::Memmap(v) => v.get_vector(key),
            VectorStorageEnum::AppendableMemmap(v) => v.get_vector(key),
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.insert_vector(key, vector),
            VectorStorageEnum::Memmap(v) => v.insert_vector(key, vector),
            VectorStorageEnum::AppendableMemmap(v) => v.insert_vector(key, vector),
        }
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        match self {
            VectorStorageEnum::Simple(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::Memmap(v) => v.update_from(other, other_ids, stopped),
            VectorStorageEnum::AppendableMemmap(v) => v.update_from(other, other_ids, stopped),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            VectorStorageEnum::Simple(v) => v.flusher(),
            VectorStorageEnum::Memmap(v) => v.flusher(),
            VectorStorageEnum::AppendableMemmap(v) => v.flusher(),
        }
    }

    fn quantize(
        &mut self,
        data_path: &Path,
        quantization_config: &QuantizationConfig,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => {
                v.quantize(data_path, quantization_config, max_threads, stopped)
            }
            VectorStorageEnum::Memmap(v) => {
                v.quantize(data_path, quantization_config, max_threads, stopped)
            }
            VectorStorageEnum::AppendableMemmap(v) => {
                v.quantize(data_path, quantization_config, max_threads, stopped)
            }
        }
    }

    fn load_quantization(&mut self, data_path: &Path) -> OperationResult<()> {
        match self {
            VectorStorageEnum::Simple(v) => v.load_quantization(data_path),
            VectorStorageEnum::Memmap(v) => v.load_quantization(data_path),
            VectorStorageEnum::AppendableMemmap(v) => v.load_quantization(data_path),
        }
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectors> {
        match self {
            VectorStorageEnum::Simple(v) => v.quantized_storage(),
            VectorStorageEnum::Memmap(v) => v.quantized_storage(),
            VectorStorageEnum::AppendableMemmap(v) => v.quantized_storage(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            VectorStorageEnum::Simple(v) => v.files(),
            VectorStorageEnum::Memmap(v) => v.files(),
            VectorStorageEnum::AppendableMemmap(v) => v.files(),
        }
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        match self {
            VectorStorageEnum::Simple(v) => v.delete_vector(key),
            VectorStorageEnum::Memmap(v) => v.delete_vector(key),
            VectorStorageEnum::AppendableMemmap(v) => v.delete_vector(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageEnum::Simple(v) => v.is_deleted_vector(key),
            VectorStorageEnum::Memmap(v) => v.is_deleted_vector(key),
            VectorStorageEnum::AppendableMemmap(v) => v.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageEnum::Simple(v) => v.deleted_vector_count(),
            VectorStorageEnum::Memmap(v) => v.deleted_vector_count(),
            VectorStorageEnum::AppendableMemmap(v) => v.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageEnum::Simple(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::Memmap(v) => v.deleted_vector_bitslice(),
            VectorStorageEnum::AppendableMemmap(v) => v.deleted_vector_bitslice(),
        }
    }

    fn is_appendable(&self) -> bool {
        match self {
            VectorStorageEnum::Simple(v) => v.is_appendable(),
            VectorStorageEnum::Memmap(v) => v.is_appendable(),
            VectorStorageEnum::AppendableMemmap(v) => v.is_appendable(),
        }
    }
}
