use std::fs::create_dir_all;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::BitSlice;

use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationResult};
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::dynamic_mmap_flags::DynamicMmapFlags;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

const VECTORS_DIR_PATH: &str = "vectors";
const DELETED_DIR_PATH: &str = "deleted";

pub struct AppendableMmapVectorStorage {
    dim: usize,
    preprocessed_dim: usize,
    vectors: ChunkedMmapVectors,
    deleted: DynamicMmapFlags,
    distance: Distance,
    deleted_count: usize,
    quantized_vectors: Option<QuantizedVectors>,
}

pub fn open_appendable_memmap_vector_storage(
    path: &Path,
    dim: usize,
    preprocessed_dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors: ChunkedMmapVectors = ChunkedMmapVectors::open(&vectors_path, dim)?;

    let num_vectors = vectors.len();

    let deleted: DynamicMmapFlags = DynamicMmapFlags::open(&deleted_path)?;

    let mut deleted_count = 0;

    for i in 0..num_vectors {
        if deleted.get(i) {
            deleted_count += 1;
        }
    }

    let storage = AppendableMmapVectorStorage {
        dim,
        preprocessed_dim,
        vectors,
        deleted,
        distance,
        deleted_count,
        quantized_vectors: None,
    };

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::AppendableMemmap(Box::new(storage)),
    )))
}

impl AppendableMmapVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> OperationResult<bool> {
        if self.vectors.len() <= key as usize {
            return Ok(false);
        }

        if self.deleted.len() <= key as usize {
            self.deleted.set_len(key as usize + 1)?;
        }
        let previous = self.deleted.set(key, deleted);
        if !previous && deleted {
            self.deleted_count += 1;
        } else if previous && !deleted {
            self.deleted_count -= 1;
        }
        Ok(previous)
    }
}

impl VectorStorage for AppendableMmapVectorStorage {
    fn dim(&self) -> usize {
        self.dim
    }

    fn preprocessed_dim(&self) -> usize {
        self.preprocessed_dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.vectors.get(key)
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()> {
        self.vectors.insert(key, vector)?;
        self.set_deleted(key, false)?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_deleted = other.is_deleted_vector(point_id);
            let other_vector = other.get_vector(point_id);
            let new_id = self.vectors.push(other_vector)?;
            self.set_deleted(new_id, other_deleted)?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        Box::new({
            let vectors_flusher = self.vectors.flusher();
            let deleted_flusher = self.deleted.flusher();
            move || {
                vectors_flusher()?;
                deleted_flusher()?;
                Ok(())
            }
        })
    }

    fn quantize(
        &mut self,
        path: &Path,
        quantization_config: &QuantizationConfig,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<()> {
        let vector_data_iterator = (0..self.vectors.len() as u32).map(|i| self.vectors.get(i));
        self.quantized_vectors = Some(QuantizedVectors::create(
            vector_data_iterator,
            quantization_config,
            self.distance,
            self.vectors.dim(),
            self.vectors.len(),
            path,
            true,
            max_threads,
            stopped,
        )?);
        Ok(())
    }

    fn load_quantization(&mut self, path: &Path) -> OperationResult<()> {
        if QuantizedVectors::config_exists(path) {
            self.quantized_vectors = Some(QuantizedVectors::load(path, true, self.distance)?);
        }
        Ok(())
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectors> {
        self.quantized_vectors.as_ref()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.files();
        files.extend(self.deleted.files());
        if let Some(quantized_vectors) = &self.quantized_vectors {
            files.extend(quantized_vectors.files())
        }
        files
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        self.set_deleted(key, true)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }

    fn is_appendable(&self) -> bool {
        true
    }
}
