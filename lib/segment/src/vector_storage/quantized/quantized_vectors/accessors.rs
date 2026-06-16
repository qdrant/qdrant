use std::alloc::Layout;
use std::borrow::Cow;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use quantization::EncodedVectors;

use super::config::QUANTIZED_CONFIG_PATH;
use super::{QuantizedVectorStorage, QuantizedVectors};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorRef;

impl QuantizedVectors {
    pub fn default_rescoring(&self) -> bool {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => false,
            QuantizedVectorStorage::ScalarMmap(_) => false,
            QuantizedVectorStorage::ScalarChunkedMmap(_) => false,
            QuantizedVectorStorage::PQRam(_) => false,
            QuantizedVectorStorage::PQMmap(_) => false,
            QuantizedVectorStorage::PQChunkedMmap(_) => false,
            QuantizedVectorStorage::BinaryRam(_) => true,
            QuantizedVectorStorage::BinaryMmap(_) => true,
            QuantizedVectorStorage::BinaryChunkedMmap(_) => true,
            QuantizedVectorStorage::TQRam(q) => {
                Self::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            QuantizedVectorStorage::TQMmap(q) => {
                Self::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            QuantizedVectorStorage::TQChunkedMmap(q) => {
                Self::tq_bits_default_rescoring(q.get_metadata().bits)
            }
            QuantizedVectorStorage::ScalarRamMulti(_) => false,
            QuantizedVectorStorage::ScalarMmapMulti(_) => false,
            QuantizedVectorStorage::ScalarChunkedMmapMulti(_) => false,
            QuantizedVectorStorage::PQRamMulti(_) => false,
            QuantizedVectorStorage::PQMmapMulti(_) => false,
            QuantizedVectorStorage::PQChunkedMmapMulti(_) => false,
            QuantizedVectorStorage::BinaryRamMulti(_) => true,
            QuantizedVectorStorage::BinaryMmapMulti(_) => true,
            QuantizedVectorStorage::BinaryChunkedMmapMulti(_) => true,
            QuantizedVectorStorage::TQRamMulti(q) => {
                Self::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
            QuantizedVectorStorage::TQMmapMulti(q) => {
                Self::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => {
                Self::tq_bits_default_rescoring(q.storage().get_metadata().bits)
            }
        }
    }

    /// Get layout for a single quantized vector.
    ///
    /// I.e. the size of a single vector in bytes, and the required alignment.
    pub fn get_quantized_vector_layout(&self) -> OperationResult<Layout> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::PQChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::TQRam(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::TQMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::TQChunkedMmap(storage) => Ok(storage.layout()),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::PQChunkedMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_)
            | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
            | QuantizedVectorStorage::TQRamMulti(_)
            | QuantizedVectorStorage::TQMmapMulti(_)
            | QuantizedVectorStorage::TQChunkedMmapMulti(_) => Err(OperationError::service_error(
                "Cannot get quantized vector layout from multivector storage",
            )),
        }
    }

    pub fn get_quantized_vector(&self, id: PointOffsetType) -> Cow<'_, [u8]> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::PQChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::TQRam(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::TQMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::TQChunkedMmap(storage) => storage.get_quantized_vector(id),
            QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::PQChunkedMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_)
            | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
            | QuantizedVectorStorage::TQRamMulti(_)
            | QuantizedVectorStorage::TQMmapMulti(_)
            | QuantizedVectorStorage::TQChunkedMmapMulti(_) => {
                panic!("Cannot get quantized vector from multivector storage");
            }
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.files(),
            QuantizedVectorStorage::ScalarMmap(q) => q.files(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::PQRam(q) => q.files(),
            QuantizedVectorStorage::PQMmap(q) => q.files(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::BinaryRam(q) => q.files(),
            QuantizedVectorStorage::BinaryMmap(q) => q.files(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::TQRam(q) => q.files(),
            QuantizedVectorStorage::TQMmap(q) => q.files(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.files(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.files(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.files(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.files(),
            QuantizedVectorStorage::PQRamMulti(q) => q.files(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.files(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.files(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.files(),
            QuantizedVectorStorage::TQRamMulti(q) => q.files(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.files(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.files(),
        };
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::PQRam(q) => q.immutable_files(),
            QuantizedVectorStorage::PQMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryRam(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::TQRam(q) => q.immutable_files(),
            QuantizedVectorStorage::TQMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::TQRamMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.immutable_files(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.immutable_files(),
        };
        files.push(self.path.join(QUANTIZED_CONFIG_PATH));
        files
    }

    pub fn populate(&self) -> OperationResult<()> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => {} // not mmap
            QuantizedVectorStorage::ScalarMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::PQRam(_) => {}
            QuantizedVectorStorage::PQMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::PQChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::BinaryRam(_) => {}
            QuantizedVectorStorage::BinaryMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::TQRam(_) => {}
            QuantizedVectorStorage::TQMmap(storage) => storage.storage().populate(),
            QuantizedVectorStorage::TQChunkedMmap(storage) => storage.storage().populate()?,
            QuantizedVectorStorage::ScalarRamMulti(_) => {}
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::PQRamMulti(_) => {}
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::BinaryRamMulti(_) => {}
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::TQRamMulti(_) => {}
            QuantizedVectorStorage::TQMmapMulti(storage) => {
                storage.storage().storage().populate();
                storage.offsets_storage().populate()?;
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(storage) => {
                storage.storage().storage().populate()?;
                storage.offsets_storage().populate()?;
            }
        }
        Ok(())
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(_) => {} // not mmap
            QuantizedVectorStorage::ScalarMmap(storage) => storage.storage().clear_cache(),
            QuantizedVectorStorage::ScalarChunkedMmap(storage) => {
                storage.storage().clear_cache()?
            }
            QuantizedVectorStorage::PQRam(_) => {}
            QuantizedVectorStorage::PQMmap(storage) => storage.storage().clear_cache(),
            QuantizedVectorStorage::PQChunkedMmap(storage) => storage.storage().clear_cache()?,
            QuantizedVectorStorage::BinaryRam(_) => {}
            QuantizedVectorStorage::BinaryMmap(storage) => storage.storage().clear_cache(),
            QuantizedVectorStorage::BinaryChunkedMmap(storage) => {
                storage.storage().clear_cache()?
            }
            QuantizedVectorStorage::TQRam(_) => {}
            QuantizedVectorStorage::TQMmap(storage) => storage.storage().clear_cache(),
            QuantizedVectorStorage::TQChunkedMmap(storage) => storage.storage().clear_cache()?,
            QuantizedVectorStorage::ScalarRamMulti(_) => {}
            QuantizedVectorStorage::ScalarMmapMulti(storage) => {
                storage.storage().storage().clear_cache();
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(storage) => {
                storage.storage().storage().clear_cache()?;
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::PQRamMulti(_) => {}
            QuantizedVectorStorage::PQMmapMulti(storage) => {
                storage.storage().storage().clear_cache();
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(storage) => {
                storage.storage().storage().clear_cache()?;
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::BinaryRamMulti(_) => {}
            QuantizedVectorStorage::BinaryMmapMulti(storage) => {
                storage.storage().storage().clear_cache();
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(storage) => {
                storage.storage().storage().clear_cache()?;
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::TQRamMulti(_) => {}
            QuantizedVectorStorage::TQMmapMulti(storage) => {
                storage.storage().storage().clear_cache();
                storage.offsets_storage().clear_cache()?;
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(storage) => {
                storage.storage().storage().clear_cache()?;
                storage.offsets_storage().clear_cache()?;
            }
        }
        Ok(())
    }

    pub fn flusher(&self) -> Flusher {
        let flusher = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => q.flusher(),
            QuantizedVectorStorage::ScalarMmap(q) => q.flusher(),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::PQRam(q) => q.flusher(),
            QuantizedVectorStorage::PQMmap(q) => q.flusher(),
            QuantizedVectorStorage::PQChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRam(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmap(q) => q.flusher(),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::TQRam(q) => q.flusher(),
            QuantizedVectorStorage::TQMmap(q) => q.flusher(),
            QuantizedVectorStorage::TQChunkedMmap(q) => q.flusher(),
            QuantizedVectorStorage::ScalarRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::ScalarMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::TQRamMulti(q) => q.flusher(),
            QuantizedVectorStorage::TQMmapMulti(q) => q.flusher(),
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => q.flusher(),
        };
        Box::new(move || flusher().map_err(OperationError::from))
    }

    pub fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQRam(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQChunkedMmap(q) => {
                Self::upsert_vector_dense(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::ScalarChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::PQChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::BinaryChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQRamMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
            QuantizedVectorStorage::TQChunkedMmapMulti(q) => {
                Self::upsert_vector_multi(q, id, vector, hw_counter)
            }
        }
    }

    fn upsert_vector_dense(
        quantization_storage: &mut impl quantization::EncodedVectors,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let VectorRef::Dense(vector) = vector {
            Ok(quantization_storage.upsert_vector(id, vector, hw_counter)?)
        } else {
            Err(OperationError::WrongMulti)
        }
    }

    fn upsert_vector_multi(
        quantization_storage: &mut impl quantization::EncodedVectors,
        id: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if let VectorRef::MultiDense(vector) = vector {
            Ok(quantization_storage.upsert_vector(id, vector.flattened_vectors, hw_counter)?)
        } else {
            Err(OperationError::WrongMulti)
        }
    }
}
