//! Logical data written by an update, with indexing bytes accounted separately.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::ops::AddAssign;

use serde::Serialize;

use super::fully_qualified_point::FullyQualifiedPoint;
use super::vectors::VectorRef;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::json_path::JsonPath;
use crate::types::{
    Indexes, PayloadContainer, SegmentConfig, VectorDataConfig, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::sparse::StoredSparseVector;
use crate::vector_storage::turbo::shared;

/// Bytes of materialized points stored by a successful batch; deletes and skips add zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpdateBytes {
    /// Full UTF-8 JSON payload and original vectors, excluding storage framing and indexes.
    pub updated_bytes: u64,
    /// Indexed payload values and indexed vectors, using their quantized representation when configured.
    pub indexed_bytes: u64,
}

impl AddAssign for UpdateBytes {
    fn add_assign(&mut self, other: Self) {
        self.updated_bytes = self.updated_bytes.saturating_add(other.updated_bytes);
        self.indexed_bytes = self.indexed_bytes.saturating_add(other.indexed_bytes);
    }
}

struct DenseMeter {
    stored_row_bytes: usize,
    indexed_row_bytes: usize,
}

/// Metering schema for the intended indexed configuration, independent of segment optimization.
pub struct UpdateMetering {
    dense: HashMap<String, DenseMeter>,
    sparse: HashMap<String, usize>,
    payload_fields: HashSet<JsonPath>,
}

impl UpdateMetering {
    pub fn new(config: &SegmentConfig, payload_fields: impl IntoIterator<Item = JsonPath>) -> Self {
        let dense = config
            .vector_data
            .iter()
            .map(|(name, config)| {
                let stored_row_bytes = dense_row_bytes(config);
                let indexed = match config.index {
                    Indexes::Plain {} => false,
                    Indexes::Hnsw(hnsw) => hnsw.m > 0 || hnsw.payload_m.unwrap_or(hnsw.m) > 0,
                };
                let indexed_row_bytes = if !indexed {
                    0
                } else if let Some(quantization) = &config.quantization_config {
                    let parameters = QuantizedVectors::construct_vector_parameters(
                        quantization,
                        config.distance,
                        config.size,
                        0,
                        QuantizedVectorsStorageType::Mutable,
                    );
                    QuantizedVectors::quantized_vector_size(
                        quantization,
                        &parameters,
                        config.multivector_config.is_some(),
                    )
                } else {
                    stored_row_bytes
                };
                (
                    name.clone(),
                    DenseMeter {
                        stored_row_bytes,
                        indexed_row_bytes,
                    },
                )
            })
            .collect();
        let sparse = config
            .sparse_vector_data
            .iter()
            .map(|(name, config)| {
                let value_bytes = match config.index.datatype.unwrap_or_default() {
                    VectorStorageDatatype::Float16 => 2,
                    VectorStorageDatatype::Uint8 => 1,
                    VectorStorageDatatype::Float32
                    | VectorStorageDatatype::Turbo4
                    | VectorStorageDatatype::Turbo8
                    | VectorStorageDatatype::Turbo16 => 4,
                };
                (name.clone(), size_of::<u32>() + value_bytes)
            })
            .collect();
        Self {
            dense,
            sparse,
            payload_fields: payload_fields.into_iter().collect(),
        }
    }

    pub fn measure(&self, point: &FullyQualifiedPoint) -> OperationResult<UpdateBytes> {
        let mut bytes = UpdateBytes {
            updated_bytes: json_bytes(&point.payload)?,
            indexed_bytes: 0,
        };
        for field in &self.payload_fields {
            for value in point.payload.get_value(field) {
                bytes.indexed_bytes = bytes.indexed_bytes.saturating_add(json_bytes(value)?);
            }
        }
        for (name, vector) in point.updated_vectors.iter() {
            let (rows, sparse_len) = match vector {
                VectorRef::Dense(_) => (1, None),
                VectorRef::MultiDense(multi) => (multi.vectors_count(), None),
                VectorRef::Sparse(sparse) => (0, Some(sparse.indices.len())),
            };
            bytes += self.vector_bytes(name, rows, sparse_len)?;
        }
        for (name, raw) in &point.stored_vectors {
            if point.updated_vectors.contains_key(name) {
                continue;
            }
            let (rows, sparse_len) = if let Some(dense) = self.dense.get(name) {
                if dense.stored_row_bytes == 0 || !raw.len().is_multiple_of(dense.stored_row_bytes)
                {
                    return Err(OperationError::service_error(format!(
                        "Invalid stored vector size for {name:?}"
                    )));
                }
                (raw.len() / dense.stored_row_bytes, None)
            } else {
                (
                    0,
                    Some(
                        StoredSparseVector::decode_untrusted_bytes(raw)?
                            .indices
                            .len(),
                    ),
                )
            };
            bytes += self.vector_bytes(name, rows, sparse_len)?;
        }
        Ok(bytes)
    }

    fn vector_bytes(
        &self,
        name: &str,
        rows: usize,
        sparse_len: Option<usize>,
    ) -> OperationResult<UpdateBytes> {
        if let Some(len) = sparse_len {
            let index_width = self.sparse.get(name).ok_or_else(|| unknown_vector(name))?;
            Ok(UpdateBytes {
                updated_bytes: len as u64 * 8,
                indexed_bytes: len as u64 * *index_width as u64,
            })
        } else {
            let dense = self.dense.get(name).ok_or_else(|| unknown_vector(name))?;
            Ok(UpdateBytes {
                updated_bytes: rows as u64 * dense.stored_row_bytes as u64,
                indexed_bytes: rows as u64 * dense.indexed_row_bytes as u64,
            })
        }
    }
}

fn unknown_vector(name: &str) -> OperationError {
    OperationError::service_error(format!(
        "Missing update metering schema for vector {name:?}"
    ))
}

fn dense_row_bytes(config: &VectorDataConfig) -> usize {
    let datatype = config.datatype.unwrap_or_default();
    match datatype {
        VectorStorageDatatype::Float32 => config.size * 4,
        VectorStorageDatatype::Float16 => config.size * 2,
        VectorStorageDatatype::Uint8 => config.size,
        VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => {
            shared::quantized_vector_size(config.size, config.distance, shared::tq_bits(datatype))
        }
    }
}

fn json_bytes(value: &impl Serialize) -> OperationResult<u64> {
    #[derive(Default)]
    struct Size(u64);
    impl Write for Size {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut size = Size::default();
    serde_json::to_writer(&mut size, value)
        .map_err(|err| OperationError::service_error(format!("Measure payload: {err}")))?;
    Ok(size.0)
}
