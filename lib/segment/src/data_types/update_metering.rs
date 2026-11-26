use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::ops::AddAssign;

use serde::Serialize;
use sparse::common::sparse_vector::SparseVector;

use super::fully_qualified_point::FullyQualifiedPoint;
use super::vectors::VectorRef;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::json_path::JsonPath;
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, CompressionRatio,
    HnswConfig, Indexes, PayloadContainer, ProductQuantization, ProductQuantizationConfig,
    QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig, ScalarType, SegmentConfig,
    SparseVectorDataConfig, TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
    VectorDataConfig, VectorStorageDatatype,
};
use crate::vector_storage::sparse::StoredSparseVector;
use crate::vector_storage::turbo::shared;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpdateBytes {
    /// Payload JSON and stored vectors, excluding storage overhead.
    pub updated_bytes: u64,
    /// Indexed payload values and vectors, quantized when configured.
    pub indexed_bytes: u64,
}

impl AddAssign for UpdateBytes {
    fn add_assign(&mut self, other: Self) {
        let Self {
            updated_bytes,
            indexed_bytes,
        } = self;
        let Self {
            updated_bytes: other_updated_bytes,
            indexed_bytes: other_indexed_bytes,
        } = other;
        *updated_bytes = updated_bytes.saturating_add(other_updated_bytes);
        *indexed_bytes = indexed_bytes.saturating_add(other_indexed_bytes);
    }
}

struct DenseMeter {
    /// Width of one row in storage's own encoding, used only to split
    /// carried-over bytes into rows.
    raw_bytes_per_row: usize,
    storage_bytes_per_row: usize,
    index_bytes_per_row: usize,
}

struct SparseMeter {
    storage_bytes_per_element: usize,
    index_bytes_per_element: usize,
}

pub struct UpdateMetering {
    dense: HashMap<String, DenseMeter>,
    sparse: HashMap<String, SparseMeter>,
    payload_fields: HashSet<JsonPath>,
}

impl UpdateMetering {
    pub fn new(config: &SegmentConfig, payload_fields: impl IntoIterator<Item = JsonPath>) -> Self {
        let SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type: _,
            id_tracker_memory: _,
        } = config;
        let dense = vector_data
            .iter()
            .map(|(name, config)| (name.clone(), DenseMeter::new(config)))
            .collect();
        let sparse = sparse_vector_data
            .iter()
            .map(|(name, config)| (name.clone(), SparseMeter::new(config)))
            .collect();
        Self {
            dense,
            sparse,
            payload_fields: payload_fields.into_iter().collect(),
        }
    }

    pub fn measure(&self, point: &FullyQualifiedPoint) -> OperationResult<UpdateBytes> {
        let FullyQualifiedPoint {
            id: _,
            version: _,
            stored_vectors,
            updated_vectors,
            payload,
        } = point;
        let Self {
            dense,
            sparse,
            payload_fields,
        } = self;
        let mut bytes = UpdateBytes {
            updated_bytes: json_bytes(payload)?,
            indexed_bytes: 0,
        };
        for field in payload_fields {
            for value in payload.get_value(field) {
                // Field indexes store an array's elements, not the array.
                let elements = value
                    .as_array()
                    .map_or(std::slice::from_ref(value), Vec::as_slice);
                for element in elements {
                    bytes.indexed_bytes = bytes.indexed_bytes.saturating_add(json_bytes(element)?);
                }
            }
        }
        for (name, vector) in updated_vectors.iter() {
            bytes += match vector {
                VectorRef::Dense(_) => meter(dense, name)?.bytes(1),
                VectorRef::MultiDense(multi) => meter(dense, name)?.bytes(multi.vectors_count()),
                VectorRef::Sparse(SparseVector { indices, values: _ }) => {
                    meter(sparse, name)?.bytes(indices.len())
                }
            };
        }
        for (name, raw) in stored_vectors {
            if updated_vectors.contains_key(name) {
                continue;
            }
            bytes += if let Some(dense_meter) = dense.get(name) {
                dense_meter.bytes(dense_meter.count_rows(name, raw)?)
            } else if let Some(sparse_meter) = sparse.get(name) {
                let SparseVector { indices, values: _ } =
                    StoredSparseVector::decode_untrusted_bytes(raw)?;
                sparse_meter.bytes(indices.len())
            } else {
                return Err(unknown_vector(name));
            };
        }
        Ok(bytes)
    }
}

fn meter<'a, M>(meters: &'a HashMap<String, M>, name: &str) -> OperationResult<&'a M> {
    meters.get(name).ok_or_else(|| unknown_vector(name))
}

fn unknown_vector(name: &str) -> OperationError {
    OperationError::service_error(format!(
        "Missing update metering schema for vector {name:?}"
    ))
}

impl DenseMeter {
    fn new(config: &VectorDataConfig) -> Self {
        let VectorDataConfig {
            size,
            distance,
            storage_type: _,
            index,
            quantization_config,
            multivector_config: _,
            datatype,
        } = config;
        let datatype = datatype.unwrap_or_default();
        let storage_bytes_per_row = storage_bits_per_row(datatype, *size).div_ceil(BYTE_BITS);
        let raw_bytes_per_row = match datatype {
            VectorStorageDatatype::Float32
            | VectorStorageDatatype::Float16
            | VectorStorageDatatype::Uint8 => storage_bytes_per_row,
            VectorStorageDatatype::Turbo4
            | VectorStorageDatatype::Turbo8
            | VectorStorageDatatype::Turbo16 => {
                shared::quantized_vector_size(*size, *distance, shared::tq_bits(datatype))
            }
        };
        let index_bytes_per_row = if !is_hnsw_indexed(index) {
            0
        } else if let Some(quantization) = quantization_config {
            quantized_bits_per_row(quantization, *size).div_ceil(BYTE_BITS)
        } else {
            storage_bytes_per_row
        };
        Self {
            raw_bytes_per_row,
            storage_bytes_per_row,
            index_bytes_per_row,
        }
    }

    fn count_rows(&self, name: &str, raw: &[u8]) -> OperationResult<usize> {
        let Self {
            raw_bytes_per_row,
            storage_bytes_per_row: _,
            index_bytes_per_row: _,
        } = self;
        if *raw_bytes_per_row == 0 || !raw.len().is_multiple_of(*raw_bytes_per_row) {
            return Err(OperationError::service_error(format!(
                "Invalid stored vector size for {name:?}"
            )));
        }
        Ok(raw.len() / raw_bytes_per_row)
    }

    fn bytes(&self, rows: usize) -> UpdateBytes {
        let Self {
            raw_bytes_per_row: _,
            storage_bytes_per_row,
            index_bytes_per_row,
        } = self;
        UpdateBytes {
            updated_bytes: rows as u64 * *storage_bytes_per_row as u64,
            indexed_bytes: rows as u64 * *index_bytes_per_row as u64,
        }
    }
}

#[allow(deprecated)]
fn is_hnsw_indexed(index: &Indexes) -> bool {
    match index {
        Indexes::Plain {} => false,
        Indexes::Hnsw(HnswConfig {
            m,
            ef_construct: _,
            full_scan_threshold: _,
            max_indexing_threads: _,
            on_disk: _,
            memory: _,
            payload_m,
            inline_storage: _,
        }) => *m > 0 || payload_m.unwrap_or(*m) > 0,
    }
}

impl SparseMeter {
    fn new(config: &SparseVectorDataConfig) -> Self {
        let SparseVectorDataConfig {
            index:
                SparseIndexConfig {
                    full_scan_threshold: _,
                    index_type: _,
                    datatype,
                    memory: _,
                },
            storage_type: _,
            modifier: _,
        } = config;
        let value_bytes = match datatype.unwrap_or_default() {
            VectorStorageDatatype::Float16 => 2,
            VectorStorageDatatype::Uint8 => 1,
            VectorStorageDatatype::Float32
            | VectorStorageDatatype::Turbo4
            | VectorStorageDatatype::Turbo8
            | VectorStorageDatatype::Turbo16 => 4,
        };
        Self {
            // Storage keeps the logical element regardless of the index datatype.
            storage_bytes_per_element: size_of::<u32>() + size_of::<f32>(),
            index_bytes_per_element: size_of::<u32>() + value_bytes,
        }
    }

    fn bytes(&self, elements: usize) -> UpdateBytes {
        let Self {
            storage_bytes_per_element,
            index_bytes_per_element,
        } = self;
        UpdateBytes {
            updated_bytes: elements as u64 * *storage_bytes_per_element as u64,
            indexed_bytes: elements as u64 * *index_bytes_per_element as u64,
        }
    }
}

const BYTE_BITS: usize = u8::BITS as usize;

/// Encoded vector data only: no padding, offsets, norms or correction terms.
fn storage_bits_per_row(datatype: VectorStorageDatatype, dim: usize) -> usize {
    match datatype {
        VectorStorageDatatype::Float32 => dim * 32,
        VectorStorageDatatype::Float16 | VectorStorageDatatype::Turbo16 => dim * 16,
        VectorStorageDatatype::Uint8 | VectorStorageDatatype::Turbo8 => dim * 8,
        VectorStorageDatatype::Turbo4 => dim * 4,
    }
}

/// Encoded vector data only: no padding, offsets, norms or correction terms.
#[allow(deprecated)]
fn quantized_bits_per_row(config: &QuantizationConfig, dim: usize) -> usize {
    match config {
        QuantizationConfig::Scalar(ScalarQuantization {
            scalar:
                ScalarQuantizationConfig {
                    r#type,
                    quantile: _,
                    always_ram: _,
                    memory: _,
                },
        }) => match r#type {
            ScalarType::Int8 => dim * 8,
        },
        // One byte per bucket of 1, 2, 4, 8 or 16 dimensions.
        QuantizationConfig::Product(ProductQuantization {
            product:
                ProductQuantizationConfig {
                    compression,
                    always_ram: _,
                    memory: _,
                },
        }) => match compression {
            CompressionRatio::X4 => dim * 8,
            CompressionRatio::X8 => dim * 4,
            CompressionRatio::X16 => dim * 2,
            CompressionRatio::X32 => dim,
            CompressionRatio::X64 => dim.div_ceil(2),
        },
        QuantizationConfig::Binary(BinaryQuantization {
            binary:
                BinaryQuantizationConfig {
                    always_ram: _,
                    memory: _,
                    encoding,
                    query_encoding: _,
                },
        }) => match encoding.unwrap_or_default() {
            BinaryQuantizationEncoding::OneBit => dim,
            BinaryQuantizationEncoding::OneAndHalfBits => (dim * 3).div_ceil(2),
            BinaryQuantizationEncoding::TwoBits => dim * 2,
        },
        QuantizationConfig::Turbo(TurboQuantization {
            turbo:
                TurboQuantQuantizationConfig {
                    always_ram: _,
                    memory: _,
                    bits,
                },
        }) => match bits.unwrap_or_default() {
            TurboQuantBitSize::Bits1 => dim,
            TurboQuantBitSize::Bits1_5 => (dim * 3).div_ceil(2),
            TurboQuantBitSize::Bits2 => dim * 2,
            TurboQuantBitSize::Bits4 => dim * 4,
            TurboQuantBitSize::Bits8 => dim * 8,
        },
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
