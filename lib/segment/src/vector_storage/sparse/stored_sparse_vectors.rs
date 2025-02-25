use common::delta_pack::{delta_pack, delta_unpack};
use gridstore::Blob;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::{SparseVector, double_sort};
use sparse::common::types::{DimId, DimId64, DimWeight};

use crate::common::operation_error::OperationError;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct StoredSparseVector {
    /// Compressed u64 indices
    pub indices: Vec<u8>,
    /// Values and indices must be the same length
    pub values: Vec<DimWeight>,
}

impl StoredSparseVector {
    /// Convert indices into a byte array
    /// Use bitpacking and delta-encoding for additional compression
    fn serialize_indices(indices: &[DimId64]) -> Vec<u8> {
        delta_pack(indices)
    }

    /// Recover indices from a byte array
    fn deserialize_indices(data: &[u8]) -> Vec<DimId64> {
        delta_unpack(data)
    }
}

impl From<&SparseVector> for StoredSparseVector {
    fn from(vector: &SparseVector) -> Self {
        let mut stored_indices: Vec<_> =
            vector.indices.iter().copied().map(DimId64::from).collect();
        let mut stored_values = vector.values.clone();

        double_sort(&mut stored_indices, &mut stored_values);

        let compressed_indices = StoredSparseVector::serialize_indices(&stored_indices);

        Self {
            indices: compressed_indices,
            values: stored_values,
        }
    }
}

impl TryFrom<StoredSparseVector> for SparseVector {
    type Error = OperationError;

    fn try_from(value: StoredSparseVector) -> Result<Self, Self::Error> {
        let decompressed_indices = StoredSparseVector::deserialize_indices(&value.indices);

        Ok(SparseVector {
            indices: decompressed_indices
                .into_iter()
                .map(DimId::try_from)
                .collect::<Result<_, _>>()
                .map_err(|err| {
                    OperationError::service_error(format!("Failed to convert indices: {err}"))
                })?,
            values: value.values,
        })
    }
}

impl Blob for StoredSparseVector {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Sparse vector serialization should not fail")
    }

    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data).expect("Sparse vector deserialization should not fail")
    }
}
