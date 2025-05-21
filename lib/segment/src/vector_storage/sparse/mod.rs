pub mod mmap_sparse_vector_storage;
#[cfg(feature = "rocksdb")]
pub mod simple_sparse_vector_storage;
mod stored_sparse_vectors;
#[cfg(test)]
pub mod volatile_sparse_vector_storage;

use crate::types::Distance;

pub const SPARSE_VECTOR_DISTANCE: Distance = Distance::Dot;
