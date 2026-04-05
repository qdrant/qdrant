pub mod empty_sparse_vector_storage;
pub mod mmap_sparse_vector_storage;
mod stored_sparse_vectors;
pub mod volatile_sparse_vector_storage;

use crate::types::Distance;

pub const SPARSE_VECTOR_DISTANCE: Distance = Distance::Dot;
