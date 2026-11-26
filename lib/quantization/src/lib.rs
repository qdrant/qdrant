pub mod encoded_storage;
pub mod encoded_vectors;
pub mod encoded_vectors_binary;
pub mod encoded_vectors_pq;
pub mod encoded_vectors_tq;
pub mod encoded_vectors_u8;
pub mod kmeans;
pub mod p_square;
pub mod quantile;
pub mod symmetric_vectors;
pub mod turboquant;
pub mod vector_stats;

use std::fmt::Display;

pub use encoded_storage::{EncodedStorage, EncodedStorageBuilder, EncodedStorageWrite};
pub use encoded_vectors::{DistanceType, EncodedVectors, EncodedVectorsWrite, VectorParameters};
pub use encoded_vectors_pq::{EncodedQueryPQ, EncodedVectorsPQ};
pub use encoded_vectors_u8::{EncodedQueryU8, EncodedVectorsU8};
pub use symmetric_vectors::{DuplicateKey, SymmetricVectors};

#[derive(Debug, PartialEq, Eq)]
pub enum EncodingError {
    IOError(String),
    EncodingError(String),
    ArgumentsError(String),
    Stopped,
}

impl Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::IOError(description) => write!(f, "IOError: {description}"),
            EncodingError::EncodingError(description) => {
                write!(f, "EncodingError: {description}")
            }
            EncodingError::ArgumentsError(description) => {
                write!(f, "ArgumentsError: {description}")
            }
            EncodingError::Stopped => write!(f, "Stopped"),
        }
    }
}
