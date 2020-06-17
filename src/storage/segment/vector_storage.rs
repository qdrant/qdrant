use crate::common::types::PointIdType;

/// Trait for vector storage
///  El - type of vector element, expected numerical type
pub trait VectorStorage<El> {
    fn get_vector(&self, key: PointIdType) -> &Vec<El>;
    fn put_vector(&mut self, vector: &Vec<El>, key: PointIdType);
}

