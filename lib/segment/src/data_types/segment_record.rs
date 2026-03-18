use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, VectorNameBuf};

pub type NamedVectorsOwned = Vec<(VectorNameBuf, VectorInternal)>;

pub struct SegmentRecord {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorsOwned>,
    pub payload: Option<Payload>,
}

impl SegmentRecord {
    pub fn empty(id: PointIdType) -> Self {
        Self {
            id,
            vectors: Some(NamedVectorsOwned::default()),
            payload: None,
        }
    }
}
