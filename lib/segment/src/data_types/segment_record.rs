use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, VectorNameBuf};

pub type NamedVectorsOwned = Vec<(VectorNameBuf, VectorInternal)>;

/// A retrieved point: id, optional vectors, optional payload.
///
/// `V` is the vector representation: [`VectorInternal`] for [`SegmentRecord`],
/// storage-native bytes for [`SegmentRecordRaw`].
pub struct SegmentRecordGeneric<V> {
    pub id: PointIdType,
    pub vectors: Option<Vec<(VectorNameBuf, V)>>,
    pub payload: Option<Payload>,
}

pub type SegmentRecord = SegmentRecordGeneric<VectorInternal>;

/// Byte-blob analogue of [`SegmentRecord`].
pub type SegmentRecordRaw = SegmentRecordGeneric<Vec<u8>>;
