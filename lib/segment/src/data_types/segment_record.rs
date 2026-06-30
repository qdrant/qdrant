use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, VectorNameBuf};

pub type NamedVectorsOwned = Vec<(VectorNameBuf, VectorInternal)>;

/// Like [`NamedVectorsOwned`], but vectors are kept as storage-native bytes
/// instead of decoded [`VectorInternal`]s (avoids a lossy round-trip).
pub type NamedVectorsOwnedRaw = Vec<(VectorNameBuf, Vec<u8>)>;

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

impl<V> SegmentRecordGeneric<V> {
    pub fn empty(id: PointIdType) -> Self {
        Self {
            id,
            vectors: Some(Vec::new()),
            payload: None,
        }
    }
}
