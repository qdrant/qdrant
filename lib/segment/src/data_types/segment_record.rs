use smallvec::SmallVec;

use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, VectorNameBuf};

/// A point almost always has a single (default) named vector, so keep it inline
/// to avoid a heap allocation on the common retrieve path.
pub type NamedVectorsOwned = SmallVec<[(VectorNameBuf, VectorInternal); 1]>;

/// A retrieved point: id, optional vectors, optional payload.
///
/// `V` is the vector representation: [`VectorInternal`] for [`SegmentRecord`],
/// storage-native bytes for [`SegmentRecordRaw`].
pub struct SegmentRecordGeneric<V> {
    pub id: PointIdType,
    pub vectors: Option<SmallVec<[(VectorNameBuf, V); 1]>>,
    pub payload: Option<Payload>,
}

pub type SegmentRecord = SegmentRecordGeneric<VectorInternal>;

/// Byte-blob analogue of [`SegmentRecord`].
pub type SegmentRecordRaw = SegmentRecordGeneric<Vec<u8>>;
