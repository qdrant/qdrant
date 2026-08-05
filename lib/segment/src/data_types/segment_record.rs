use smallvec::SmallVec;

use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, RawPayload, VectorNameBuf};

/// A point almost always has a single (default) named vector, so keep it inline
/// to avoid a heap allocation on the common retrieve path.
pub type NamedVectorsOwned = SmallVec<[(VectorNameBuf, VectorInternal); 1]>;

/// Byte-blob analogue of [`NamedVectorsOwned`]: vectors as storage-native bytes.
pub type NamedVectorBytesOwned = SmallVec<[(VectorNameBuf, Vec<u8>); 1]>;

/// A retrieved point: id, optional vectors, optional payload.
pub struct SegmentRecord {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorsOwned>,
    pub payload: Option<Payload>,
}

/// Byte-blob analogue of [`SegmentRecord`]: vectors and payload as stored, so a
/// reader that only relocates the point does not decode them.
pub struct SegmentRecordRaw {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorBytesOwned>,
    /// The payload as stored, sparing both a parse here and an encode wherever
    /// it is going. `None` when the point has no payload stored.
    pub payload: Option<RawPayload>,
}
