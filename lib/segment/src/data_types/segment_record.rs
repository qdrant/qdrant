use smallvec::SmallVec;

use crate::data_types::tiny_map;
use crate::data_types::vectors::VectorInternal;
use crate::types::{Payload, PointIdType, VectorNameBuf};

pub type NamedVectorsOwned = Vec<(VectorNameBuf, VectorInternal)>;

/// Storage-native named vectors of one point: the `(name, bytes)` list read
/// by `retrieve_raw`. Inline capacity follows [`tiny_map::CAPACITY`] for the
/// same reason `NamedVectors` does — points rarely carry more named vectors
/// than that, so the common case doesn't heap-allocate the list itself.
pub type RawNamedVectors = SmallVec<[(VectorNameBuf, Vec<u8>); tiny_map::CAPACITY]>;

/// A retrieved point: id, optional vectors, optional payload.
pub struct SegmentRecord {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorsOwned>,
    pub payload: Option<Payload>,
}

/// Byte-blob analogue of [`SegmentRecord`] for single-point raw retrieval:
/// vectors are storage-native bytes instead of decoded [`VectorInternal`]s.
/// The point id is implied by the lookup, so it is not carried here.
pub struct SegmentRecordRaw {
    pub vectors: Option<RawNamedVectors>,
    pub payload: Option<Payload>,
}
