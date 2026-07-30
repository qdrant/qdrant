use smallvec::SmallVec;

use crate::data_types::vectors::VectorInternal;
use crate::types::{MaybeRawPayload, Payload, PointIdType, VectorNameBuf};

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

/// Byte-blob analogue of [`SegmentRecord`]: vectors as stored, so a reader that
/// only relocates the point does not decode them.
pub struct SegmentRecordRaw {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorBytesOwned>,
    /// The payload in the form [`RawPayloadFormat`] asked for, or parsed if the
    /// storage could not answer with a blob — see [`MaybeRawPayload`].
    pub payload: Option<MaybeRawPayload>,
}

/// What a raw retrieval should do about the payload.
///
/// The caller states what it wants; [`MaybeRawPayload`] states what it got,
/// which can differ only in one direction: a storage that keeps payloads parsed
/// cannot answer [`Self::Raw`] with a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RawPayloadFormat {
    /// Do not read the payload at all.
    #[default]
    NoPayload,
    /// Read it parsed, as [`SegmentRecord`] carries it.
    Parsed,
    /// Read it as stored, sparing both a parse here and an encode wherever the
    /// payload is going.
    Raw,
}
