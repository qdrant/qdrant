use serde::{Deserialize, Serialize};
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

/// Byte-blob analogue of [`SegmentRecord`].
pub struct SegmentRecordRaw {
    pub id: PointIdType,
    pub vectors: Option<NamedVectorBytesOwned>,
    pub payload: Option<Payload>,
    pub payload_raw: Option<RawPayload>,
}

/// Encoding of a raw payload blob transferred alongside raw vectors,
/// as it is persisted in WAL.
///
/// Internal counterpart of `api::grpc::qdrant::RawPayloadEncoding`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RawPayloadEncoding {
    /// serde_json encoding of the whole payload object, uncompressed,
    /// exactly as stored in gridstore.
    #[default]
    JsonBytes,
}
