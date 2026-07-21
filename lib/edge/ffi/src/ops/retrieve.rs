//! [`EdgeShard::retrieve`] — fetching points by ID.

use segment::types::{PointIdType, WithPayloadInterface, WithVector as SegmentWithVector};

use crate::EdgeShard;
use crate::error::Result;
use crate::types::{PointId, Record, WithPayload, WithVector};

#[uniffi::export]
impl EdgeShard {
    /// Fetches the points with the given IDs.
    ///
    /// IDs that do not exist in the shard are simply omitted from the
    /// returned list; no error is raised. Set `request.with_payload` and/or
    /// `request.with_vector` to control what is included in each returned
    /// [`Record`](crate::types::Record).
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::InvalidArgument`](crate::error::EdgeError) if a UUID ID
    /// or a payload selector key is malformed.
    pub fn retrieve(&self, request: RetrieveRequest) -> Result<Vec<Record>> {
        self.with_shard(|shard| {
            let records = shard.retrieve(request.try_into()?)?;
            Ok(records.into_iter().map(Record::from).collect())
        })
    }
}

// ── RetrieveRequest ─────────────────────────────────────────────────────────

/// A by-ID retrieval request, mirroring `edge::RetrieveRequest`.
#[derive(Clone, Debug, uniffi::Record)]
pub struct RetrieveRequest {
    /// IDs of the points to retrieve. IDs not present in the shard are
    /// omitted from the response.
    pub point_ids: Vec<PointId>,
    /// Select which payload to return with the response. Defaults to all
    /// payload when unset.
    #[uniffi(default = None)]
    pub with_payload: Option<WithPayload>,
    /// Select which vectors to include in the response. Defaults to none
    /// when unset.
    #[uniffi(default = None)]
    pub with_vector: Option<WithVector>,
}

impl TryFrom<RetrieveRequest> for edge::RetrieveRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: RetrieveRequest) -> Result<Self, Self::Error> {
        let RetrieveRequest {
            point_ids,
            with_payload,
            with_vector,
        } = r;
        Ok(edge::RetrieveRequest {
            point_ids: point_ids
                .into_iter()
                .map(PointIdType::try_from)
                .collect::<Result<Vec<_>>>()?,
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?,
            with_vector: with_vector.map(SegmentWithVector::from),
        })
    }
}
