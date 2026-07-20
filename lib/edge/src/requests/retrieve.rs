use segment::types::{ExtendedPointId, WithPayloadInterface, WithVector};

/// Retrieve points by their ids. The response preserves the requested id order and skips
/// ids that do not exist in the shard.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrieveRequest {
    /// Ids of the points to retrieve.
    pub point_ids: Vec<ExtendedPointId>,
    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: Option<WithVector>,
}

impl RetrieveRequest {
    pub fn new(point_ids: Vec<ExtendedPointId>) -> Self {
        Self {
            point_ids,
            with_payload: None,
            with_vector: None,
        }
    }
}
