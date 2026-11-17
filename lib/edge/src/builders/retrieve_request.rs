//! Fluent builder for [`RetrieveRequest`].
//!
//! Builder fields mirror [`RetrieveRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::types::{ExtendedPointId, WithPayloadInterface, WithVector};

use crate::requests::retrieve::RetrieveRequest;

/// Fluent builder for [`RetrieveRequest`].
///
/// `point_ids` is required and passed through [`Self::new`]; every other field
/// is optional and falls back to the [`RetrieveRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct RetrieveRequestBuilder {
    point_ids: Vec<ExtendedPointId>,
    with_payload: Option<WithPayloadInterface>,
    with_vector: Option<WithVector>,
}

impl RetrieveRequestBuilder {
    pub fn new(point_ids: Vec<ExtendedPointId>) -> Self {
        let RetrieveRequest {
            point_ids,
            with_payload,
            with_vector,
        } = RetrieveRequest::new(point_ids);
        Self {
            point_ids,
            with_payload,
            with_vector,
        }
    }

    pub fn with_payload(mut self, with_payload: WithPayloadInterface) -> Self {
        self.with_payload = Some(with_payload);
        self
    }

    pub fn with_vector(mut self, with_vector: WithVector) -> Self {
        self.with_vector = Some(with_vector);
        self
    }

    pub fn build(self) -> RetrieveRequest {
        // Exhaustively destructure Self and construct RetrieveRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            point_ids,
            with_payload,
            with_vector,
        } = self;
        RetrieveRequest {
            point_ids,
            with_payload,
            with_vector,
        }
    }
}
