//! Fluent builder for [`ScrollRequest`].
//!
//! Builder fields mirror [`ScrollRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::data_types::order_by::OrderByInterface;
use segment::types::{Filter, PointIdType, WithPayloadInterface, WithVector};

use crate::requests::scroll::ScrollRequest;

/// Fluent builder for [`ScrollRequest`].
///
/// Every field is optional and falls back to the [`ScrollRequest::new`] defaults.
#[derive(Clone, Debug)]
pub struct ScrollRequestBuilder {
    offset: Option<PointIdType>,
    limit: Option<usize>,
    filter: Option<Filter>,
    with_payload: Option<WithPayloadInterface>,
    with_vector: WithVector,
    order_by: Option<OrderByInterface>,
}

impl ScrollRequestBuilder {
    pub fn new() -> Self {
        let ScrollRequest {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = ScrollRequest::new();
        Self {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        }
    }

    pub fn offset(mut self, offset: PointIdType) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_payload(mut self, with_payload: WithPayloadInterface) -> Self {
        self.with_payload = Some(with_payload);
        self
    }

    pub fn with_vector(mut self, with_vector: WithVector) -> Self {
        self.with_vector = with_vector;
        self
    }

    pub fn order_by(mut self, order_by: OrderByInterface) -> Self {
        self.order_by = Some(order_by);
        self
    }

    pub fn build(self) -> ScrollRequest {
        // Exhaustively destructure Self and construct ScrollRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = self;
        ScrollRequest {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        }
    }
}

impl Default for ScrollRequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}
