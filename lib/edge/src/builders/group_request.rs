//! Fluent builder for [`GroupRequest`].
//!
//! Builder fields mirror [`GroupRequest`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::json_path::JsonPath;

use crate::requests::grouping::GroupRequest;
use crate::requests::query::QueryRequest;

/// Fluent builder for [`GroupRequest`].
///
/// All fields are required and passed through [`Self::new`].
#[derive(Clone, Debug)]
pub struct GroupRequestBuilder {
    query: QueryRequest,
    group_by: JsonPath,
    groups: usize,
    group_size: usize,
}

impl GroupRequestBuilder {
    pub fn new(query: QueryRequest, group_by: JsonPath, groups: usize, group_size: usize) -> Self {
        let GroupRequest {
            query,
            group_by,
            groups,
            group_size,
        } = GroupRequest::new(query, group_by, groups, group_size);
        Self {
            query,
            group_by,
            groups,
            group_size,
        }
    }

    pub fn build(self) -> GroupRequest {
        // Exhaustively destructure Self and construct GroupRequest:
        // adding a field to either type forces a compile error here.
        let Self {
            query,
            group_by,
            groups,
            group_size,
        } = self;
        GroupRequest {
            query,
            group_by,
            groups,
            group_size,
        }
    }
}
