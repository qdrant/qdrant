//! [`EdgeShard::query_groups`] — querying with results grouped by a payload
//! field.

use edge::EdgeShardRead as _;
use segment::data_types::groups::GroupId as SegmentGroupId;
use shard::grouping::Group as InternalGroup;

use crate::EdgeShard;
use crate::error::Result;
use crate::ops::query::QueryRequest;
use crate::types::ScoredPoint;

#[uniffi::export]
impl EdgeShard {
    /// Executes a query whose results are grouped by a payload field: at most
    /// `request.groups` groups, each holding up to `request.group_size` of
    /// the field value's best hits.
    ///
    /// Points missing the `group_by` field are skipped. The base query's
    /// `limit`/`offset` are driven by the grouping itself.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the base
    /// query is invalid.
    pub fn query_groups(&self, request: GroupRequest) -> Result<Vec<Group>> {
        self.with_shard(|shard| {
            let groups = shard.query_groups(request.try_into()?)?;
            Ok(groups.into_iter().map(Group::from).collect())
        })
    }
}

// ── GroupRequest ────────────────────────────────────────────────────────────

/// A grouped-query request.
#[derive(Clone, Debug, uniffi::Record)]
pub struct GroupRequest {
    /// Base scoring query. Its `limit`/`offset` are driven by the grouping;
    /// the query vector, filter, params and score threshold are honoured.
    pub query: QueryRequest,
    /// Payload key to group by (JSON-path syntax supported).
    pub group_by: String,
    /// Maximum number of groups to return.
    pub groups: u64,
    /// Maximum number of hits per group.
    pub group_size: u64,
}

impl TryFrom<GroupRequest> for edge::GroupRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: GroupRequest) -> Result<Self, Self::Error> {
        let GroupRequest {
            query,
            group_by,
            groups,
            group_size,
        } = r;
        Ok(edge::GroupRequest {
            query: query.try_into()?,
            group_by: crate::error::parse_json_path(&group_by)?,
            groups: crate::error::bounded_limit("groups", groups)?,
            group_size: crate::error::bounded_limit("group_size", group_size)?,
        })
    }
}

// ── Group ───────────────────────────────────────────────────────────────────

/// The `group_by` value shared by all hits of a [`Group`].
#[derive(Clone, Debug, uniffi::Enum)]
pub enum GroupId {
    /// A string-valued group key.
    String { value: String },
    /// An unsigned integer group key.
    NumberU64 { value: u64 },
    /// A signed integer group key.
    NumberI64 { value: i64 },
}

impl From<SegmentGroupId> for GroupId {
    fn from(id: SegmentGroupId) -> Self {
        match id {
            SegmentGroupId::String(value) => GroupId::String { value },
            SegmentGroupId::NumberU64(value) => GroupId::NumberU64 { value },
            SegmentGroupId::NumberI64(value) => GroupId::NumberI64 { value },
        }
    }
}

/// One group of a [`EdgeShard::query_groups`] response: the shared key and
/// the group's best hits, in score order.
#[derive(Clone, Debug, uniffi::Record)]
pub struct Group {
    /// Value of the `group_by` field shared by every hit in this group.
    pub key: GroupId,
    /// The group's hits, best first, at most `group_size` of them.
    pub hits: Vec<ScoredPoint>,
}

impl From<InternalGroup> for Group {
    fn from(g: InternalGroup) -> Self {
        let InternalGroup { hits, key } = g;
        Group {
            key: GroupId::from(key),
            hits: hits.into_iter().map(ScoredPoint::from).collect(),
        }
    }
}
