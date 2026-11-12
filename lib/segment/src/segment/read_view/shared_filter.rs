use common::types::DeferredBehavior;

use crate::common::operation_error::OperationResult;
use crate::data_types::filter_user::FilterUser;
use crate::data_types::query_context::SegmentQueryContext;
use crate::id_tracker::IdTrackerRead;
use crate::index::query_optimization::filter_checks::worth_sharing;
use crate::index::query_optimization::resolved_points::ResolvedPoints;
use crate::index::{PayloadIndexRead, VectorIndexRead};
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::types::{Condition, Filter};

impl<TIdT, TPI, TPS, TVD> SegmentReadView<'_, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// See [`ReadSegmentEntry::share_filter`].
    ///
    /// [`ReadSegmentEntry::share_filter`]: crate::entry::entry_point::ReadSegmentEntry::share_filter
    pub fn share_filter(
        &self,
        filter: &Filter,
        users: &[FilterUser<'_>],
        query_context: &SegmentQueryContext,
    ) -> OperationResult<Option<Filter>> {
        if users.len() < 2 {
            return Ok(None);
        }
        // Account the filter evaluation to the query, like `search_batch` does
        let _scope = query_context.handoff().enter_guard();

        let cardinality = self.payload_index.estimate_cardinality(filter)?;
        let materialization_cost = self.payload_index.materialization_cost(&cardinality)?;

        let mut uses = Vec::with_capacity(users.len());
        for user in users {
            let FilterUser {
                vector_name,
                query_vectors,
                top,
                params,
            } = *user;
            let Some(vector_data) = self.vector_data.get(vector_name) else {
                // That search fails anyway
                return Ok(None);
            };
            let checks = vector_data.vector_index().filter_checks(
                query_vectors,
                &cardinality,
                top,
                params,
            )?;
            uses.push(checks);
        }
        if !worth_sharing(materialization_cost, uses.iter().copied()) {
            return Ok(None);
        }

        // The visibility vector searches use
        let points = self.payload_index.materialize_filter(
            filter,
            &cardinality,
            query_context.is_stopped_flag(),
            DeferredBehavior::VisibleOnly,
        )?;
        Ok(Some(Filter::new_must(Condition::ResolvedPoints(
            ResolvedPoints::new(self.segment_uuid, points),
        ))))
    }
}
