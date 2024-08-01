use std::sync::Arc;

use futures::future::try_join_all;
use itertools::{process_results, Itertools};
use segment::data_types::facets::{aggregate_facet_hits, FacetRequest, FacetValueHit};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::operations::types::CollectionResult;

impl LocalShard {
    pub async fn do_facet(
        &self,
        request: Arc<FacetRequest>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<FacetValueHit>> {
        let spawn_read = |segment: LockedSegment| {
            let request = Arc::clone(&request);

            search_runtime_handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();

                read_segment.facet(&request)
            })
        };

        let all_reads = {
            let segments_lock = self.segments().read();

            try_join_all(
                segments_lock
                    .non_appendable_then_appendable_segments()
                    .map(spawn_read),
            )
        }
        .await?;

        let merged_hits =
            process_results(all_reads, |reads| aggregate_facet_hits(reads.flatten()))?;

        let top_hits = merged_hits
            .into_iter()
            .map(|(value, count)| FacetValueHit { value, count })
            .k_largest(request.limit)
            .collect();

        Ok(top_hits)
    }
}
