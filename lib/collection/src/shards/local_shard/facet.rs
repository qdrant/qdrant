use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::process_results;
use segment::data_types::facets::{aggregate_facet_hits, FacetRequestInternal, FacetValueHit};
use tokio::runtime::Handle;
use tokio::time::error::Elapsed;

use super::LocalShard;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{CollectionError, CollectionResult};

impl LocalShard {
    pub async fn do_facet(
        &self,
        request: Arc<FacetRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<FacetValueHit>> {
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let stopping_guard = StoppingGuard::new();

        let spawn_read = |segment: LockedSegment| {
            let request = Arc::clone(&request);
            let is_stopped = stopping_guard.get_is_stopped();

            search_runtime_handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();

                read_segment.facet(&request, &is_stopped)
            })
        };

        let all_reads = {
            let segments_lock = self.segments().read();

            tokio::time::timeout(
                timeout,
                try_join_all(
                    segments_lock
                        .non_appendable_then_appendable_segments()
                        .map(spawn_read),
                ),
            )
        }
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "facet"))??;

        let merged_hits =
            process_results(all_reads, |reads| aggregate_facet_hits(reads.flatten()))?;

        // TODO(luis): We can't just select top values, because we need to aggregate across segments,
        // which we can't assume to select the same best top.
        //
        // We need all values to be able to aggregate correctly across segments
        let top_hits = merged_hits
            .into_iter()
            .map(|(value, count)| FacetValueHit { value, count })
            .collect();

        Ok(top_hits)
    }
}
