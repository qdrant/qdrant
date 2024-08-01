use std::time::Duration;

use futures::future::try_join_all;
use segment::types::{Filter, PointIdType};
use tokio::runtime::Handle;
use tokio::time::error::Elapsed;

use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::local_shard::LocalShard;

impl LocalShard {
    pub async fn do_sample_filtered_points(
        &self,
        limit: usize,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<PointIdType>> {
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let segments = self.segments();

        let (non_appendable, appendable) = segments.read().split_segments();

        let read_filtered = |segment: LockedSegment| {
            let filter = filter.cloned();
            search_runtime_handle.spawn_blocking(move || {
                let get_segment = segment.get();
                let read_segment = get_segment.read();
                // TODO handle cancellation with dropped StoppingGuard
                read_segment.read_random_filtered(limit, filter.as_ref())
            })
        };

        let all_reads = tokio::time::timeout(
            timeout,
            try_join_all(
                non_appendable
                    .into_iter()
                    .chain(appendable)
                    .map(read_filtered),
            ),
        )
        .await
        .map_err(|_: Elapsed| CollectionError::timeout(timeout.as_secs() as usize, "sampling"))??;
        let point_ids = all_reads.into_iter().flatten().collect();
        Ok(point_ids)
    }
}
