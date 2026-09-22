use common::counter::hardware_accumulator::HwMeasurementAcc;
use rayon::prelude::*;
use segment::data_types::attention::{AttentionRequest, AttentionResponse};
use segment::entry::ReadSegmentEntry;
use segment::index::vector_index_base::VectorIndexEnum;
use shard::common::stopping_guard::StoppingGuard;
use shard::locked_segment::LockedSegment;

use super::LocalShard;
use crate::operations::types::{CollectionError, CollectionResult};

impl LocalShard {
    pub(crate) async fn attention(
        &self,
        requests: Vec<AttentionRequest>,
        hw: HwMeasurementAcc,
    ) -> CollectionResult<Vec<AttentionResponse>> {
        self.check_read_rate_limiter(&hw, "attention", || requests.len())?;
        let segments = self.segments.clone();
        let guard = StoppingGuard::new();
        let stopped = guard.get_is_stopped();
        self.search_runtime
            .spawn_blocking(move || {
                let holder = segments.read();
                let mut populated = None;
                for (_, segment) in holder.iter() {
                    let LockedSegment::Original(segment) = segment else {
                        return Err(CollectionError::bad_request(
                            "attention does not support proxy segments; wait for indexing",
                        ));
                    };
                    if segment.read().available_point_count() == 0 {
                        continue;
                    }
                    if populated.replace(segment.clone()).is_some() {
                        return Err(CollectionError::bad_request(
                            "attention requires exactly one populated segment",
                        ));
                    }
                }
                let segment = populated.ok_or_else(|| {
                    CollectionError::bad_request(
                        "attention requires a populated page-attention segment",
                    )
                })?;
                requests
                    .par_iter()
                    .map(|request| {
                        let segment = segment.read();
                        let data = segment.vector_data.get(&request.using).ok_or_else(|| {
                            CollectionError::bad_request(format!(
                                "Unknown vector name: {}",
                                request.using
                            ))
                        })?;
                        let index = data.vector_index.borrow();
                        let VectorIndexEnum::PageAttention(index) = &*index else {
                            return Err(CollectionError::bad_request(
                                "attention requires a page-attention index; wait for indexing",
                            ));
                        };
                        index.attention(request, &stopped).map_err(Into::into)
                    })
                    .collect()
            })
            .await
            .map_err(|err| CollectionError::service_error(err.to_string()))?
    }
}
