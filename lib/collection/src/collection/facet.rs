use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::data_types::facets::{FacetParams, FacetResponse, FacetValue};

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::CollectionResult;

/// Multiplier applied to the user-facing `limit` to derive the per-shard
/// over-fetch cap when running approximate facet queries. With unique-per-point
/// values (e.g. UUIDs), every distinct value has count = 1, so the practical
/// accuracy of approximate top-k is bounded by what each shard returns; the
/// factor is a trade-off between per-shard payload/work and result accuracy.
const FACET_OVERSAMPLE_FACTOR: usize = 4;

/// Lower bound for the per-shard over-fetch cap, so small `limit` values still
/// have enough headroom to absorb cross-shard rank differences.
const FACET_MIN_OVERSAMPLE: usize = 128;

impl Collection {
    pub async fn facet(
        &self,
        mut request: FacetParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        let response_limit = request.limit;
        if response_limit == 0 {
            return Ok(FacetResponse::default());
        }
        if !request.exact {
            // Approximate facet sends only the established limit between shards.
            //
            // Oversample to preserve accuracy.
            request.limit = request
                .limit
                .saturating_mul(FACET_OVERSAMPLE_FACTOR)
                .max(FACET_MIN_OVERSAMPLE);
        };

        self.facet_internal(
            request,
            response_limit,
            shard_selection,
            read_consistency,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    pub async fn facet_internal(
        &self,
        request: FacetParams,
        response_limit: usize,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        if request.limit == 0 {
            return Ok(FacetResponse::default());
        }

        let request = Arc::new(request);

        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(&shard_selection)?;

        let mut shards_reads_f = target_shards
            .iter()
            .map(|(shard, _shard_key)| {
                shard.facet(
                    request.clone(),
                    read_consistency,
                    shard_selection.is_shard_id(),
                    timeout,
                    hw_measurement_acc.clone(),
                )
            })
            .collect::<FuturesUnordered<_>>();

        // Collect results from all shards into a single map
        let mut aggregated_results: HashMap<FacetValue, usize> = HashMap::new();
        while let Some(response) = shards_reads_f.try_next().await? {
            for hit in response.hits {
                *aggregated_results.entry(hit.value).or_insert(0) += hit.count;
            }
        }

        Ok(FacetResponse::top_hits(aggregated_results, response_limit))
    }
}
