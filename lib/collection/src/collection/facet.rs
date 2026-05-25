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
const FACET_OVER_FETCH_FACTOR: usize = 4;

/// Lower bound for the per-shard over-fetch cap, so small `limit` values still
/// have enough headroom to absorb cross-shard rank differences.
const FACET_MIN_OVER_FETCH: usize = 128;

impl Collection {
    pub async fn facet(
        &self,
        request: FacetParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        self.facet_with_output_limit(
            request,
            None,
            shard_selection,
            read_consistency,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    /// Like [`Self::facet`], but allows the caller to override the size of the
    /// returned hits set. Used by the internal single-shard gRPC handler, where
    /// the upstream coordinator passes the over-fetched `top_k` it expects so
    /// the per-shard response is bounded but cross-shard aggregation stays
    /// accurate.
    pub async fn facet_with_output_limit(
        &self,
        request: FacetParams,
        caller_output_limit: Option<usize>,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        if request.limit == 0 {
            return Ok(FacetResponse::default());
        }

        let limit = request.limit;
        // Pick the cap on the number of hits each shard is allowed to return
        // and on the size of the response we produce. Three cases:
        //
        // - `caller_output_limit = Some(k)`: an upstream coordinator is asking
        //   us for a single shard's contribution; respect its requested cap
        //   verbatim and skip the default over-fetch (which would otherwise
        //   compound across hops).
        // - `request.exact = true`: the exact path needs every value to be
        //   counted correctly, so we cannot bound the response.
        // - Otherwise (the user-facing approximate path): apply the default
        //   over-fetch so each shard returns enough hits to keep cross-shard
        //   aggregation accurate while bounding payload size and aggregation
        //   work for high-cardinality fields (e.g. UUIDs).
        let output_limit = if let Some(k) = caller_output_limit {
            Some(k)
        } else if request.exact {
            None
        } else {
            Some(
                limit
                    .saturating_mul(FACET_OVER_FETCH_FACTOR)
                    .max(FACET_MIN_OVER_FETCH),
            )
        };
        let request = Arc::new(request);

        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(&shard_selection)?;

        let mut shards_reads_f = target_shards
            .iter()
            .map(|(shard, _shard_key)| {
                shard.facet(
                    request.clone(),
                    output_limit,
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

        // When serving an internal single-shard request, propagate the caller's
        // requested cap instead of the user-facing limit, so the upstream
        // coordinator receives the over-fetched window it asked for.
        let response_limit = caller_output_limit.unwrap_or(limit);
        Ok(FacetResponse::top_hits(aggregated_results, response_limit))
    }
}
