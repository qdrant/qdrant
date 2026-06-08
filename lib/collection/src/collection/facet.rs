use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::data_types::facets::{FacetParams, FacetResponse, FacetValue};

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::routing::RoutingToken;
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
    /// Entry point for a facet request. Applies oversampling (for approximate
    /// queries) before fanning out to shards. This runs *once*, on the node
    /// that received the client request; peer nodes enter through
    /// [`Collection::facet_internal`] instead and never re-oversample.
    ///
    /// How `limit` flows through a distributed cluster for an approximate query.
    /// `L` is the user limit (here `L = 10`); `N` is the oversampled per-shard
    /// limit `N = max(L * 4, 128) = 128`:
    ///
    /// ```text
    ///                            Client  (limit = L = 10)
    ///                               │
    ///                               ▼
    ///   ┌── Node A: Collection::facet ───────────────────────────────────┐
    ///   │     request.limit:   L  ──oversample──▶  N = 128               │
    ///   │     response_limit:  L = 10  (kept for the final truncation)   │
    ///   │                              │                                 │
    ///   │                              ▼                                 │
    ///   │     Collection::facet_internal(limit = N, response_limit = L)  │
    ///   │              ┌───────────────┴────────────────┐                │
    ///   │              ▼ local shard                    ▼ remote shard   │
    ///   │     approx_facet(limit = N)          gRPC FacetCountsInternal  │
    ///   │     returns ≤ N hits                       { limit = N }       │
    ///   │              │                                 │               │
    ///   └──────────────┼─────────────────────────────────┼───────────────┘
    ///                  │                                 ▼
    ///                  │     ┌── Node B: facet_counts_internal ──────────┐
    ///                  │     │     toc.facet_internal: peer_limit = N    │
    ///                  │     │     (NOT re-oversampled)                  │
    ///                  │     │              │                            │
    ///                  │     │              ▼                            │
    ///                  │     │     Collection::facet_internal(           │
    ///                  │     │         limit = N, response_limit = N)    │
    ///                  │     │     approx_facet(limit = N)               │
    ///                  │     │     top_hits(.., N)  ──▶  ≤ N hits        │
    ///                  │     └──────────────┬────────────────────────────┘
    ///                  │                    │
    ///                  └─────────┬──────────┘
    ///                            ▼
    ///   Node A:  aggregate all shard hits (sum counts per value)
    ///            top_hits(aggregated, response_limit = L)  ──▶  L = 10 hits
    ///                            │
    ///                            ▼
    ///                          Client  (L = 10 results)
    /// ```
    ///
    /// Key points:
    /// - Oversampling (`L` → `N`) happens only here, on the entry node. Each
    ///   shard — local or remote — returns up to `N` values, giving the final
    ///   aggregation enough headroom to absorb cross-shard rank differences.
    /// - Remote peers are reached via the internal gRPC path, which calls
    ///   `facet_internal` directly with `peer_limit = N`, so the limit is *not*
    ///   multiplied a second time.
    /// - Only the final aggregation on the entry node truncates back to the
    ///   user-facing `response_limit = L`.
    pub async fn facet(
        &self,
        mut request: FacetParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
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
            routing_token,
            timeout,
            hw_measurement_acc,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn facet_internal(
        &self,
        request: FacetParams,
        response_limit: usize,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
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
                    routing_token,
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
