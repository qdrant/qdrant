use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::data_types::idf_estimate::{IdfEstimate, IdfEstimateParams, IdfStats};
use segment::data_types::modifier::Modifier;

use super::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::routing::RoutingToken;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult};

impl Collection {
    /// Entry point for an IDF estimation request. Validates the request
    /// against the collection config, collects raw statistics across shards
    /// and derives the IDF estimate from them. This runs *once*, on the node
    /// that received the client request; peer nodes enter through
    /// [`Collection::estimate_idf_internal`] instead and report raw
    /// statistics for the coordinating node to aggregate.
    pub async fn estimate_idf(
        &self,
        request: IdfEstimateParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<IdfEstimate> {
        // The endpoint reports the statistics sparse vector search with the
        // `idf` modifier uses, so vectors without the modifier are rejected
        // the same way the `idf` search param is.
        let is_idf_vector = self
            .collection_config
            .read()
            .await
            .params
            .get_sparse_vector_params_opt(&request.using)
            .map(|params| params.modifier == Some(Modifier::Idf))
            .unwrap_or(false);
        if !is_idf_vector {
            return Err(CollectionError::bad_input(format!(
                "IDF estimation requires a sparse vector with the `idf` modifier, \
                 which vector {:?} is not",
                request.using,
            )));
        }

        let indices = request.query.indices.clone();
        let stats = self
            .estimate_idf_internal(
                request,
                shard_selection,
                read_consistency,
                routing_token,
                timeout,
                hw_measurement_acc,
            )
            .await?;

        Ok(stats.estimate(&indices))
    }

    /// Collect raw IDF statistics — the document count and per-term document
    /// frequencies — over the selected shards.
    ///
    /// Unlike sparse vector search, where each shard scores with its own
    /// local statistics, the estimate aggregates the raw statistics across
    /// all shards of the collection.
    pub async fn estimate_idf_internal(
        &self,
        request: IdfEstimateParams,
        shard_selection: ShardSelectorInternal,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<IdfStats> {
        let request = Arc::new(request);

        let shard_holder = self.shards_holder.read().await;
        let target_shards = shard_holder.select_shards(&shard_selection)?;

        let mut shards_reads_f = target_shards
            .iter()
            .map(|(shard, _shard_key)| {
                shard.estimate_idf(
                    request.clone(),
                    read_consistency,
                    routing_token,
                    shard_selection.is_shard_id(),
                    timeout,
                    hw_measurement_acc.clone(),
                )
            })
            .collect::<FuturesUnordered<_>>();

        // Sum raw statistics from all shards
        let mut aggregated_stats = IdfStats::default();
        while let Some(stats) = shards_reads_f.try_next().await? {
            aggregated_stats.merge(stats);
        }

        Ok(aggregated_stats)
    }
}
