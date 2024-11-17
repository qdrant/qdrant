//! Methods here are for distributed internal use only.

use std::time::Duration;

use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::UpdateResult;
use collection::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use collection::shards::shard::ShardId;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::facets::{FacetParams, FacetResponse};

use super::TableOfContent;
use crate::content_manager::errors::StorageResult;
use crate::rbac::{Access, AccessRequirements};

impl TableOfContent {
    pub async fn query_batch_internal(
        &self,
        collection_name: &str,
        requests: Vec<ShardQueryRequest>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> StorageResult<Vec<ShardQueryResponse>> {
        let collection = self.get_collection_unchecked(collection_name).await?;

        let res = collection
            .query_batch_internal(requests, &shard_selection, timeout, hw_measurement_acc)
            .await?;

        Ok(res)
    }

    pub async fn facet_internal(
        &self,
        collection_name: &str,
        request: FacetParams,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> StorageResult<FacetResponse> {
        let collection = self.get_collection_unchecked(collection_name).await?;

        let res = collection
            .facet(request, shard_selection, None, timeout)
            .await?;

        Ok(res)
    }

    pub async fn cleanup_local_shard(
        &self,
        collection_name: &str,
        shard_id: ShardId,
        access: Access,
    ) -> StorageResult<UpdateResult> {
        let collection_pass = access
            .check_collection_access(collection_name, AccessRequirements::new().write().whole())?;

        self.get_collection(&collection_pass)
            .await?
            .cleanup_local_shard(shard_id)
            .await
            .map_err(Into::into)
    }
}
