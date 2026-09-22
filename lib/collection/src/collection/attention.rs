use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::attention::{AttentionRequest, AttentionResponse};
use segment::types::{Filter, SearchParams, StrictModeConfig};
use validator::Validate;

use super::Collection;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::verification::{StrictModeVerification, check_limit_opt};

impl StrictModeVerification for AttentionRequest {
    fn query_limit(&self) -> Option<usize> {
        Some(self.return_top_k)
    }
    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }
    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }
    fn request_exact(&self) -> Option<bool> {
        Some(false)
    }
    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
    async fn check_custom(
        &self,
        _: &Collection,
        config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        check_limit_opt(Some(self.ef), config.search_max_hnsw_ef, "ef")
    }
}

impl Collection {
    /// Prototype: a complete answer is computed on exactly one local populated segment.
    pub async fn attention(
        &self,
        requests: Vec<AttentionRequest>,
        timeout: Option<Duration>,
        hw: HwMeasurementAcc,
    ) -> CollectionResult<Vec<AttentionResponse>> {
        if requests.is_empty() || requests.len() > 64 {
            return Err(CollectionError::bad_request(
                "attention batch must contain 1..=64 queries",
            ));
        }
        for request in &requests {
            request
                .validate()
                .map_err(|err| CollectionError::bad_request(err.to_string()))?;
        }
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let operation = async {
            let shards = self.shards_holder.read().await;
            if shards.len() != 1 {
                return Err(CollectionError::bad_request(
                    "attention requires exactly one shard",
                ));
            }
            shards
                .all_shards()
                .next()
                .expect("checked one shard")
                .attention(requests, hw)
                .await
        };
        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| CollectionError::timeout(timeout, "attention"))?
    }
}
