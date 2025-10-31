use collection::telemetry::{CollectionTelemetry, CollectionsAggregatedTelemetry};
use common::types::TelemetryDetail;

use crate::content_manager::toc::TableOfContent;
use crate::rbac::Access;

impl TableOfContent {
    pub async fn get_telemetry_data(
        &self,
        detail: TelemetryDetail,
        access: &Access,
    ) -> Vec<CollectionTelemetry> {
        let mut result = Vec::new();
        let all_collections = self.all_collections_access(access).await;
        for collection_pass in &all_collections {
            if let Ok(collection) = self.get_collection(collection_pass).await {
                result.push(collection.get_telemetry_data(detail).await);
            }
        }
        result
    }

    pub async fn get_aggregated_telemetry_data(
        &self,
        access: &Access,
    ) -> Vec<CollectionsAggregatedTelemetry> {
        let mut result = Vec::new();
        let all_collections = self.all_collections_access(access).await;
        for collection_pass in &all_collections {
            if let Ok(collection) = self.get_collection(collection_pass).await {
                result.push(collection.get_aggregated_telemetry_data().await);
            }
        }
        result
    }

    pub fn max_collections(&self) -> Option<usize> {
        self.storage_config.max_collections
    }
}
