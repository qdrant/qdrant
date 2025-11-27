use std::sync::atomic::AtomicBool;
use std::time::Duration;

use collection::operations::types::CollectionResult;
use collection::telemetry::{
    CollectionSnapshotTelemetry, CollectionTelemetry, CollectionsAggregatedTelemetry,
};
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
#[serde(untagged)]
pub enum CollectionTelemetryEnum {
    Full(Box<CollectionTelemetry>),
    Aggregated(CollectionsAggregatedTelemetry),
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize, Default)]
pub struct CollectionsTelemetry {
    #[anonymize(false)]
    pub number_of_collections: usize,
    #[anonymize(false)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_collections: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<CollectionTelemetryEnum>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<CollectionSnapshotTelemetry>>,
}

impl CollectionsTelemetry {
    pub async fn collect(
        detail: TelemetryDetail,
        access: &Access,
        toc: &TableOfContent,
        timeout: Duration,
        is_stopped: &AtomicBool,
    ) -> CollectionResult<Self> {
        let number_of_collections = toc.all_collections(access).await.len();
        let (collections, snapshots) = if detail.level >= DetailsLevel::Level1 {
            let telemetry_data = if detail.level >= DetailsLevel::Level2 {
                let toc_telemetry = toc
                    .get_telemetry_data(detail, access, timeout, is_stopped)
                    .await?;

                let collections: Vec<_> = toc_telemetry
                    .collection_telemetry
                    .into_iter()
                    .map(|t| CollectionTelemetryEnum::Full(Box::new(t)))
                    .collect();

                (collections, toc_telemetry.snapshot_telemetry)
            } else {
                let collections = toc
                    .get_aggregated_telemetry_data(access, timeout, is_stopped)
                    .await?
                    .into_iter()
                    .map(CollectionTelemetryEnum::Aggregated)
                    .collect();
                (collections, vec![])
            };

            (Some(telemetry_data.0), Some(telemetry_data.1))
        } else {
            (None, None)
        };

        let max_collections = toc.max_collections();

        Ok(CollectionsTelemetry {
            number_of_collections,
            max_collections,
            collections,
            snapshots,
        })
    }
}
