use collection::config::CollectionParams;
use collection::operations::types::OptimizersStatus;
use collection::telemetry::CollectionTelemetry;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::content_manager::toc::TableOfContent;

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct CollectionsAggregatedTelemetry {
    pub vectors: usize,
    pub optimizers_status: OptimizersStatus,
    pub params: CollectionParams,
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum CollectionTelemetryEnum {
    Full(CollectionTelemetry),
    Aggregated(CollectionsAggregatedTelemetry),
}

#[derive(Serialize, Clone, Debug, JsonSchema)]
pub struct CollectionsTelemetry {
    pub number_of_collections: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<CollectionTelemetryEnum>>,
}

impl From<CollectionTelemetry> for CollectionsAggregatedTelemetry {
    fn from(telemetry: CollectionTelemetry) -> Self {
        let optimizers_status = telemetry
            .shards
            .iter()
            .flat_map(|shard| shard.local.as_ref().map(|x| x.optimizations.status.clone()))
            .max()
            .unwrap_or(OptimizersStatus::Ok);

        CollectionsAggregatedTelemetry {
            vectors: telemetry.count_vectors(),
            optimizers_status,
            params: telemetry.config.params,
        }
    }
}

impl CollectionsTelemetry {
    pub async fn collect(detail: TelemetryDetail, toc: &TableOfContent) -> Self {
        let number_of_collections = toc.all_collections().await.len();
        let collections = if detail.level >= DetailsLevel::Level1 {
            let telemetry_data = toc
                .get_telemetry_data(detail)
                .await
                .into_iter()
                .map(|telemetry| {
                    if detail.level >= DetailsLevel::Level2 {
                        CollectionTelemetryEnum::Full(telemetry)
                    } else {
                        CollectionTelemetryEnum::Aggregated(telemetry.into())
                    }
                })
                .collect();

            Some(telemetry_data)
        } else {
            None
        };

        CollectionsTelemetry {
            number_of_collections,
            collections,
        }
    }
}

impl Anonymize for CollectionsTelemetry {
    fn anonymize(&self) -> Self {
        CollectionsTelemetry {
            number_of_collections: self.number_of_collections,
            collections: self.collections.anonymize(),
        }
    }
}

impl Anonymize for CollectionTelemetryEnum {
    fn anonymize(&self) -> Self {
        match self {
            CollectionTelemetryEnum::Full(telemetry) => {
                CollectionTelemetryEnum::Full(telemetry.anonymize())
            }
            CollectionTelemetryEnum::Aggregated(telemetry) => {
                CollectionTelemetryEnum::Aggregated(telemetry.anonymize())
            }
        }
    }
}

impl Anonymize for CollectionsAggregatedTelemetry {
    fn anonymize(&self) -> Self {
        CollectionsAggregatedTelemetry {
            optimizers_status: self.optimizers_status.clone(),
            vectors: self.vectors.anonymize(),
            params: self.params.anonymize(),
        }
    }
}
