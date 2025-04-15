use collection::telemetry::{CollectionTelemetry, CollectionsAggregatedTelemetry};
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
#[serde(untagged)]
pub enum CollectionTelemetryEnum {
    Full(CollectionTelemetry),
    Aggregated(CollectionsAggregatedTelemetry),
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct CollectionsTelemetry {
    #[anonymize(false)]
    pub number_of_collections: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<Vec<CollectionTelemetryEnum>>,
}

impl CollectionsTelemetry {
    pub async fn collect(detail: TelemetryDetail, access: &Access, toc: &TableOfContent) -> Self {
        let number_of_collections = toc.all_collections(access).await.len();
        let collections = if detail.level >= DetailsLevel::Level1 {
            let telemetry_data = if detail.level >= DetailsLevel::Level2 {
                toc.get_telemetry_data(detail, access)
                    .await
                    .into_iter()
                    .map(CollectionTelemetryEnum::Full)
                    .collect()
            } else {
                toc.get_aggregated_telemetry_data(access)
                    .await
                    .into_iter()
                    .map(CollectionTelemetryEnum::Aggregated)
                    .collect()
            };

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
