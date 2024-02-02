use std::sync::Arc;

use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use uuid::Uuid;

use super::telemetry_ops::collections_telemetry::CollectionTelemetryEnum;
use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppBuildTelemetryCollector};
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;
use crate::common::telemetry_ops::collections_telemetry::CollectionsTelemetry;
use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, RequestsTelemetry, TonicTelemetryCollector,
};
use crate::settings::Settings;

pub struct TelemetryCollector {
    process_id: Uuid,
    settings: Settings,
    dispatcher: Arc<Dispatcher>,
    pub app_telemetry_collector: AppBuildTelemetryCollector,
    pub actix_telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
    pub tonic_telemetry_collector: Arc<Mutex<TonicTelemetryCollector>>,
}

// Whole telemetry data
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TelemetryData {
    id: String,
    // filters of collection names
    pub(crate) filters: Vec<String>,
    pub(crate) app: AppBuildTelemetry,
    pub(crate) collections: TelemetryDataCollectionType,
    pub(crate) cluster: ClusterTelemetry,
    pub(crate) requests: RequestsTelemetry,
}

// If the [`TelemetryData`] is about one or more collections
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub enum TelemetryDataCollectionType {
    Single(CollectionTelemetryEnum),
    Multiple(CollectionsTelemetry),
}

impl Anonymize for TelemetryData {
    fn anonymize(&self) -> Self {
        TelemetryData {
            id: self.id.clone(),
            filters: self.filters.clone(),
            app: self.app.anonymize(),
            collections: self.collections.anonymize(),
            cluster: self.cluster.anonymize(),
            requests: self.requests.anonymize(),
        }
    }
}

impl Anonymize for TelemetryDataCollectionType {
    fn anonymize(&self) -> Self {
        match self {
            Self::Single(v) => Self::Single(v.anonymize()),
            Self::Multiple(v) => Self::Multiple(v.anonymize()),
        }
    }
}

impl TelemetryCollector {
    pub fn reporting_id(&self) -> String {
        self.process_id.to_string()
    }

    pub fn generate_id() -> Uuid {
        Uuid::new_v4()
    }

    pub fn new(settings: Settings, dispatcher: Arc<Dispatcher>, id: Uuid) -> Self {
        Self {
            process_id: id,
            settings,
            dispatcher,
            app_telemetry_collector: AppBuildTelemetryCollector::new(),
            actix_telemetry_collector: Arc::new(Mutex::new(ActixTelemetryCollector {
                workers: Vec::new(),
            })),
            tonic_telemetry_collector: Arc::new(Mutex::new(TonicTelemetryCollector {
                workers: Vec::new(),
            })),
        }
    }

    pub async fn prepare_data(&self, level: usize) -> TelemetryData {
        let collections = TelemetryDataCollectionType::Multiple(
            CollectionsTelemetry::collect(level, self.dispatcher.toc()).await,
        );

        TelemetryData {
            id: self.process_id.to_string(),
            filters: Vec::new(),
            collections,
            app: AppBuildTelemetry::collect(level, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(level, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
            ),
        }
    }

    pub async fn prepare_data_for(
        &self,
        level: usize,
        collection_name: String,
    ) -> Result<TelemetryData, StorageError> {
        let collection = self
            .dispatcher
            .toc()
            .get_collection(&collection_name)
            .await?;
        let telemetry = collection.get_telemetry_data().await;
        let collections = TelemetryDataCollectionType::Single(if level > 1 {
            CollectionTelemetryEnum::Full(telemetry)
        } else {
            CollectionTelemetryEnum::Aggregated(telemetry.into())
        });

        Ok(TelemetryData {
            id: self.process_id.to_string(),
            filters: vec![collection_name],
            collections,
            app: AppBuildTelemetry::collect(level, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(level, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
            ),
        })
    }
}
