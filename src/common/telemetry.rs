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
    pub(crate) app: AppBuildTelemetry,
    pub(crate) collections: CollectionsTelemetry,
    pub(crate) cluster: ClusterTelemetry,
    pub(crate) requests: RequestsTelemetry,
}

impl Anonymize for TelemetryData {
    fn anonymize(&self) -> Self {
        TelemetryData {
            id: self.id.clone(),
            app: self.app.anonymize(),
            collections: self.collections.anonymize(),
            cluster: self.cluster.anonymize(),
            requests: self.requests.anonymize(),
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
        let collections = CollectionsTelemetry::collect(level, self.dispatcher.toc()).await;

        TelemetryData {
            id: self.process_id.to_string(),
            collections,
            app: AppBuildTelemetry::collect(level, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(level, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
                &Vec::new(),
            ),
        }
    }

    /// Get [`TelemetryData`] for the given collection_names
    /// # Important
    /// If the list of collection_names is empty, the function will return data for all
    /// collections.
    pub async fn prepare_data_for(
        &self,
        level: usize,
        collection_names: &Vec<String>,
    ) -> Result<TelemetryData, StorageError> {
        let collections = {
            if collection_names.is_empty() {
                CollectionsTelemetry::collect(level, self.dispatcher.toc()).await
            } else {
                CollectionsTelemetry::collect_for(level, self.dispatcher.toc(), collection_names)
                    .await?
            }
        };

        Ok(TelemetryData {
            id: self.process_id.to_string(),
            collections,
            app: AppBuildTelemetry::collect(level, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(level, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
                collection_names,
            ),
        })
    }
}
