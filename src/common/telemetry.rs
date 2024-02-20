use std::sync::Arc;

use common::types::TelemetryDetail;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};
use storage::dispatcher::Dispatcher;
use uuid::Uuid;

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

    pub async fn prepare_data(&self, detail: TelemetryDetail) -> TelemetryData {
        TelemetryData {
            id: self.process_id.to_string(),
            collections: CollectionsTelemetry::collect(detail, self.dispatcher.toc()).await,
            app: AppBuildTelemetry::collect(detail, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(detail, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
                detail,
            ),
        }
    }
}
