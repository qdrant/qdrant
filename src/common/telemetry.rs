use std::sync::Arc;
use std::time::Duration;

use collection::operations::verification::new_unchecked_verification_pass;
use common::types::{DetailsLevel, TelemetryDetail};
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use shard::common::stopping_guard::StoppingGuard;
use storage::content_manager::errors::{StorageError, StorageResult};
use storage::dispatcher::Dispatcher;
use storage::rbac::Access;
use tokio::time::error::Elapsed;
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppBuildTelemetryCollector};
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;
use crate::common::telemetry_ops::collections_telemetry::CollectionsTelemetry;
use crate::common::telemetry_ops::hardware::HardwareTelemetry;
use crate::common::telemetry_ops::memory_telemetry::MemoryTelemetry;
use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, RequestsTelemetry, TonicTelemetryCollector,
};
use crate::settings::Settings;

const DEFAULT_TELEMETRY_TIMEOUT: Duration = Duration::from_secs(60);

pub struct TelemetryCollector {
    process_id: Uuid,
    settings: Settings,
    dispatcher: Arc<Dispatcher>,
    pub app_telemetry_collector: AppBuildTelemetryCollector,
    pub actix_telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
    pub tonic_telemetry_collector: Arc<Mutex<TonicTelemetryCollector>>,
}

// Whole telemetry data
#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct TelemetryData {
    #[anonymize(false)]
    id: String,
    pub(crate) app: AppBuildTelemetry,
    pub(crate) collections: CollectionsTelemetry,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cluster: Option<ClusterTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) requests: Option<RequestsTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memory: Option<MemoryTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hardware: Option<HardwareTelemetry>,
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

    pub async fn prepare_data(
        &self,
        access: &Access,
        detail: TelemetryDetail,
        timeout: Option<Duration>,
    ) -> StorageResult<TelemetryData> {
        let timeout = timeout.unwrap_or(DEFAULT_TELEMETRY_TIMEOUT);
        // Use blocking pool because the collection telemetry acquires several sync. locks.
        let is_stopped_guard = StoppingGuard::new();
        let is_stopped = is_stopped_guard.get_is_stopped();
        let collections_telemetry_handle = {
            let toc = self
                .dispatcher
                .toc(access, &new_unchecked_verification_pass())
                .clone();
            let runtime_handle = toc.general_runtime_handle().clone();
            let access_collection = access.clone();

            let handle = runtime_handle.spawn_blocking(move || {
                // Re-enter the async runtime in this blocking thread
                tokio::runtime::Handle::current().block_on(async move {
                    CollectionsTelemetry::collect(
                        detail,
                        &access_collection,
                        &toc,
                        timeout,
                        &is_stopped,
                    )
                    .await
                })
            });
            AbortOnDropHandle::new(handle)
        };

        let collections_telemetry = tokio::time::timeout(timeout, collections_telemetry_handle)
            .await
            .map_err(|_: Elapsed| {
                StorageError::timeout(timeout.as_secs() as usize, "collections telemetry")
            })???;

        Ok(TelemetryData {
            id: self.process_id.to_string(),
            collections: collections_telemetry,
            app: AppBuildTelemetry::collect(detail, &self.app_telemetry_collector, &self.settings),
            cluster: ClusterTelemetry::collect(access, detail, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                access,
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
                detail,
            ),
            memory: (detail.level > DetailsLevel::Level0)
                .then(|| MemoryTelemetry::collect(access))
                .flatten(),
            hardware: (detail.level > DetailsLevel::Level0)
                .then(|| HardwareTelemetry::new(&self.dispatcher, access)),
        })
    }
}
