use std::sync::Arc;
use std::time::Duration;

use ahash::HashSet;
use api::grpc;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::telemetry::CollectionTelemetry;
use common::types::{DetailsLevel, TelemetryDetail};
use itertools::Itertools;
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::Serialize;
use shard::common::stopping_guard::StoppingGuard;
use storage::content_manager::errors::{StorageError, StorageResult};
use storage::dispatcher::Dispatcher;
use storage::rbac::Auth;
use tokio::time::error::Elapsed;
use tokio_util::task::AbortOnDropHandle;
use tonic::Status;
use uuid::Uuid;

use crate::common::telemetry_ops::app_telemetry::{AppBuildTelemetry, AppBuildTelemetryCollector};
use crate::common::telemetry_ops::cluster_telemetry::ClusterTelemetry;
use crate::common::telemetry_ops::collections_telemetry::{
    CollectionTelemetryEnum, CollectionsTelemetry,
};
use crate::common::telemetry_ops::hardware::HardwareTelemetry;
use crate::common::telemetry_ops::memory_telemetry::MemoryTelemetry;
use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, RequestsTelemetry, TonicTelemetryCollector,
};
use crate::settings::Settings;

// Keep in sync with openapi/openapi-service.ytt.yaml
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
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app: Option<AppBuildTelemetry>,
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
        auth: &Auth,
        detail: TelemetryDetail,
        only_collections: Option<HashSet<String>>,
        timeout: Option<Duration>,
    ) -> StorageResult<TelemetryData> {
        // Do not log access to telemetry
        let access = auth.unlogged_access();

        let timeout = timeout.unwrap_or(DEFAULT_TELEMETRY_TIMEOUT);
        // Use blocking pool because the collection telemetry acquires several sync. locks.
        let is_stopped_guard = StoppingGuard::new();
        let is_stopped = is_stopped_guard.get_is_stopped();
        let collections_telemetry_handle = {
            let toc = self
                .dispatcher
                .toc(auth, &new_unchecked_verification_pass())
                .clone();
            let runtime_handle = toc.general_runtime_handle().clone();
            let access_collection = access.clone();

            let handle = runtime_handle.spawn_blocking(move || {
                // Re-enter the async runtime in this blocking thread
                tokio::runtime::Handle::current().block_on(async move {
                    CollectionsTelemetry::collect(
                        detail,
                        &access_collection,
                        only_collections,
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
            .map_err(|_: Elapsed| StorageError::timeout(timeout, "collections telemetry"))???;

        Ok(TelemetryData {
            id: self.process_id.to_string(),
            collections: collections_telemetry,
            app: Some(AppBuildTelemetry::collect(
                detail,
                &self.app_telemetry_collector,
                &self.settings,
            )),
            cluster: ClusterTelemetry::collect(auth, detail, &self.dispatcher, &self.settings),
            requests: RequestsTelemetry::collect(
                auth,
                &self.actix_telemetry_collector.lock(),
                &self.tonic_telemetry_collector.lock(),
                detail,
            ),
            memory: (detail.level > DetailsLevel::Level0)
                .then(|| MemoryTelemetry::collect(auth))
                .flatten(),
            hardware: (detail.level > DetailsLevel::Level0)
                .then(|| HardwareTelemetry::new(&self.dispatcher, access)),
        })
    }
}

impl TryFrom<grpc::PeerTelemetry> for TelemetryData {
    type Error = Status;

    fn try_from(value: grpc::PeerTelemetry) -> Result<Self, Self::Error> {
        let grpc::PeerTelemetry {
            app,
            collections,
            cluster,
        } = value;

        let app = app.map(AppBuildTelemetry::try_from).transpose()?;

        let collections = collections
            .into_values()
            .map(|collection| {
                Ok(CollectionTelemetryEnum::Full(Box::new(
                    CollectionTelemetry::try_from(collection)?,
                )))
            })
            .try_collect::<_, Vec<_>, Status>()?;

        let cluster = cluster.map(ClusterTelemetry::try_from).transpose()?;

        Ok(TelemetryData {
            id: "".to_string(),
            app,
            collections: CollectionsTelemetry {
                number_of_collections: collections.len(),
                max_collections: None,
                collections: Some(collections),
                snapshots: None,
            },
            cluster,
            requests: None,
            memory: None,
            hardware: None,
        })
    }
}

impl TryFrom<TelemetryData> for grpc::PeerTelemetry {
    type Error = Status;

    fn try_from(telemetry_data: TelemetryData) -> Result<Self, Self::Error> {
        let TelemetryData {
            id: _,
            app,
            collections,
            cluster,
            requests: _,
            memory: _,
            hardware: _,
        } = telemetry_data;

        let app = app.map(grpc::AppTelemetry::from);

        let collections = collections
            .collections
            .into_iter()
            .flatten()
            .map(|telemetry_enum| {
                match telemetry_enum {
                    CollectionTelemetryEnum::Full(collection_telemetry) => {
                        let telemetry = grpc::CollectionTelemetry::from(*collection_telemetry);
                        let collection_name = telemetry.id.clone();
                        Ok((collection_name, telemetry))
                    }
                    // This only happens when details_level is < 2, which we explicitly fail
                    CollectionTelemetryEnum::Aggregated(_) => Err(Status::invalid_argument("Expected CollectionTelemetryEnum::Full variant, got CollectionTelemetryEnum::Aggregated")),
                }
            })
            .try_collect()?;

        Ok(grpc::PeerTelemetry {
            app,
            collections,
            cluster: cluster.map(grpc::ClusterTelemetry::from),
        })
    }
}
