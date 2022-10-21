use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use collection::telemetry::{CollectionShortInfoTelemetry, CollectionTelemetry};
use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use segment::telemetry::VectorIndexSearchesTelemetry;
use serde::{Deserialize, Serialize};
use storage::dispatcher::Dispatcher;
use storage::types::ClusterStatus;
use uuid::Uuid;

use crate::settings::Settings;

pub type HttpStatusCode = u16;

pub struct TelemetryCollector {
    process_id: Uuid,
    settings: Settings,
    dispatcher: Arc<Dispatcher>,
    pub actix_telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
    pub tonic_telemetry_collector: Arc<Mutex<TonicTelemetryCollector>>,
}

pub struct ActixTelemetryCollector {
    workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<String, HashMap<HttpStatusCode, Arc<Mutex<OperationDurationsAggregator>>>>,
}

pub struct TonicTelemetryCollector {
    workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }
}

impl TonicTelemetryCollector {
    pub fn create_grpc_telemetry_collector(&mut self) -> Arc<Mutex<TonicWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }
}

// Whole telemetry data
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TelemetryData {
    id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    app: Option<AppBuildTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    system: Option<RunningEnvironmentTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    configs: Option<ConfigsTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    collections_short_info: Option<CollectionShortInfoTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    collections: Option<Vec<CollectionTelemetry>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    web: Option<WebApiTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    grpc_calls_statistics: Option<GrpcTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    cluster_status: Option<ClusterStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    vector_index_searches: Option<VectorIndexSearchesTelemetry>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    peers_count: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct AppBuildTelemetry {
    version: String,
    debug: bool,
    web_feature: bool,
    service_debug_feature: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RunningEnvironmentTelemetry {
    distribution: Option<String>,
    distribution_version: Option<String>,
    is_docker: bool,
    // TODO(ivan) parse dockerenv file
    // docker_version: Option<String>,
    cores: Option<usize>,
    ram_size: Option<usize>,
    disk_size: Option<usize>,
    cpu_flags: String,
    // TODO(ivan) get locale and region
    // locale: Option<String>,
    // region: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ServiceConfigTelemetry {
    grpc_enable: bool,
    max_request_size_mb: usize,
    max_workers: Option<usize>,
    enable_cors: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct P2pConfigTelemetry {
    connection_pool_size: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ConsensusConfigTelemetry {
    max_message_queue_size: usize,
    tick_period_ms: u64,
    bootstrap_timeout_sec: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ClusterConfigTelemetry {
    enabled: bool,
    grpc_timeout_ms: u64,
    p2p: P2pConfigTelemetry,
    consensus: ConsensusConfigTelemetry,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct ConfigsTelemetry {
    service_config: ServiceConfigTelemetry,
    cluster_config: ClusterConfigTelemetry,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    responses: HashMap<String, HashMap<HttpStatusCode, OperationDurationStatistics>>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    responses: HashMap<String, OperationDurationStatistics>,
}

impl Anonymize for TelemetryData {
    fn anonymize(&self) -> Self {
        TelemetryData {
            id: self.id.clone(),
            app: self.app.anonymize(),
            system: self.system.anonymize(),
            configs: self.configs.anonymize(),
            collections_short_info: self.collections_short_info.anonymize(),
            collections: self.collections.anonymize(),
            web: self.web.anonymize(),
            grpc_calls_statistics: self.grpc_calls_statistics.anonymize(),
            cluster_status: self.cluster_status.anonymize(),
            vector_index_searches: self.vector_index_searches.anonymize(),
            peers_count: self.peers_count,
        }
    }
}

impl Anonymize for AppBuildTelemetry {
    fn anonymize(&self) -> Self {
        AppBuildTelemetry {
            version: self.version.clone(),
            debug: self.debug,
            web_feature: self.web_feature,
            service_debug_feature: self.service_debug_feature,
        }
    }
}

impl Anonymize for RunningEnvironmentTelemetry {
    fn anonymize(&self) -> Self {
        RunningEnvironmentTelemetry {
            distribution: self.distribution.clone(),
            distribution_version: self.distribution_version.clone(),
            is_docker: self.is_docker,
            cores: self.cores,
            ram_size: self.ram_size.anonymize(),
            disk_size: self.disk_size.anonymize(),
            cpu_flags: self.cpu_flags.clone(),
        }
    }
}

impl Anonymize for ConfigsTelemetry {
    fn anonymize(&self) -> Self {
        ConfigsTelemetry {
            service_config: self.service_config.anonymize(),
            cluster_config: self.cluster_config.anonymize(),
        }
    }
}

impl Anonymize for ServiceConfigTelemetry {
    fn anonymize(&self) -> Self {
        ServiceConfigTelemetry {
            grpc_enable: self.grpc_enable,
            max_request_size_mb: self.max_request_size_mb,
            max_workers: self.max_workers,
            enable_cors: self.enable_cors,
        }
    }
}

impl Anonymize for ClusterConfigTelemetry {
    fn anonymize(&self) -> Self {
        ClusterConfigTelemetry {
            enabled: self.enabled,
            grpc_timeout_ms: self.grpc_timeout_ms,
            p2p: self.p2p.anonymize(),
            consensus: self.consensus.anonymize(),
        }
    }
}

impl Anonymize for P2pConfigTelemetry {
    fn anonymize(&self) -> Self {
        P2pConfigTelemetry {
            connection_pool_size: self.connection_pool_size,
        }
    }
}

impl Anonymize for ConsensusConfigTelemetry {
    fn anonymize(&self) -> Self {
        ConsensusConfigTelemetry {
            max_message_queue_size: self.max_message_queue_size,
            tick_period_ms: self.tick_period_ms,
            bootstrap_timeout_sec: self.bootstrap_timeout_sec,
        }
    }
}

impl Anonymize for WebApiTelemetry {
    fn anonymize(&self) -> Self {
        WebApiTelemetry {
            responses: self.responses.clone(),
        }
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        GrpcTelemetry {
            responses: self.responses.clone(),
        }
    }
}

impl TonicWorkerTelemetryCollector {
    pub fn add_response(&mut self, method: String, instant: std::time::Instant) {
        let aggregator = self
            .methods
            .entry(method)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics());
        }
        GrpcTelemetry { responses }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        method: String,
        status_code: HttpStatusCode,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .methods
            .entry(method)
            .or_default()
            .entry(status_code)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (method, status_codes) in &self.methods {
            let mut status_codes_map = HashMap::new();
            for (status_code, aggregator) in status_codes {
                status_codes_map.insert(*status_code, aggregator.lock().get_statistics());
            }
            responses.insert(method.clone(), status_codes_map);
        }
        WebApiTelemetry { responses }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
        }
    }
}

impl WebApiTelemetry {
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (method, status_codes) in &other.responses {
            let status_codes_map = self.responses.entry(method.clone()).or_default();
            for (status_code, statistics) in status_codes {
                let entry = status_codes_map
                    .entry(*status_code)
                    .or_insert_with(OperationDurationStatistics::default);
                *entry = entry.clone() + statistics.clone();
            }
        }
    }
}

impl TelemetryCollector {
    pub fn new(settings: Settings, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            process_id: Uuid::new_v4(),
            settings,
            dispatcher,
            actix_telemetry_collector: Arc::new(Mutex::new(ActixTelemetryCollector {
                workers: Vec::new(),
            })),
            tonic_telemetry_collector: Arc::new(Mutex::new(TonicTelemetryCollector {
                workers: Vec::new(),
            })),
        }
    }

    pub async fn prepare_data(&self) -> TelemetryData {
        let collections = self.dispatcher.get_telemetry_data().await;
        let cluster_status = self.dispatcher.cluster_status();
        let grpc_calls_statistics = self.grpc_calls_statistics();
        let mut result = TelemetryData {
            id: self.process_id.to_string(),
            app: Some(self.get_app_data()),
            system: Some(self.get_system_data()),
            configs: Some(self.get_configs_data()),
            collections_short_info: None,
            collections: Some(collections),
            web: Some(self.get_web_data()),
            grpc_calls_statistics: Some(grpc_calls_statistics),
            cluster_status: Some(cluster_status),
            vector_index_searches: None,
            peers_count: None,
        };
        result.aggregate();
        result
    }

    fn get_app_data(&self) -> AppBuildTelemetry {
        AppBuildTelemetry {
            version: env!("CARGO_PKG_VERSION").to_string(),
            debug: cfg!(debug_assertions),
            web_feature: cfg!(feature = "web"),
            service_debug_feature: cfg!(feature = "service_debug"),
        }
    }

    fn get_system_data(&self) -> RunningEnvironmentTelemetry {
        let distribution = if let Ok(release) = sys_info::linux_os_release() {
            release.id
        } else {
            sys_info::os_type().ok()
        };
        let distribution_version = if let Ok(release) = sys_info::linux_os_release() {
            release.version_id
        } else {
            sys_info::os_release().ok()
        };
        let mut cpu_flags = String::new();
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::arch::is_x86_feature_detected!("sse") {
                cpu_flags += "sse,";
            }
            if std::arch::is_x86_feature_detected!("avx") {
                cpu_flags += "avx,";
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                cpu_flags += "avx2,";
            }
            if std::arch::is_x86_feature_detected!("fma") {
                cpu_flags += "fma,";
            }
            if std::arch::is_x86_feature_detected!("avx512f") {
                cpu_flags += "avx512f,";
            }
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                cpu_flags += "neon,";
            }
        }
        RunningEnvironmentTelemetry {
            distribution,
            distribution_version,
            is_docker: cfg!(unix) && Path::new("/.dockerenv").exists(),
            cores: sys_info::cpu_num().ok().map(|x| x as usize),
            ram_size: sys_info::mem_info().ok().map(|x| x.total as usize),
            disk_size: sys_info::disk_info().ok().map(|x| x.total as usize),
            cpu_flags,
        }
    }

    fn get_configs_data(&self) -> ConfigsTelemetry {
        let settings = self.settings.clone();
        ConfigsTelemetry {
            service_config: ServiceConfigTelemetry {
                grpc_enable: settings.service.grpc_port.is_some(),
                max_request_size_mb: settings.service.max_request_size_mb,
                max_workers: settings.service.max_workers,
                enable_cors: settings.service.enable_cors,
            },
            cluster_config: ClusterConfigTelemetry {
                enabled: settings.cluster.enabled,
                grpc_timeout_ms: settings.cluster.grpc_timeout_ms,
                p2p: P2pConfigTelemetry {
                    connection_pool_size: settings.cluster.p2p.connection_pool_size,
                },
                consensus: ConsensusConfigTelemetry {
                    max_message_queue_size: settings.cluster.consensus.max_message_queue_size,
                    tick_period_ms: settings.cluster.consensus.tick_period_ms,
                    bootstrap_timeout_sec: settings.cluster.consensus.bootstrap_timeout_sec,
                },
            },
        }
    }

    fn get_web_data(&self) -> WebApiTelemetry {
        let actix_telemetry_collector = self.actix_telemetry_collector.lock();
        let mut result = WebApiTelemetry::default();
        for web_data in &actix_telemetry_collector.workers {
            let lock = web_data.lock().get_telemetry_data();
            result.merge(&lock);
        }
        result
    }

    fn grpc_calls_statistics(&self) -> GrpcTelemetry {
        let tonic_telemetry_collector = self.tonic_telemetry_collector.lock();
        let mut result = GrpcTelemetry::default();
        for grpc_data in &tonic_telemetry_collector.workers {
            let lock = grpc_data.lock().get_telemetry_data();
            result.merge(&lock);
        }
        result
    }
}

impl TelemetryData {
    pub fn aggregate(&mut self) {
        let mut collections_short_info = Default::default();
        let mut vector_index_searches: VectorIndexSearchesTelemetry = Default::default();
        if let Some(collections) = &mut self.collections {
            for collection in collections.iter_mut() {
                let optimizations = collection.calculate_optimizations_from_shards();
                collection.optimizations = Some(optimizations.clone());
                collections_short_info = collections_short_info + collection.short_info.clone();

                let searches = collection.calculate_vector_index_searches_from_shards();
                collection.vector_index_searches = Some(vector_index_searches.clone());
                vector_index_searches = vector_index_searches + searches;
            }
        }
        self.collections_short_info = Some(collections_short_info);
        self.vector_index_searches = Some(vector_index_searches);
        if let Some(ClusterStatus::Enabled(status)) = &self.cluster_status {
            self.peers_count = Some(status.peers.len());
        }
    }

    pub fn cut_by_detail_level(&mut self, level: usize) {
        if level < 4 {
            self.collections
                .as_mut()
                .unwrap()
                .iter_mut()
                .for_each(|collection| {
                    collection.shards = None;
                });
            self.collections
                .as_mut()
                .unwrap()
                .iter_mut()
                .for_each(|collection| {
                    collection.config = None;
                });
        }

        if level < 3 {
            self.collections = None;
        }

        if level < 2 {
            self.cluster_status = None;
            self.vector_index_searches = None;
        }

        if level < 1 {
            self.configs = None;
            self.web = None;
            self.grpc_calls_statistics = None;
            self.peers_count = None;
            self.collections_short_info = None;
        }
    }
}
