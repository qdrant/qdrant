use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use collection::telemetry::CollectionTelemetry;
use schemars::JsonSchema;
use segment::telemetry::Anonymize;
use serde::{Deserialize, Serialize};
use storage::dispatcher::Dispatcher;
use storage::types::ClusterStatus;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::settings::Settings;

pub type HttpStatusCode = u16;

pub struct TelemetryCollector {
    process_id: Uuid,
    settings: Settings,
    dispatcher: Arc<Dispatcher>,
    web_workers_telemetry: Vec<Arc<Mutex<WebApiTelemetry>>>,
}

// Whole telemtry data
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TelemetryData {
    id: String,
    app: AppBuildTelemetry,
    system: RunningEnvironmentTelemetry,
    configs: ConfigsTelemetry,
    collections: Vec<CollectionTelemetry>,
    web: WebApiTelemetry,
    cluster_status: ClusterStatus,
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
    responses: HashMap<HttpStatusCode, usize>,
}

impl Anonymize for TelemetryData {
    fn anonymize(&self) -> Self {
        TelemetryData {
            id: self.id.clone(),
            app: self.app.anonymize(),
            system: self.system.anonymize(),
            configs: self.configs.anonymize(),
            collections: self
                .collections
                .iter()
                .map(|collection| collection.anonymize())
                .collect(),
            web: self.web.anonymize(),
            cluster_status: self.cluster_status.anonymize(),
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
            ram_size: self.ram_size,
            disk_size: self.disk_size,
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

impl WebApiTelemetry {
    pub fn add_response(&mut self, status_code: HttpStatusCode) {
        *self.responses.entry(status_code).or_insert(0) += 1;
    }

    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (status_code, count) in &other.responses {
            *self.responses.entry(*status_code).or_insert(0) += *count;
        }
    }
}

impl TelemetryCollector {
    pub fn new(settings: Settings, dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            process_id: Uuid::new_v4(),
            settings,
            dispatcher,
            web_workers_telemetry: Vec::new(),
        }
    }

    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<WebApiTelemetry>> {
        let web_worker_telemetry = Arc::new(Mutex::new(WebApiTelemetry::default()));
        self.web_workers_telemetry
            .push(web_worker_telemetry.clone());
        web_worker_telemetry
    }

    pub async fn prepare_data(&self) -> TelemetryData {
        let collections = self.dispatcher.get_telemetry_data().await;
        let cluster_status = self.dispatcher.cluster_status();
        TelemetryData {
            id: self.process_id.to_string(),
            app: self.get_app_data(),
            system: self.get_system_data(),
            configs: self.get_configs_data(),
            collections,
            web: self.get_web_data().await,
            cluster_status,
        }
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

    async fn get_web_data(&self) -> WebApiTelemetry {
        let mut result = WebApiTelemetry::default();
        for web_data in &self.web_workers_telemetry {
            let lock = web_data.lock().await;
            result.merge(&lock);
        }
        result
    }
}
