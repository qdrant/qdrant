use serde::Serialize;
use std::path::Path;
use uuid::Uuid;

use crate::settings::Settings;

pub struct UserTelemetryCollector {
    process_id: Uuid,
    settings: Option<Settings>,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryApp {
    version: String,
    debug: bool,
    web_feature: bool,
    service_debug_feature: bool,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetrySystem {
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

#[derive(Serialize, Clone)]
pub struct UserTelemetryServiceConfig {
    grpc_enable: bool,
    max_request_size_mb: usize,
    max_workers: Option<usize>,
    enable_cors: bool,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryP2pConfig {
    connection_pool_size: usize,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryConsensusConfig {
    max_message_queue_size: usize,
    tick_period_ms: u64,
    bootstrap_timeout_sec: u64,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryClusterConfig {
    enabled: bool,
    grpc_timeout_ms: u64,
    p2p: UserTelemetryP2pConfig,
    consensus: UserTelemetryConsensusConfig,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryConfigs {
    service_config: UserTelemetryServiceConfig,
    cluster_config: UserTelemetryClusterConfig,
}

#[derive(Serialize, Clone)]
pub struct UserTelemetryData {
    id: String,
    app: UserTelemetryApp,
    system: UserTelemetrySystem,
    configs: UserTelemetryConfigs,
}

impl UserTelemetryCollector {
    pub fn new() -> Self {
        Self {
            process_id: Uuid::new_v4(),
            settings: None,
        }
    }

    pub fn put_settings(&mut self, settings: Settings) {
        self.settings = Some(settings);
    }

    #[allow(dead_code)]
    pub fn prepare_data(&self) -> UserTelemetryData {
        UserTelemetryData {
            id: self.process_id.to_string(),
            app: self.get_app_data(),
            system: self.get_system_data(),
            configs: self.get_configs_data(),
        }
    }

    fn get_app_data(&self) -> UserTelemetryApp {
        UserTelemetryApp {
            version: env!("CARGO_PKG_VERSION").to_string(),
            debug: cfg!(debug_assertions),
            web_feature: cfg!(feature = "web"),
            service_debug_feature: cfg!(feature = "service_debug"),
        }
    }

    fn get_system_data(&self) -> UserTelemetrySystem {
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
        UserTelemetrySystem {
            distribution,
            distribution_version,
            is_docker: cfg!(unix) && Path::new("/.dockerenv").exists(),
            cores: sys_info::cpu_num().ok().map(|x| x as usize),
            ram_size: sys_info::mem_info().ok().map(|x| x.total as usize),
            disk_size: sys_info::disk_info().ok().map(|x| x.total as usize),
            cpu_flags,
        }
    }

    fn get_configs_data(&self) -> UserTelemetryConfigs {
        let settings = self
            .settings
            .clone()
            .expect("User settings have been not provided");
        UserTelemetryConfigs {
            service_config: UserTelemetryServiceConfig {
                grpc_enable: settings.service.grpc_port.is_some(),
                max_request_size_mb: settings.service.max_request_size_mb,
                max_workers: settings.service.max_workers,
                enable_cors: settings.service.enable_cors,
            },
            cluster_config: UserTelemetryClusterConfig {
                enabled: settings.cluster.enabled,
                grpc_timeout_ms: settings.cluster.grpc_timeout_ms,
                p2p: UserTelemetryP2pConfig {
                    connection_pool_size: settings.cluster.p2p.connection_pool_size,
                },
                consensus: UserTelemetryConsensusConfig {
                    max_message_queue_size: settings.cluster.consensus.max_message_queue_size,
                    tick_period_ms: settings.cluster.consensus.tick_period_ms,
                    bootstrap_timeout_sec: settings.cluster.consensus.bootstrap_timeout_sec,
                },
            },
        }
    }
}
