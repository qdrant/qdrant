use std::path::Path;

use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct AppFeaturesTelemetry {
    debug: bool,
    web_feature: bool,
    service_debug_feature: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RunningEnvironmentTelemetry {
    distribution: Option<String>,
    distribution_version: Option<String>,
    is_docker: bool,
    cores: Option<usize>,
    ram_size: Option<usize>,
    disk_size: Option<usize>,
    cpu_flags: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct AppBuildTelemetry {
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub features: Option<AppFeaturesTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub system: Option<RunningEnvironmentTelemetry>,
}

impl AppBuildTelemetry {
    pub fn collect(level: usize) -> Self {
        AppBuildTelemetry {
            version: env!("CARGO_PKG_VERSION").to_string(),
            features: if level > 0 {
                Some(AppFeaturesTelemetry {
                    debug: cfg!(debug_assertions),
                    web_feature: cfg!(feature = "web"),
                    service_debug_feature: cfg!(feature = "service_debug"),
                })
            } else {
                None
            },
            system: if level > 0 {
                Some(get_system_data())
            } else {
                None
            },
        }
    }
}

fn get_system_data() -> RunningEnvironmentTelemetry {
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

impl Anonymize for AppFeaturesTelemetry {
    fn anonymize(&self) -> Self {
        AppFeaturesTelemetry {
            debug: self.debug,
            web_feature: self.web_feature,
            service_debug_feature: self.service_debug_feature,
        }
    }
}

impl Anonymize for AppBuildTelemetry {
    fn anonymize(&self) -> Self {
        AppBuildTelemetry {
            version: self.version.clone(),
            features: self.features.anonymize(),
            system: self.system.anonymize(),
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
