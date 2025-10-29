use std::path::Path;

use chrono::{DateTime, SubsecRound, Utc};
use common::flags::FeatureFlags;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::HnswGlobalConfig;
use serde::Serialize;

use crate::settings::Settings;

pub struct AppBuildTelemetryCollector {
    pub startup: DateTime<Utc>,
}

impl AppBuildTelemetryCollector {
    pub fn new() -> Self {
        AppBuildTelemetryCollector {
            startup: Utc::now().round_subsecs(2),
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct AppFeaturesTelemetry {
    pub debug: bool,
    pub service_debug_feature: bool,
    pub recovery_mode: bool,
    pub gpu: bool,
    pub rocksdb: bool,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct RunningEnvironmentTelemetry {
    #[anonymize(false)]
    distribution: Option<String>,
    #[anonymize(false)]
    distribution_version: Option<String>,
    is_docker: bool,
    #[anonymize(false)]
    cores: Option<usize>,
    ram_size: Option<usize>,
    disk_size: Option<usize>,
    #[anonymize(false)]
    cpu_flags: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_endian: Option<CpuEndian>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gpu_devices: Option<Vec<GpuDeviceTelemetry>>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct AppBuildTelemetry {
    #[anonymize(false)]
    pub name: String,
    #[anonymize(false)]
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub features: Option<AppFeaturesTelemetry>,
    #[anonymize(value = None)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_features: Option<FeatureFlags>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_global_config: Option<HnswGlobalConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<RunningEnvironmentTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_rbac: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hide_jwt_dashboard: Option<bool>,
    pub startup: DateTime<Utc>,
}

impl AppBuildTelemetry {
    pub fn collect(
        detail: TelemetryDetail,
        collector: &AppBuildTelemetryCollector,
        settings: &Settings,
    ) -> Self {
        AppBuildTelemetry {
            name: env!("CARGO_PKG_NAME").to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            features: (detail.level >= DetailsLevel::Level1).then(|| AppFeaturesTelemetry {
                debug: cfg!(debug_assertions),
                service_debug_feature: cfg!(feature = "service_debug"),
                recovery_mode: settings.storage.recovery_mode.is_some(),
                gpu: cfg!(feature = "gpu"),
                rocksdb: cfg!(feature = "rocksdb"),
            }),
            runtime_features: (detail.level >= DetailsLevel::Level1)
                .then(common::flags::feature_flags),
            hnsw_global_config: (detail.level >= DetailsLevel::Level1)
                .then(|| settings.storage.hnsw_global_config.clone()),
            system: (detail.level >= DetailsLevel::Level1).then(get_system_data),
            jwt_rbac: settings.service.jwt_rbac,
            hide_jwt_dashboard: settings.service.hide_jwt_dashboard,
            startup: collector.startup,
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
    let mut cpu_flags = vec![];
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("sse") {
            cpu_flags.push("sse");
        }
        if std::arch::is_x86_feature_detected!("sse2") {
            cpu_flags.push("sse2");
        }
        if std::arch::is_x86_feature_detected!("avx") {
            cpu_flags.push("avx");
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            cpu_flags.push("avx2");
        }
        if std::arch::is_x86_feature_detected!("fma") {
            cpu_flags.push("fma");
        }
        if std::arch::is_x86_feature_detected!("f16c") {
            cpu_flags.push("f16c");
        }
        if std::arch::is_x86_feature_detected!("avx512f") {
            cpu_flags.push("avx512f");
        }
        if std::arch::is_x86_feature_detected!("avx512vl") {
            cpu_flags.push("avx512vl");
        }
        if std::arch::is_x86_feature_detected!("avx512vpopcntdq") {
            cpu_flags.push("avx512vpopcntdq");
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            cpu_flags.push("neon");
        }
        if std::arch::is_aarch64_feature_detected!("fp16") {
            cpu_flags.push("fp16");
        }
    }

    #[cfg(feature = "gpu")]
    let gpu_devices = segment::index::hnsw_index::gpu::GPU_DEVICES_MANAGER
        .read()
        .as_ref()
        .map(|gpu_devices_manager| {
            gpu_devices_manager
                .all_found_device_names()
                .iter()
                .map(|name| GpuDeviceTelemetry { name: name.clone() })
                .collect::<Vec<_>>()
        });

    #[cfg(not(feature = "gpu"))]
    let gpu_devices = None;

    RunningEnvironmentTelemetry {
        distribution,
        distribution_version,
        is_docker: cfg!(unix) && Path::new("/.dockerenv").exists(),
        cores: sys_info::cpu_num().ok().map(|x| x as usize),
        ram_size: sys_info::mem_info().ok().map(|x| x.total as usize),
        disk_size: sys_info::disk_info().ok().map(|x| x.total as usize),
        cpu_flags: cpu_flags.join(","),
        cpu_endian: Some(CpuEndian::current()),
        gpu_devices,
    }
}

#[derive(Serialize, Clone, Copy, Debug, JsonSchema, Anonymize)]
#[serde(rename_all = "snake_case")]
pub enum CpuEndian {
    Little,
    Big,
    Other,
}

impl CpuEndian {
    /// Get the current used byte order
    pub const fn current() -> Self {
        if cfg!(target_endian = "little") {
            CpuEndian::Little
        } else if cfg!(target_endian = "big") {
            CpuEndian::Big
        } else {
            CpuEndian::Other
        }
    }
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct GpuDeviceTelemetry {
    #[anonymize(false)]
    pub name: String,
}
