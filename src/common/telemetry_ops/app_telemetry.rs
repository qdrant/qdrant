use std::path::Path;
use std::sync::LazyLock;

use chrono::{DateTime, SubsecRound, Utc};
use common::flags::FeatureFlags;
use common::low_memory::LowMemoryMode;
use common::types::{DetailsLevel, TelemetryDetail};
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::types::HnswGlobalConfig;
use serde::Serialize;

use crate::common::audit::{AuditConfig, AuditRotation};
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
    pub staging: bool,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct RunningEnvironmentTelemetry {
    #[anonymize(false)]
    distribution: Option<String>,
    #[anonymize(false)]
    distribution_version: Option<String>,
    is_docker: bool,
    /// Container runtime Qdrant runs under (detects Kubernetes etc., unlike `is_docker`).
    #[anonymize(false)]
    container_runtime: ContainerRuntime,
    // Number of CPU cores Qdrant will use (accounting for cgroup/host limits)
    #[anonymize(false)]
    cores: Option<usize>,
    /// Average number of CPU cores used by this process over roughly the last
    /// two seconds. `None` on unsupported platforms, before two samples are
    /// collected, or on transient failures reading process CPU time.
    #[anonymize(false)]
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_cores_used: Option<f32>,
    /// Effective total memory for this process in KiB (cgroup limit or host RAM).
    ram_size: Option<usize>,
    /// Size in KiB of the filesystem hosting Qdrant's /storage path (if not available, fallback to host disk size)
    disk_size: Option<usize>,
    #[anonymize(false)]
    cpu_flags: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_endian: Option<CpuEndian>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gpu_devices: Option<Vec<GpuDeviceTelemetry>>,
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct AuditTelemetry {
    #[anonymize(false)]
    pub dir: String,
    #[anonymize(false)]
    pub rotation: String,
    pub max_log_files: usize,
    pub trust_forwarded_headers: bool,
    pub log_api: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dir_size_bytes: Option<usize>,
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
    #[anonymize(value = None)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low_memory_mode: Option<LowMemoryMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_global_config: Option<HnswGlobalConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<RunningEnvironmentTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt_rbac: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hide_jwt_dashboard: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit: Option<AuditTelemetry>,
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
                rocksdb: false,
                staging: cfg!(feature = "staging"),
            }),
            runtime_features: (detail.level >= DetailsLevel::Level1)
                .then(common::flags::feature_flags),
            low_memory_mode: (detail.level >= DetailsLevel::Level1)
                .then(common::low_memory::low_memory_mode),
            hnsw_global_config: (detail.level >= DetailsLevel::Level1)
                .then(|| settings.storage.hnsw_global_config.clone()),
            system: (detail.level >= DetailsLevel::Level1)
                .then(|| get_system_data(&settings.storage.storage_path)),
            jwt_rbac: settings.service.jwt_rbac,
            hide_jwt_dashboard: settings.service.hide_jwt_dashboard,
            audit: collect_audit_telemetry(settings.audit.as_ref(), detail),
            startup: collector.startup,
        }
    }
}

fn collect_audit_telemetry(
    audit: Option<&AuditConfig>,
    detail: TelemetryDetail,
) -> Option<AuditTelemetry> {
    let config = audit.filter(|c| c.enabled)?;
    let dir_size_bytes = (detail.level > DetailsLevel::Level2)
        .then(|| {
            common::disk::dir_disk_size(&config.dir)
                .ok()
                .map(|v| v as usize)
        })
        .flatten();
    let rotation = match config.rotation {
        AuditRotation::Daily => "daily",
        AuditRotation::Hourly => "hourly",
    };
    Some(AuditTelemetry {
        dir: config.dir.display().to_string(),
        rotation: rotation.to_string(),
        max_log_files: config.max_log_files,
        trust_forwarded_headers: config.trust_forwarded_headers,
        log_api: config.log_api,
        dir_size_bytes,
    })
}

fn get_system_data(storage_path: &Path) -> RunningEnvironmentTelemetry {
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

    let container_runtime = *CONTAINER_RUNTIME;

    RunningEnvironmentTelemetry {
        distribution,
        distribution_version,
        is_docker: container_runtime.is_docker(),
        container_runtime,
        cores: Some(common::cpu::get_num_cpus()),
        cpu_cores_used: common::process_cpu_usage::process_cpu_usage_cores(),
        ram_size: Some((segment::utils::mem::total_memory_bytes() / 1024) as usize),
        // Through the quota manager, the node's single reader of disk usage, so
        // this shares the reading the quota check takes rather than adding a
        // `statvfs` to every telemetry request.
        disk_size: shard::quota::global()
            .disk_capacity_bytes(storage_path)
            .map(|total| (total / 1024) as usize)
            .or_else(|| sys_info::disk_info().ok().map(|x| x.total as usize)),
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

/// Container runtime Qdrant is running under (`none` if bare metal).
#[derive(Serialize, Clone, Copy, Debug, JsonSchema, Anonymize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    None,
    Docker,
    Kubernetes,
    // Some other container (Podman, LXC, systemd-nspawn, plain containerd, …).
    Other,
}

/// Detected once: the container runtime cannot change during the process's life.
static CONTAINER_RUNTIME: LazyLock<ContainerRuntime> = LazyLock::new(ContainerRuntime::detect);

impl ContainerRuntime {
    /// Detect the runtime, most specific marker first.
    pub fn detect() -> Self {
        // k8s (containerd/CRI-O) writes no `/.dockerenv`; the injected env is the signal.
        if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
            || Path::new("/var/run/secrets/kubernetes.io").exists()
        {
            return ContainerRuntime::Kubernetes;
        }
        if Path::new("/.dockerenv").exists() {
            return ContainerRuntime::Docker;
        }
        // Podman, LXC/nspawn, or an unlabeled container image (overlay root).
        if Path::new("/run/.containerenv").exists()
            || Path::new("/run/systemd/container").exists()
            || has_overlay_root()
        {
            return ContainerRuntime::Other;
        }
        ContainerRuntime::None
    }

    fn is_docker(self) -> bool {
        matches!(self, ContainerRuntime::Docker)
    }
}

fn has_overlay_root() -> bool {
    // /proc/mounts columns: device mountpoint fstype opts …
    let Ok(mounts) = fs_err::read_to_string("/proc/mounts") else {
        return false;
    };
    mounts.lines().any(|line| {
        let mut cols = line.split_whitespace();
        let (_device, mountpoint, fstype) = (cols.next(), cols.next(), cols.next());
        mountpoint == Some("/") && fstype == Some("overlay")
    })
}

#[derive(Serialize, Clone, Debug, JsonSchema, Anonymize)]
pub struct GpuDeviceTelemetry {
    #[anonymize(false)]
    pub name: String,
}
