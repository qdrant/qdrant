//! Optional [dial9](https://github.com/dial9-rs/dial9) Tokio telemetry.
//!
//! Enabled at compile time with the `dial9` feature and at runtime with
//! `DIAL9_ENABLED=true`. When disabled (feature off, or env unset/false),
//! runtime construction is unchanged and recording is a no-op.

use std::sync::OnceLock;
use std::time::Duration;
use std::{env, io};

use dial9::cpu::{CpuProfilingConfig, SchedEventConfig};
use dial9::{
    Dial9Handle, Dial9HandleTokioExt, DiskBuffer, Recorder, RecorderPerfExt, TokioAttachOptions,
    recorder_disabled, recorder_or_disabled,
};
use tokio::runtime::{Builder, Runtime};

/// Process-wide dial9 handle for attaching Tokio runtimes.
static HANDLE: OnceLock<Dial9Handle> = OnceLock::new();

/// Holds the dial9 recorder for the process lifetime.
///
/// Constructed by [`init`]. Dropping it (at process shutdown) flushes and
/// stops the recorder.
#[derive(Debug)]
pub struct Dial9Guard {
    recorder: Option<Recorder>,
}

impl Drop for Dial9Guard {
    fn drop(&mut self) {
        if let Some(recorder) = self.recorder.take() {
            recorder.graceful_shutdown(Duration::from_secs(5));
        }
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

/// Initialize dial9 from `DIAL9_*` environment variables.
///
/// Safe to call once at process start, before any Tokio runtimes are built.
/// Returns a guard that must be kept alive until shutdown.
///
/// `application_version` is recorded into every trace segment (e.g. the qdrant
/// binary version).
///
/// Recognized variables (subset of dial9's env surface):
/// - `DIAL9_ENABLED` — `true` to record (default: off)
/// - `DIAL9_TRACE_DIR` — trace directory (default: `/tmp/dial9-traces`)
/// - `DIAL9_MAX_DISK_USAGE_MB` — total on-disk budget (default: `1024`)
/// - `DIAL9_ROTATION_SECS` — segment rotation period (default: `60`)
/// - `DIAL9_CPU_PROFILE_ENABLED` — CPU stack sampling (default: on, Linux)
/// - `DIAL9_CPU_SAMPLE_HZ` — sampling frequency (default: `99`)
/// - `DIAL9_SCHEDULE_PROFILE_ENABLED` — sched-switch capture (default: on, Linux)
pub fn init(application_version: &str) -> Dial9Guard {
    let enabled = env_bool("DIAL9_ENABLED", false);

    if !enabled {
        log::info!("dial9 telemetry disabled (set DIAL9_ENABLED=true to enable)");
        let recorder = recorder_disabled();
        let _ = HANDLE.set(recorder.handle().clone());
        return Dial9Guard {
            recorder: Some(recorder),
        };
    }

    let trace_dir = env::var("DIAL9_TRACE_DIR").unwrap_or_else(|_| "/tmp/dial9-traces".to_string());
    let max_disk_mb = env::var("DIAL9_MAX_DISK_USAGE_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1024);
    let rotation_secs = env::var("DIAL9_ROTATION_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60);
    let cpu_profile_enabled = env_bool("DIAL9_CPU_PROFILE_ENABLED", cfg!(target_os = "linux"));
    let schedule_profile_enabled =
        env_bool("DIAL9_SCHEDULE_PROFILE_ENABLED", cfg!(target_os = "linux"));
    let cpu_sample_hz = env::var("DIAL9_CPU_SAMPLE_HZ")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(99);

    let writer = DiskBuffer::builder()
        .base_path(&trace_dir)
        .max_total_size(max_disk_mb.saturating_mul(1024 * 1024))
        .rotation_period(Duration::from_secs(rotation_secs))
        .build();

    if let Err(ref err) = writer {
        log::error!(
            "dial9: failed to create trace writer at {trace_dir}: {err}; telemetry disabled"
        );
    }

    let mut builder = recorder_or_disabled(writer).segment_metadata([
        ("service".to_string(), "qdrant".to_string()),
        (
            "application.version".to_string(),
            application_version.to_string(),
        ),
    ]);

    if cpu_profile_enabled {
        builder =
            builder.with_cpu_profiling(CpuProfilingConfig::default().frequency_hz(cpu_sample_hz));
    }
    if schedule_profile_enabled {
        builder = builder.with_sched_events(SchedEventConfig::default());
    }

    let recorder = builder.build();

    if recorder.handle().is_connected() {
        if let Err(err) = recorder.install_global_handle() {
            log::warn!("dial9: could not install global handle: {err}");
        }
        log::info!(
            "dial9 telemetry enabled; writing traces to {trace_dir} \
             (max {max_disk_mb} MiB, rotate every {rotation_secs}s, \
             cpu_profile={cpu_profile_enabled}, sched_profile={schedule_profile_enabled}, \
             sample_hz={cpu_sample_hz})"
        );
    } else {
        log::error!(
            "dial9: recorder not connected after init (trace_dir={trace_dir});              check DiskBuffer / cpu-profiling permissions; telemetry inactive"
        );
    }

    let _ = HANDLE.set(recorder.handle().clone());
    Dial9Guard {
        recorder: Some(recorder),
    }
}

fn handle() -> Dial9Handle {
    HANDLE.get().cloned().unwrap_or_else(Dial9Handle::disabled)
}

/// Build a Tokio runtime, attaching dial9 hooks when telemetry is active.
pub fn build_runtime(mut builder: Builder, runtime_name: &str) -> io::Result<Runtime> {
    let handle = handle();
    if !handle.is_connected() {
        return builder.build();
    }

    let task_tracking = env_bool("DIAL9_TASK_TRACKING_ENABLED", true);

    let options = TokioAttachOptions::builder()
        .runtime_name(runtime_name.to_string())
        .task_tracking_enabled(task_tracking)
        .build();

    handle.attach_tokio_runtime(builder, options)
}
