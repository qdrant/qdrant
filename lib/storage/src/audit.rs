use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use crate::rbac::AuthType;

/// Global audit logger singleton.
static AUDIT_LOGGER: OnceLock<AuditLogger> = OnceLock::new();

/// Whether the audit logger trusts forwarded headers (`X-Forwarded-For`).
/// Stored separately so it can be queried before/without an active logger.
static TRUST_FORWARDED_HEADERS: OnceLock<bool> = OnceLock::new();

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AuditConfig {
    /// Enable audit logging.
    #[serde(default)]
    pub enabled: bool,

    /// Directory to write audit log files into.
    #[serde(default = "default_audit_dir")]
    pub dir: PathBuf,

    /// Rotation interval: "daily" (default) or "hourly".
    #[serde(default)]
    pub rotation: AuditRotation,

    /// Maximum number of rotated audit log files to keep.  Older files are
    /// deleted when a new log file is created.  Default: 7.
    #[serde(default = "default_max_log_files")]
    pub max_log_files: usize,

    /// If true, use `X-Forwarded-For` header to determine the client address
    /// recorded in audit log entries.  Only enable this when running behind a
    /// trusted reverse proxy or load balancer.
    /// Default: false
    #[serde(default)]
    pub trust_forwarded_headers: bool,
}

fn default_audit_dir() -> PathBuf {
    PathBuf::from("./storage/audit")
}

const fn default_max_log_files() -> usize {
    7
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuditRotation {
    #[default]
    Daily,
    Hourly,
}

// ---------------------------------------------------------------------------
// Audit event
// ---------------------------------------------------------------------------

/// A single structured audit log entry.
#[derive(Serialize)]
pub struct AuditEvent {
    /// ISO‑8601 timestamp.
    pub timestamp: DateTime<Utc>,
    /// The API method / handler name.
    pub method: String,
    /// How the request was authenticated.
    pub auth_type: AuthType,
    /// The `subject` field from the JWT (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Remote IP address of the client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    /// Collection name, if the check was collection‑scoped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// `"ok"` when the access check passed, `"denied"` otherwise.
    pub result: &'static str,
    /// Error message when the access check failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Logger implementation
// ---------------------------------------------------------------------------

struct AuditLogger {
    writer: Mutex<NonBlocking>,
}

impl AuditLogger {
    fn new(config: &AuditConfig) -> anyhow::Result<(Self, WorkerGuard)> {
        fs_err::create_dir_all(&config.dir)?;

        let rotation = match config.rotation {
            AuditRotation::Daily => Rotation::DAILY,
            AuditRotation::Hourly => Rotation::HOURLY,
        };

        let appender = RollingFileAppender::builder()
            .rotation(rotation)
            .filename_prefix("audit")
            .filename_suffix("log")
            .max_log_files(config.max_log_files.max(1))
            .build(&config.dir)
            .map_err(|err| anyhow::anyhow!("Failed to create audit log appender: {err}"))?;

        // Wrap the appender in a non-blocking writer.  The actual file I/O is
        // performed by a dedicated worker thread.  The returned `WorkerGuard`
        // **must** be kept alive for the lifetime of the program – dropping it
        // flushes remaining buffered events and shuts down the worker thread.
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);

        Ok((
            Self {
                writer: Mutex::new(non_blocking),
            },
            guard,
        ))
    }

    fn write(&self, event: &AuditEvent) {
        // Serialize to a buffer first so the entire event is sent as one
        // atomic message to the non-blocking writer (avoids interleaved
        // partial writes from concurrent callers).
        let mut buf = match serde_json::to_vec(event) {
            Ok(buf) => buf,
            Err(err) => {
                log::error!("Failed to serialize audit log entry: {err}");
                return;
            }
        };
        buf.push(b'\n');

        let mut writer = self.writer.lock();
        if let Err(err) = writer.write_all(&buf) {
            log::error!("Failed to write audit log entry: {err}");
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Initialise the global audit logger from configuration.  Must be called at
/// most once (from `main`).  If the config is `None` or `enabled` is `false`,
/// no logger is created and all `audit_log` calls are no‑ops.
///
/// Returns a [`WorkerGuard`] that **must** be held alive (typically in
/// `main`) until the program exits.  Dropping the guard flushes any
/// remaining buffered audit events to disk.
pub fn init_audit_logger(config: Option<&AuditConfig>) -> anyhow::Result<Option<WorkerGuard>> {
    let Some(config) = config else {
        return Ok(None);
    };

    if !config.enabled {
        return Ok(None);
    }

    // Persist the forwarded-headers flag so it is available globally even
    // outside the audit logger itself (e.g. in auth middleware).
    let _ = TRUST_FORWARDED_HEADERS.set(config.trust_forwarded_headers);

    let (logger, guard) = AuditLogger::new(config)?;
    AUDIT_LOGGER
        .set(logger)
        .map_err(|_| anyhow::anyhow!("Audit logger already initialised"))?;

    log::info!("Audit logging enabled, writing to {}", config.dir.display());

    Ok(Some(guard))
}

/// Write an audit event.  If the audit logger was not initialised this is a
/// no‑op.
pub fn audit_log(event: AuditEvent) {
    if let Some(logger) = AUDIT_LOGGER.get() {
        logger.write(&event);
    }
}

/// Returns `true` if the audit logger is active.
pub fn is_audit_enabled() -> bool {
    AUDIT_LOGGER.get().is_some()
}

/// Returns `true` if the audit logger is configured to trust forwarded
/// headers (`X-Forwarded-For`) for determining the client address.
pub fn audit_trust_forwarded_headers() -> bool {
    TRUST_FORWARDED_HEADERS.get().copied().unwrap_or(false)
}
