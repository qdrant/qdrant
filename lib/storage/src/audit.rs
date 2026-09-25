use std::path::PathBuf;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing_appender::non_blocking::WorkerGuard;

use crate::audit_sink::{AuditSink, AuditSinkKind, OnFull, build_sinks};
use crate::rbac::AuthType;

/// Maximum length for a tracing ID extracted from request headers.
pub const MAX_TRACING_ID_LEN: usize = 256;

/// Request headers checked (in priority order) to extract a tracing ID.
pub const TRACING_ID_HEADERS: &[&str] = &["x-request-id", "x-tracing-id", "traceparent"];

/// Extract a tracing ID from request headers, checking [`TRACING_ID_HEADERS`]
/// in priority order.  The value is truncated to [`MAX_TRACING_ID_LEN`] bytes.
pub fn extract_tracing_id(get_header: impl Fn(&str) -> Option<String>) -> Option<String> {
    let value = TRACING_ID_HEADERS.iter().find_map(|h| get_header(h))?;
    if value.len() > MAX_TRACING_ID_LEN {
        // Floor to a char boundary at or below MAX_TRACING_ID_LEN bytes so the
        // cap is enforced in bytes (as documented) without splitting a code point.
        let end = (0..=MAX_TRACING_ID_LEN)
            .rev()
            .find(|&i| value.is_char_boundary(i))
            .unwrap_or(0);
        Some(value[..end].to_string())
    } else {
        Some(value)
    }
}

/// Global audit logger singleton.
static AUDIT_LOGGER: OnceLock<AuditLogger> = OnceLock::new();

/// Whether the audit logger trusts forwarded headers (`X-Forwarded-For`).
/// Stored separately so it can be queried before/without an active logger.
static TRUST_FORWARDED_HEADERS: OnceLock<bool> = OnceLock::new();

/// Whether the audit logger should log the API method path.
static LOG_API: OnceLock<bool> = OnceLock::new();

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Clone)]
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

    /// If true, log the API method path (REST path or gRPC method) in addition
    /// to the internal operation name.  Default: true.
    #[serde(default = "default_log_api")]
    pub log_api: bool,

    /// If true, write audit records to rotating files in [`Self::dir`].
    ///
    /// This is the only sink the `/audit/logs` query API can read (for now?), so
    /// disabling it makes audit records unqueryable through Qdrant, they are
    /// only available wherever the other enabled sinks send them.
    ///
    /// Default: true
    #[serde(default = "default_write_to_file")]
    pub write_to_file: bool,

    /// If true, write audit records to the process' stdout, so that a log
    /// collector consuming stdout receives them.  Intended for stdout shipping
    /// (i.e. hybrid) esp. containerized deployments.
    ///
    /// Note that audit records are always JSON, so with the default
    /// `logger.format: text` the stdout stream mixes text log lines with JSON
    /// audit lines.  Set `logger.format: json` for a uniform stream.
    ///
    /// Default: false
    #[serde(default)]
    pub write_to_stdout: bool,

    /// What the file sink does when its 128k-record queue is full because the
    /// disk cannot keep up: `drop` (default) or `block`.
    ///
    /// `drop` never blocks but can lose audit records under sustained load.
    /// `block` loses nothing to a full queue, but applies backpressure to the
    /// request path: an indefinitely stalled disk stalls writes to the
    /// database.
    ///
    /// Default: `drop`
    #[serde(default)]
    pub on_file_full: OnFull,

    /// What the stdout sink does when its 128k-record queue is full because
    /// the consumer cannot keep up: `drop` (default) or `block`.
    ///
    /// Note that `block` here makes the stdout consumer, typically a log
    /// collector, a liveness dependency of the database. While it is not
    /// reading, writes stall.
    ///
    /// Default: `drop`
    #[serde(default)]
    pub on_stdout_full: OnFull,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dir: default_audit_dir(),
            rotation: AuditRotation::default(),
            max_log_files: default_max_log_files(),
            trust_forwarded_headers: false,
            log_api: default_log_api(),
            write_to_file: default_write_to_file(),
            write_to_stdout: false,
            on_file_full: OnFull::default(),
            on_stdout_full: OnFull::default(),
        }
    }
}

fn default_audit_dir() -> PathBuf {
    PathBuf::from("./storage/audit")
}

const fn default_max_log_files() -> usize {
    7
}

const fn default_log_api() -> bool {
    true
}

const fn default_write_to_file() -> bool {
    true
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

/// Whether the access check passed or was denied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditResult {
    Ok,
    Denied,
}

/// A single structured audit log entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    /// ISO‑8601 timestamp.
    pub timestamp: DateTime<Utc>,
    /// The internal operation name (e.g. `upsert_points`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// The API method path (REST path or gRPC method name).
    /// Populated when the `log_api` audit config option is enabled,
    /// or for denied authentication requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
    /// How the request was authenticated.
    pub auth_type: AuthType,
    /// The `subject` field from the JWT (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Remote IP address of the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    /// Collection name, if the check was collection‑scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// Tracing ID extracted from request headers, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_id: Option<String>,
    /// Whether the access check passed or was denied.
    pub result: AuditResult,
    /// Error message when the access check failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Logger implementation
// ---------------------------------------------------------------------------

struct AuditLogger {
    /// Every enabled destination. each record is fanned out to all of them.
    sinks: Vec<AuditSink>,
}

impl AuditLogger {
    fn new(config: &AuditConfig) -> anyhow::Result<(Self, Vec<WorkerGuard>)> {
        let (sinks, guards) = build_sinks(config)?;
        Ok((Self { sinks }, guards))
    }

    fn write(&self, event: &AuditEvent) {
        // Serialize once, then hand the same bytes to every sink, sending
        // whole record in a single write should keep concurrent callers from
        // interleaving partial JSON
        let mut buf = match serde_json::to_vec(event) {
            Ok(buf) => buf,
            Err(err) => {
                log::error!("Failed to serialize audit log entry: {err}");
                return;
            }
        };
        buf.push(b'\n');

        for sink in &self.sinks {
            sink.write(&buf);
        }
    }

    fn sink(&self, kind: AuditSinkKind) -> Option<&AuditSink> {
        self.sinks.iter().find(|sink| sink.kind() == kind)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Initialise the global audit logger from configuration.  Must be called at
/// most once (from `main`).  If the config is `None` or `enabled` is `false`,
/// no logger is created and all `audit_log` calls are no‑ops.
///
/// Returns the per-sink [`WorkerGuard`]s, which **must** be held alive
/// (typically in `main`) until the program exits.  Dropping them flushes any
/// remaining buffered audit events.
pub fn init_audit_logger(config: Option<&AuditConfig>) -> anyhow::Result<Vec<WorkerGuard>> {
    let Some(config) = config else {
        return Ok(Vec::new());
    };

    let AuditConfig {
        enabled,
        dir,
        rotation: _,
        max_log_files: _,
        trust_forwarded_headers,
        log_api,
        write_to_file,
        write_to_stdout,
        on_file_full: _,
        on_stdout_full: _,
    } = config;

    if !enabled {
        return Ok(Vec::new());
    }

    // Persist flags so they are available globally even outside the audit
    // logger itself (e.g. in auth middleware).
    let _ = TRUST_FORWARDED_HEADERS.set(*trust_forwarded_headers);
    let _ = LOG_API.set(*log_api);

    let (logger, guards) = AuditLogger::new(config)?;
    AUDIT_LOGGER
        .set(logger)
        .map_err(|_| anyhow::anyhow!("Audit logger already initialised"))?;

    let mut destinations = Vec::with_capacity(2);
    if *write_to_file {
        destinations.push(dir.display().to_string());
    }
    if *write_to_stdout {
        destinations.push("stdout".to_string());
    }
    log::info!(
        "Audit logging enabled, writing to {}",
        destinations.join(" and ")
    );

    if !*write_to_file {
        log::warn!(
            "Audit logging has no file sink (`audit.write_to_file` is false), \
             so the `/audit/logs` API cannot return records from this peer",
        );
    }

    Ok(guards)
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

/// Returns `true` if the audit logger is configured to log API method paths.
pub fn audit_log_api() -> bool {
    LOG_API.get().copied().unwrap_or(false)
}

/// Number of audit records discarded by the given sink because its queue was
/// full.  Returns `None` when audit logging is disabled or the sink is not
/// enabled.
pub fn audit_dropped_records(kind: AuditSinkKind) -> Option<u64> {
    Some(AUDIT_LOGGER.get()?.sink(kind)?.dropped_records())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_id_is_capped_in_bytes_for_multibyte_input() {
        // 300 four-byte chars ~= 1200 bytes. The old char-count truncation kept
        // 256 chars (~1024 bytes), overshooting the documented byte cap.
        let long = "😀".repeat(300);
        let out = extract_tracing_id(|h| (h == TRACING_ID_HEADERS[0]).then(|| long.clone()))
            .expect("header is present");
        assert!(
            out.len() <= MAX_TRACING_ID_LEN,
            "tracing id not capped in bytes: {} bytes",
            out.len()
        );
        assert!(!out.is_empty());
    }

    #[test]
    fn tracing_id_below_cap_is_unchanged() {
        let out =
            extract_tracing_id(|h| (h == TRACING_ID_HEADERS[0]).then(|| "req-123".to_string()));
        assert_eq!(out.as_deref(), Some("req-123"));
    }
}
