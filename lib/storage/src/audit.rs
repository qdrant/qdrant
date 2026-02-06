use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::rbac::AuthType;

/// Global audit logger singleton.
static AUDIT_LOGGER: OnceLock<AuditLogger> = OnceLock::new();

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

    /// Maximum age of audit log files in seconds.  Files older than this are
    /// deleted on startup.  Default: 604800 (7 days).
    #[serde(default = "default_max_retention_sec")]
    pub max_retention_sec: u64,
}

fn default_audit_dir() -> PathBuf {
    PathBuf::from("./storage/audit")
}

const fn default_max_retention_sec() -> u64 {
    604_800 // 7 days
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
    dir: PathBuf,
    rotation: AuditRotation,
    state: Mutex<LoggerState>,
}

struct LoggerState {
    writer: std::io::BufWriter<fs_err::File>,
    current_tag: String,
}

impl AuditLogger {
    fn new(config: &AuditConfig) -> anyhow::Result<Self> {
        fs_err::create_dir_all(&config.dir)?;

        let tag = Self::current_tag(&config.rotation);
        let path = Self::file_path(&config.dir, &tag);
        let file = fs_err::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            dir: config.dir.clone(),
            rotation: config.rotation.clone(),
            state: Mutex::new(LoggerState {
                writer: std::io::BufWriter::new(file),
                current_tag: tag,
            }),
        })
    }

    fn current_tag(rotation: &AuditRotation) -> String {
        let now = Utc::now();
        match rotation {
            AuditRotation::Daily => now.format("%Y-%m-%d").to_string(),
            AuditRotation::Hourly => now.format("%Y-%m-%d-%H").to_string(),
        }
    }

    fn file_path(dir: &Path, tag: &str) -> PathBuf {
        dir.join(format!("audit-{tag}.log"))
    }

    fn write(&self, event: &AuditEvent) {
        let Ok(mut state) = self.state.lock() else {
            // Mutex poisoned – silently drop the event.
            return;
        };

        // Rotate if needed.
        let tag = Self::current_tag(&self.rotation);
        if tag != state.current_tag {
            let path = Self::file_path(&self.dir, &tag);
            match fs_err::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(file) => {
                    state.writer = std::io::BufWriter::new(file);
                    state.current_tag = tag;
                }
                Err(err) => {
                    log::error!("Audit log rotation failed: {err}");
                    return;
                }
            }
        }

        // Serialize and write.
        if let Err(err) = serde_json::to_writer(&mut state.writer, event)
            .and_then(|_| {
                // newline delimited JSON
                state.writer.write_all(b"\n").map_err(serde_json::Error::io)
            })
            .and_then(|_| state.writer.flush().map_err(serde_json::Error::io))
        {
            log::error!("Failed to write audit log entry: {err}");
        }
    }
}

// ---------------------------------------------------------------------------
// Retention cleanup
// ---------------------------------------------------------------------------

fn cleanup_old_files(dir: &Path, max_retention_sec: u64) {
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(max_retention_sec);

    let entries = match fs_err::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            log::warn!("Failed to read audit log dir for cleanup: {err}");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !name.starts_with("audit-") || !name.ends_with(".log") {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            let modified = meta.modified().unwrap_or(std::time::SystemTime::now());
            if modified < cutoff {
                if let Err(err) = fs_err::remove_file(&path) {
                    log::warn!("Failed to remove old audit log {}: {err}", path.display());
                } else {
                    log::info!("Removed old audit log file: {}", path.display());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Initialise the global audit logger from configuration.  Must be called at
/// most once (from `main`).  If the config is `None` or `enabled` is `false`,
/// no logger is created and all `audit_log` calls are no‑ops.
pub fn init_audit_logger(config: Option<&AuditConfig>) -> anyhow::Result<()> {
    let Some(config) = config else {
        return Ok(());
    };

    if !config.enabled {
        return Ok(());
    }

    // Clean up old files before creating the logger.
    cleanup_old_files(&config.dir, config.max_retention_sec);

    let logger = AuditLogger::new(config)?;
    AUDIT_LOGGER
        .set(logger)
        .map_err(|_| anyhow::anyhow!("Audit logger already initialised"))?;

    log::info!("Audit logging enabled, writing to {}", config.dir.display());

    Ok(())
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
