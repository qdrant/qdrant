use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use cancel::CancellationToken;
use chrono::{DateTime, NaiveDate, Utc};

use crate::audit::AuditConfig;
use crate::content_manager::errors::StorageError;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;
pub const TIMESTAMP_KEY: &str = "timestamp";

/// Parameters for querying audit logs.
#[derive(Debug, Clone)]
pub struct AuditLogQuery {
    pub time_from: Option<DateTime<Utc>>,
    pub time_to: Option<DateTime<Utc>>,
    pub filters: HashMap<String, String>,
    pub limit: usize,
}

impl AuditLogQuery {
    pub fn new(
        time_from: Option<DateTime<Utc>>,
        time_to: Option<DateTime<Utc>>,
        filters: HashMap<String, String>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            time_from,
            time_to,
            filters,
            limit: limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
        }
    }
}

/// Read audit log entries from local files, applying time range and field filters.
///
/// Returns matching entries as raw JSON strings.
pub fn read_local_audit_logs(
    config: &AuditConfig,
    query: &AuditLogQuery,
    cancel: &CancellationToken,
) -> Result<Vec<String>, StorageError> {
    if !config.enabled {
        return Err(StorageError::BadRequest {
            description: "Audit logging is not enabled".to_string(),
        });
    }

    let dir = &config.dir;
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut log_files = list_audit_files(dir)?;
    log_files.sort();

    let log_files = filter_files_by_time_range(&log_files, query);

    let mut results = Vec::new();

    for file_path in log_files {
        if cancel.is_cancelled() || results.len() >= query.limit {
            break;
        }

        let remaining = query.limit - results.len();
        let mut entries = read_entries_from_file(file_path, query, remaining, cancel)?;
        results.append(&mut entries);
    }

    Ok(results)
}

/// List all audit log files in the directory.
///
/// `tracing_appender::rolling` with prefix "audit" and suffix "log" produces:
/// - Daily:  `audit.2024-01-15.log`
/// - Hourly: `audit.2024-01-15-14.log`
/// - Also the current active file may be named `audit.log` (symlink or current)
fn list_audit_files(dir: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let entries = fs_err::read_dir(dir).map_err(|e| StorageError::service_error(e.to_string()))?;

    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| StorageError::service_error(e.to_string()))?;
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|name| name.starts_with("audit") && name.ends_with(".log"))
        {
            files.push(path);
        }
    }

    Ok(files)
}

/// Parsed audit log file timestamp with its rotation granularity.
struct AuditFileDate {
    start: DateTime<Utc>,
    /// Duration that this file covers (1 day for daily, 1 hour for hourly rotation).
    span: chrono::Duration,
}

/// Parse the date portion from an audit log filename.
///
/// Expected formats:
/// - `audit.2024-01-15.log`    -> daily file starting at 2024-01-15T00:00:00Z
/// - `audit.2024-01-15-14.log` -> hourly file starting at 2024-01-15T14:00:00Z
/// - `audit.log`               -> None (current, always included)
fn parse_file_date(path: &Path) -> Option<AuditFileDate> {
    let name = path.file_stem()?.to_str()?;
    let date_part = name.strip_prefix("audit.")?;

    // Daily format: "2024-01-15"
    if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
        return Some(AuditFileDate {
            start: date.and_hms_opt(0, 0, 0)?.and_utc(),
            span: chrono::Duration::days(1),
        });
    }

    // Hourly format: "2024-01-15-14"
    let parts: Vec<_> = date_part.rsplitn(2, '-').collect();
    if let Some(&[hour_str, date_str]) = parts.get(0..2)
        && let (Ok(date), Ok(hour)) = (
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d"),
            hour_str.parse::<u32>(),
        )
        && hour < 24
    {
        return Some(AuditFileDate {
            start: date.and_hms_opt(hour, 0, 0)?.and_utc(),
            span: chrono::Duration::hours(1),
        });
    }

    None
}

/// Filter file list to only those whose date range may overlap the query window.
fn filter_files_by_time_range<'a>(files: &'a [PathBuf], query: &AuditLogQuery) -> Vec<&'a PathBuf> {
    files
        .iter()
        .filter(|path| {
            let Some(file_date) = parse_file_date(path) else {
                return true;
            };

            if let Some(ref time_to) = query.time_to
                && file_date.start >= *time_to
            {
                return false;
            }

            if let Some(ref time_from) = query.time_from {
                let file_end = file_date.start + file_date.span;
                if file_end <= *time_from {
                    return false;
                }
            }

            true
        })
        .collect()
}

/// Read and filter entries from a single audit log file.
fn read_entries_from_file(
    path: &Path,
    query: &AuditLogQuery,
    remaining: usize,
    cancel: &CancellationToken,
) -> Result<Vec<String>, StorageError> {
    let file = fs_err::File::open(path).map_err(|e| StorageError::service_error(e.to_string()))?;
    let reader = BufReader::new(file);

    let mut results = Vec::new();

    for line in reader.lines() {
        if cancel.is_cancelled() || results.len() >= remaining {
            break;
        }

        let line = line.map_err(|e| StorageError::service_error(e.to_string()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if matches_query(trimmed, query) {
            results.push(trimmed.to_string());
        }
    }

    Ok(results)
}

/// Check if a JSON line matches the query filters and time range.
fn matches_query(json_line: &str, query: &AuditLogQuery) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json_line) else {
        return false;
    };

    let Some(obj) = value.as_object() else {
        return false;
    };

    // Time range check
    if query.time_from.is_some() || query.time_to.is_some() {
        let Some(ts_val) = obj.get(TIMESTAMP_KEY).and_then(|v| v.as_str()) else {
            return false;
        };
        let Ok(ts) = ts_val.parse::<DateTime<Utc>>() else {
            return false;
        };
        if let Some(ref from) = query.time_from
            && ts < *from
        {
            return false;
        }
        if let Some(ref to) = query.time_to
            && ts >= *to
        {
            return false;
        }
    }

    // Key=value filtering: each filter key must match the corresponding top-level field
    for (key, expected_value) in &query.filters {
        match obj.get(key) {
            Some(actual) => {
                let actual_str = match actual {
                    serde_json::Value::String(s) => s.as_str().to_string(),
                    other => other.to_string(),
                };
                if actual_str != *expected_value {
                    return false;
                }
            }
            None => return false,
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_date_daily() {
        let path = PathBuf::from("/storage/audit/audit.2024-06-15.log");
        let file_date = parse_file_date(&path).unwrap();
        assert_eq!(
            file_date.start,
            "2024-06-15T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(file_date.span, chrono::Duration::days(1));
    }

    #[test]
    fn test_parse_file_date_hourly() {
        let path = PathBuf::from("/storage/audit/audit.2024-06-15-14.log");
        let file_date = parse_file_date(&path).unwrap();
        assert_eq!(
            file_date.start,
            "2024-06-15T14:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(file_date.span, chrono::Duration::hours(1));
    }

    #[test]
    fn test_parse_file_date_current() {
        let path = PathBuf::from("/storage/audit/audit.log");
        assert!(parse_file_date(&path).is_none());
    }

    #[test]
    fn test_matches_query_filters() {
        let line = r#"{"timestamp":"2024-06-15T10:30:00Z","method":"upsert_points","result":"ok","auth_type":"ApiKey"}"#;
        let query = AuditLogQuery::new(
            None,
            None,
            HashMap::from([("method".to_string(), "upsert_points".to_string())]),
            None,
        );
        assert!(matches_query(line, &query));

        let query2 = AuditLogQuery::new(
            None,
            None,
            HashMap::from([("method".to_string(), "delete_points".to_string())]),
            None,
        );
        assert!(!matches_query(line, &query2));
    }

    #[test]
    fn test_matches_query_time_range() {
        let line = r#"{"timestamp":"2024-06-15T10:30:00Z","method":"upsert_points","result":"ok"}"#;
        let query = AuditLogQuery::new(
            Some("2024-06-15T00:00:00Z".parse().unwrap()),
            Some("2024-06-16T00:00:00Z".parse().unwrap()),
            HashMap::new(),
            None,
        );
        assert!(matches_query(line, &query));

        let query_before = AuditLogQuery::new(
            Some("2024-06-16T00:00:00Z".parse().unwrap()),
            None,
            HashMap::new(),
            None,
        );
        assert!(!matches_query(line, &query_before));
    }
}
