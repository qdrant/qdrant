use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use cancel::CancellationToken;
use chrono::{DateTime, NaiveDate, Utc};

use crate::audit::{AuditConfig, AuditEvent};
use crate::content_manager::errors::StorageError;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;

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
/// Returns matching entries as [`AuditEvent`]s in **descending** chronological
/// order (newest first). Files are iterated from newest to oldest; within each
/// file a sliding window keeps only the last (newest) `remaining` matches so
/// memory stays O(limit) regardless of file size.
pub fn read_local_audit_logs(
    config: &AuditConfig,
    query: &AuditLogQuery,
    cancel: &CancellationToken,
) -> Result<Vec<AuditEvent>, StorageError> {
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

    // Sort newest-first: `audit.log` (current, no date) comes first,
    // then dated files in reverse chronological order.
    log_files.sort_by(|a, b| {
        let a_is_current = parse_file_date(a).is_none();
        let b_is_current = parse_file_date(b).is_none();
        match (a_is_current, b_is_current) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => b.cmp(a),
        }
    });

    let log_files = filter_files_by_time_range(&log_files, query);

    let mut results = Vec::new();

    for file_path in log_files {
        if cancel.is_cancelled() || results.len() >= query.limit {
            break;
        }

        let remaining = query.limit - results.len();
        let mut entries = read_entries_from_file(file_path, query, remaining, cancel)?;
        // Entries come out ascending from the file; reverse to newest-first.
        entries.reverse();
        results.append(&mut entries);
    }

    results.truncate(query.limit);
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
///
/// Returns the **last** (newest) `remaining` matching entries in ascending order.
/// Uses a sliding window so memory is O(remaining), not O(file_size).
fn read_entries_from_file(
    path: &Path,
    query: &AuditLogQuery,
    remaining: usize,
    cancel: &CancellationToken,
) -> Result<Vec<AuditEvent>, StorageError> {
    let file = fs_err::File::open(path).map_err(|e| StorageError::service_error(e.to_string()))?;
    let reader = BufReader::new(file);

    let mut window: VecDeque<AuditEvent> = VecDeque::with_capacity(remaining);

    for line in reader.lines() {
        if cancel.is_cancelled() {
            break;
        }

        let line = line.map_err(|e| StorageError::service_error(e.to_string()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let Ok(event) = serde_json::from_str::<AuditEvent>(trimmed) else {
            continue;
        };

        match matches_query_result(&event, query) {
            MatchResult::Match => {
                if window.len() >= remaining {
                    window.pop_front();
                }
                window.push_back(event);
            }
            MatchResult::NoMatch => {}
            // Entries are chronological; once we exceed time_to, all
            // subsequent entries will too.
            MatchResult::PastTimeTo => break,
        }
    }

    Ok(Vec::from(window))
}

enum MatchResult {
    Match,
    NoMatch,
    /// Entry timestamp >= time_to; all subsequent entries in this file will
    /// also exceed the bound (entries are chronological).
    PastTimeTo,
}

/// Check whether an audit event matches the query time range and filters.
fn matches_query_result(event: &AuditEvent, query: &AuditLogQuery) -> MatchResult {
    let AuditLogQuery {
        time_from,
        time_to,
        filters,
        limit: _,
    } = query;

    // Time range check
    if let Some(from) = time_from
        && event.timestamp < *from
    {
        return MatchResult::NoMatch;
    }
    if let Some(to) = time_to
        && event.timestamp >= *to
    {
        return MatchResult::PastTimeTo;
    }

    // Key=value filtering with exhaustive destructuring so that adding a new
    // field to AuditEvent causes a compile error here until it is handled.
    if !filters.is_empty() && !matches_filters(event, filters) {
        return MatchResult::NoMatch;
    }

    MatchResult::Match
}

/// Check whether every filter key=value pair matches the corresponding field
/// on `event`. Destructures `AuditEvent` exhaustively so new fields produce
/// a compile error until handled.
fn matches_filters(event: &AuditEvent, filters: &HashMap<String, String>) -> bool {
    filters
        .iter()
        .all(|(key, expected)| event_field_matches(event, key, expected).unwrap_or(false))
}

/// Return whether the named field on `event` equals `expected`.
/// Returns `None` for unknown field names.
fn event_field_matches(event: &AuditEvent, key: &str, expected: &str) -> Option<bool> {
    let AuditEvent {
        timestamp: _, // filtered separately via time_from/time_to
        method,
        auth_type,
        subject,
        remote,
        collection,
        tracing_id,
        result,
        error,
    } = event;

    match key {
        "method" => Some(method == expected),
        "auth_type" => {
            // Compare against the serde-serialized form of the enum variant.
            let serialized = serde_json::to_value(auth_type).ok()?;
            Some(serialized.as_str() == Some(expected))
        }
        "result" => {
            let serialized = serde_json::to_value(result).ok()?;
            Some(serialized.as_str() == Some(expected))
        }
        "subject" => Some(subject.as_deref() == Some(expected)),
        "remote" => Some(remote.as_deref() == Some(expected)),
        "collection" => Some(collection.as_deref() == Some(expected)),
        "tracing_id" => Some(tracing_id.as_deref() == Some(expected)),
        "error" => Some(error.as_deref() == Some(expected)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::AuditResult;
    use crate::rbac::AuthType;

    fn make_event() -> AuditEvent {
        AuditEvent {
            timestamp: "2024-06-15T10:30:00Z".parse().unwrap(),
            method: "upsert_points".to_string(),
            auth_type: AuthType::ApiKey,
            subject: None,
            remote: None,
            collection: None,
            tracing_id: None,
            result: AuditResult::Ok,
            error: None,
        }
    }

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
        let event = make_event();
        let query = AuditLogQuery::new(
            None,
            None,
            HashMap::from([("method".to_string(), "upsert_points".to_string())]),
            None,
        );
        assert!(matches!(
            matches_query_result(&event, &query),
            MatchResult::Match
        ));

        let query2 = AuditLogQuery::new(
            None,
            None,
            HashMap::from([("method".to_string(), "delete_points".to_string())]),
            None,
        );
        assert!(matches!(
            matches_query_result(&event, &query2),
            MatchResult::NoMatch
        ));
    }

    #[test]
    fn test_matches_query_time_range() {
        let event = make_event();
        let query = AuditLogQuery::new(
            Some("2024-06-15T00:00:00Z".parse().unwrap()),
            Some("2024-06-16T00:00:00Z".parse().unwrap()),
            HashMap::new(),
            None,
        );
        assert!(matches!(
            matches_query_result(&event, &query),
            MatchResult::Match
        ));

        let query_before = AuditLogQuery::new(
            Some("2024-06-16T00:00:00Z".parse().unwrap()),
            None,
            HashMap::new(),
            None,
        );
        assert!(matches!(
            matches_query_result(&event, &query_before),
            MatchResult::NoMatch
        ));
    }

    #[test]
    fn test_roundtrip_serialization() {
        let event = make_event();
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.method, event.method);
        assert_eq!(deserialized.timestamp, event.timestamp);
        assert_eq!(deserialized.auth_type, event.auth_type);
        assert_eq!(deserialized.result, event.result);
    }
}
