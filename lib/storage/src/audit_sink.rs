//! Audit log sinks
//!
//! An [`AuditSink`] is a single destination for serialized audit records. The
//! audit logger owns a list of them and fans every record out to each, so a
//! deployment can write audit files and stream the same records to stdout
//! for a log collector to pick up.
//!
//! Every sink performs its file/stream I/O on a dedicated worker thread with
//! its own bounded queue (128k records), so in normal operation the request
//! path should never touches the destination.
//!
//! When a queue fills, the sink's configured [`OnFull`] policy decides
//! between losing records and stalling the request path.  Both defaults are
//! [`OnFull::Drop`], which preserves the behaviour Qdrant has always had 
//! (pre 1.20?).
//! [`OnFull::Block`] is opt-in per sink (since its somewhat anti-pattern to 
//! Qdrant performance targets but necessary for things like SOC 2 compliance)
//!
//! (note that records are fanned out sequentially, so a sink configured to
//! block holds up the sinks after it for that record)

use std::io::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};

use itertools::Itertools as _;
use parking_lot::Mutex;
use serde::Deserialize;
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

use crate::audit::{AuditConfig, AuditRotation};

/// Identifies a sink in log messages, metrics and telemetry.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AuditSinkKind {
    /// Rotating files in the configured audit directory.
    File,
    /// The process' standard output.
    Stdout,
}

impl AuditSinkKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Stdout => "stdout",
        }
    }

    const fn thread_name(self) -> &'static str {
        match self {
            Self::File => "audit-file",
            Self::Stdout => "audit-stdout",
        }
    }

    const fn policy_key(self) -> &'static str {
        match self {
            Self::File => "on_file_full",
            Self::Stdout => "on_stdout_full",
        }
    }
}

/// What a sink does when its queue is full because the worker is lagging
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFull {
    /// Discard the record and count it in [`AuditSink::dropped_records`].
    /// Never blocks, but audit records can be lost under sustained load.
    /// (default pre-1.20)
    #[default]
    Drop,
    /// Block the calling thread until the worker drains. No records are lost
    /// to a full queue at the cost of applying backpressure to the request
    /// path. An indefinitely stalled destination stalls writes to the
    /// database.
    Block,
}

impl OnFull {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Block => "block",
        }
    }
}

/// One audit record destination.
pub struct AuditSink {
    kind: AuditSinkKind,
    on_full: OnFull,
    // TODO: replace `NonBlocking`, maybe with a purpose-built worker would let 
    // us drop this mutex (the underlying crossbeam sender is `Sync`), avoid the
    // per-record `to_vec` copy that `NonBlocking::write` performs, and support
    // a bounded `send_timeout` instead of the lazy binary block-or-drop above
    writer: Mutex<NonBlocking>,
    /// Records this sink discarded because its queue was full
    dropped: AtomicU64,
}

impl AuditSink {
    /// Builds the rotating-file sink, meant to be durable, also what
    // backs the `/audit/logs` query API
    fn new_file(config: &AuditConfig, on_full: OnFull) -> anyhow::Result<(Self, WorkerGuard)> {
        let AuditConfig {
            enabled: _,
            dir,
            rotation,
            max_log_files,
            trust_forwarded_headers: _,
            log_api: _,
            write_to_file: _,
            write_to_stdout: _,
            on_file_full: _,
            on_stdout_full: _,
        } = config;

        fs_err::create_dir_all(dir)?;

        let rotation = match rotation {
            AuditRotation::Daily => Rotation::DAILY,
            AuditRotation::Hourly => Rotation::HOURLY,
        };

        let appender = RollingFileAppender::builder()
            .rotation(rotation)
            .filename_prefix("audit")
            .filename_suffix("log")
            .max_log_files((*max_log_files).max(1))
            .build(dir)
            .map_err(|err| anyhow::anyhow!("Failed to create audit log appender: {err}"))?;

        Ok(Self::spawn(AuditSinkKind::File, on_full, appender))
    }

    fn new_stdout(on_full: OnFull) -> (Self, WorkerGuard) {
        Self::spawn(AuditSinkKind::Stdout, on_full, io::stdout())
    }

    fn spawn<W>(kind: AuditSinkKind, on_full: OnFull, writer: W) -> (Self, WorkerGuard)
    where
        W: io::Write + Send + 'static,
    {
        // The returned `WorkerGuard` must be held for the lifetime of the
        // program since dropping it flushes queued records and stops the worker
        let (non_blocking, guard) = NonBlockingBuilder::default()
            .lossy(on_full == OnFull::Drop)
            .thread_name(kind.thread_name())
            .finish(writer);

        let sink = Self {
            kind,
            on_full,
            writer: Mutex::new(non_blocking),
            dropped: AtomicU64::new(0),
        };

        (sink, guard)
    }

    pub const fn kind(&self) -> AuditSinkKind {
        self.kind
    }

    pub const fn on_full(&self) -> OnFull {
        self.on_full
    }

    /// Number of records this sink has discarded due to a full queue.
    ///
    /// This does not cover records lost to a failed write on the worker
    /// thread (full/disconnected disk, broken pipe): `tracing_appender`'s 
    //  worker swallows I/O errors and keeps draining, so such losses are 
    //  invisible here and to [`OnFull::Block`] alike.
    //
    // TODO: closing that hole needs a purpose-built worker (see the TODO on
    // `AuditSink::writer`) that reports write failures back to the sink
    pub fn dropped_records(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Write one already-serialized, newline-terminated audit record.
    ///
    /// `line` must be a complete record. It is handed to the worker in a
    /// single `write_all` so concurrent callers cannot interleave partial JSON
    pub(crate) fn write(&self, line: &[u8]) {
        // Snapshot the queue's drop counter so we can attribute new drops to
        // this sink. In lossy mode `write_all` reports success even when the
        // record was discarded, so the counter is the only signal.
        let dropped_before = self.queue_dropped();

        let result = {
            let mut writer = self.writer.lock();
            writer.write_all(line)
        };

        if let Err(err) = result {
            log::error!(
                "Failed to write audit record to {} sink: {err}",
                self.kind.as_str(),
            );
            return;
        }

        let newly_dropped = self.queue_dropped().saturating_sub(dropped_before);
        if newly_dropped > 0 {
            self.dropped.fetch_add(newly_dropped, Ordering::Relaxed);
            // TODO: rate-limit this to prevent spam, maybe also emit a 
            // synthetic `audit_gap` record into the sink itself so the stream
            // declares its own gaps?
            log::error!(
                "Audit {} sink queue is full, discarded {newly_dropped} record(s)",
                self.kind.as_str(),
            );
        }
    }

    fn queue_dropped(&self) -> u64 {
        self.writer.lock().error_counter().dropped_lines() as u64
    }
}

/// Build every sink enabled by `config`, together with the worker guards that
/// must be held until shutdown.
///
/// Errors if audit logging is enabled but every sink is switched off.
/// This combination would leave access to the data unlogged while looking
/// configured, so it is rejected at startup rather than honoured.
///
/// Warns if every enabled sink is set to [`OnFull::Drop`], since audit
/// records can then be lost with no durable copy anywhere.
pub fn build_sinks(config: &AuditConfig) -> anyhow::Result<(Vec<AuditSink>, Vec<WorkerGuard>)> {
    let AuditConfig {
        write_to_file,
        write_to_stdout,
        on_file_full,
        on_stdout_full,
        ..
    } = config;

    // Checked against the resolved values, not against which keys the operator
    // wrote down
    // `write_to_file: false` on its own already leaves no sinks,
    // because `write_to_stdout` defaults to false.
    if !write_to_file && !write_to_stdout {
        anyhow::bail!(
            "Audit logging is enabled but every sink is disabled - \
             set `audit.write_to_file` and/or `audit.write_to_stdout` to true, \
             or set `audit.enabled: false`",
        );
    }

    let mut sinks = Vec::new();
    let mut guards = Vec::new();

    if *write_to_file {
        let (sink, guard) = AuditSink::new_file(config, *on_file_full)?;
        sinks.push(sink);
        guards.push(guard);
    }

    if *write_to_stdout {
        let (sink, guard) = AuditSink::new_stdout(*on_stdout_full);
        sinks.push(sink);
        guards.push(guard);
    }

    warn_if_every_sink_is_lossy(&sinks);

    Ok((sinks, guards))
}

/// Warn when no enabled sink is willing to block, so there is no destination
/// guaranteed to retain a record under sustained load.
///
/// Both policies default to [`OnFull::Drop`], so this fires for any config
/// that just sets `audit.enabled: true`
fn warn_if_every_sink_is_lossy(sinks: &[AuditSink]) {
    debug_assert!(!sinks.is_empty(), "callers reject a sink-less config");

    if !every_sink_is_lossy(sinks) {
        return;
    }

    let sink_list = sinks.iter().map(|sink| sink.kind().as_str()).join(" and ");
    let keys = sinks
        .iter()
        .map(|sink| format!("`audit.{}`", sink.kind().policy_key()))
        .join(" or ");

    log::warn!(
        "Audit logging may not be comprehensive: every enabled sink ({sink_list}) \
         is set to discard records when its queue is full, so entries may be lost \
         under sustained load. Set {keys} to `block` to guarantee no records are \
         lost to a full queue.",
    );
}

/// Whether no enabled sink is willing to block, i.e. no destination is
/// guaranteed to retain a record under sustained load.
fn every_sink_is_lossy(sinks: &[AuditSink]) -> bool {
    !sinks.is_empty() && sinks.iter().all(|sink| sink.on_full() == OnFull::Drop)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(write_to_file: bool, write_to_stdout: bool, dir: &std::path::Path) -> AuditConfig {
        AuditConfig {
            enabled: true,
            dir: dir.to_path_buf(),
            write_to_file,
            write_to_stdout,
            ..AuditConfig::default()
        }
    }

    #[test]
    fn defaults_reproduce_legacy_behaviour() {
        // A config that only sets `audit.enabled: true` must behave exactly
        // like every Qdrant release (pre 1.20) before these settings existed
        let config = AuditConfig::default();
        assert!(config.write_to_file);
        assert!(!config.write_to_stdout);
        assert_eq!(config.on_file_full, OnFull::Drop);
        assert_eq!(config.on_stdout_full, OnFull::Drop);
        // Unrelated defaults the hand-written `Default` also has to preserve
        assert!(config.log_api);
        assert_eq!(config.max_log_files, 7);
    }

    #[test]
    fn policies_are_passed_through_verbatim() {
        let dir = tempfile::tempdir().unwrap();

        let config = AuditConfig {
            on_file_full: OnFull::Block,
            on_stdout_full: OnFull::Drop,
            ..config(true, true, dir.path())
        };

        let (sinks, guards) = build_sinks(&config).unwrap();
        assert_eq!(sinks.len(), 2);
        assert_eq!(guards.len(), 2);
        assert_eq!(sinks[0].kind(), AuditSinkKind::File);
        assert_eq!(sinks[0].on_full(), OnFull::Block);
        assert_eq!(sinks[1].kind(), AuditSinkKind::Stdout);
        assert_eq!(sinks[1].on_full(), OnFull::Drop);
    }

    #[test]
    fn file_sink_only() {
        let dir = tempfile::tempdir().unwrap();
        let (sinks, guards) = build_sinks(&config(true, false, dir.path())).unwrap();
        assert_eq!(sinks.len(), 1);
        assert_eq!(guards.len(), 1);
        assert_eq!(sinks[0].kind(), AuditSinkKind::File);
    }

    #[test]
    fn stdout_sink_only() {
        let dir = tempfile::tempdir().unwrap();
        let (sinks, guards) = build_sinks(&config(false, true, dir.path())).unwrap();
        assert_eq!(sinks.len(), 1);
        assert_eq!(guards.len(), 1);
        assert_eq!(sinks[0].kind(), AuditSinkKind::Stdout);
    }

    /// The "audit may not be comprehensive" warning fires when no enabled
    /// sink is willing to block (not just when both settings read `drop`)
    #[test]
    fn lossy_warning_covers_only_enabled_sinks() {
        let dir = tempfile::tempdir().unwrap();

        let cases = [
            // (write_to_file, on_file_full, write_to_stdout, on_stdout_full, warns)
            // The default, legacy-equivalent config: warns.
            (true, OnFull::Drop, false, OnFull::Drop, true),
            (true, OnFull::Block, false, OnFull::Drop, false),
            (true, OnFull::Drop, true, OnFull::Drop, true),
            // One blocking sink retains the record, so no warning
            (true, OnFull::Block, true, OnFull::Drop, false),
            (true, OnFull::Drop, true, OnFull::Block, false),
            // `on_file_full` is irrelevant here since file sink is disabled
            // so the sole enabled sink dropping must still warn
            (false, OnFull::Block, true, OnFull::Drop, true),
            (false, OnFull::Drop, true, OnFull::Block, false),
        ];

        for (write_to_file, on_file_full, write_to_stdout, on_stdout_full, expected) in cases {
            let config = AuditConfig {
                on_file_full,
                on_stdout_full,
                ..config(write_to_file, write_to_stdout, dir.path())
            };

            let (sinks, _guards) = build_sinks(&config).unwrap();
            assert_eq!(
                every_sink_is_lossy(&sinks),
                expected,
                "write_to_file={write_to_file} ({on_file_full:?}), \
                 write_to_stdout={write_to_stdout} ({on_stdout_full:?})",
            );
        }
    }

    #[test]
    fn deserializes_policies_from_snake_case() {
        let parsed: OnFull = serde_json::from_str("\"drop\"").unwrap();
        assert_eq!(parsed, OnFull::Drop);
        let parsed: OnFull = serde_json::from_str("\"block\"").unwrap();
        assert_eq!(parsed, OnFull::Block);
    }

    #[test]
    fn stdout_only_does_not_create_audit_dir() {
        let dir = tempfile::tempdir().unwrap();
        let unused = dir.path().join("should-not-be-created");
        let (_sinks, _guards) = build_sinks(&config(false, true, &unused)).unwrap();
        assert!(!unused.exists());
    }

    #[test]
    fn no_sinks_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        // Enabled-but-no-sinks would leave data access unlogged while looking
        // configured, so it must fail at startup.
        assert!(build_sinks(&config(false, false, dir.path())).is_err());
    }

    #[test]
    fn disabling_the_file_sink_alone_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        // `write_to_stdout` defaults to false, so `write_to_file: false` on its
        // own already leaves no sinks
        let config = AuditConfig {
            write_to_file: false,
            ..AuditConfig {
                enabled: true,
                dir: dir.path().to_path_buf(),
                ..AuditConfig::default()
            }
        };
        assert!(!config.write_to_stdout, "stdout must default to false");
        assert!(build_sinks(&config).is_err());
    }

    #[test]
    fn file_sink_creates_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("does").join("not").join("exist");
        let (sinks, _guards) = build_sinks(&config(true, false, &nested)).unwrap();
        assert_eq!(sinks.len(), 1);
        assert!(nested.exists());
    }

    /// E2E for a record handed to the file sink lands on disk as one JSON
    /// line that deserializes back into an equivalent [`AuditEvent`].
    #[test]
    fn file_sink_round_trips_a_record() {
        use crate::audit::{AuditEvent, AuditResult};
        use crate::rbac::AuthType;

        let dir = tempfile::tempdir().unwrap();

        let event = AuditEvent {
            timestamp: "2026-09-03T10:30:00Z".parse().unwrap(),
            method: Some("upsert_points".to_string()),
            api: Some("/collections/demo/points".to_string()),
            auth_type: AuthType::ApiKey,
            subject: None,
            remote: Some("10.4.2.17".to_string()),
            collection: Some("demo".to_string()),
            tracing_id: Some("req-abc123".to_string()),
            result: AuditResult::Ok,
            error: None,
        };

        let mut line = serde_json::to_vec(&event).unwrap();
        line.push(b'\n');

        {
            let (sinks, guards) = build_sinks(&config(true, false, dir.path())).unwrap();
            sinks[0].write(&line);
            assert_eq!(sinks[0].dropped_records(), 0);
            // Dropping the guards flushes the worker and joins its thread.
            drop(guards);
        }

        let log_file = fs_err::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("audit") && name.ends_with(".log"))
            })
            .expect("audit log file should exist");

        let contents = fs_err::read_to_string(&log_file).unwrap();
        let lines: Vec<_> = contents.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 1, "expected exactly one record: {contents:?}");

        let read_back: AuditEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(read_back.timestamp, event.timestamp);
        assert_eq!(read_back.method, event.method);
        assert_eq!(read_back.api, event.api);
        assert_eq!(read_back.collection, event.collection);
        assert_eq!(read_back.tracing_id, event.tracing_id);
        assert_eq!(read_back.result, event.result);
    }
}
