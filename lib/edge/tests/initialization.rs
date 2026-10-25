use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::flags::{FeatureFlags, feature_flags, init_feature_flags};
use common::mmap::{MULTI_MMAP_IS_SUPPORTED, MULTI_MMAP_SUPPORT_CHECK_RESULT};
use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams, WalOptions};

const SCENARIO_ENV: &str = "QDRANT_EDGE_INITIALIZATION_TEST";
static INITIALIZATION_WARNINGS: AtomicUsize = AtomicUsize::new(0);
static LOGGER: InitializationLogger = InitializationLogger;

struct InitializationLogger;

impl log::Log for InitializationLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record<'_>) {
        let message = record.args().to_string();
        if message.contains("Feature flags not initialized!")
            || message.contains("Feature flags already initialized!")
            || message
                .contains("MULTI_MMAP_SUPPORT_CHECK_RESULT should be initialized before accessing")
        {
            INITIALIZATION_WARNINGS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn flush(&self) {}
}

#[test]
fn initializes_process_globals() {
    if let Ok(scenario) = std::env::var(SCENARIO_ENV) {
        check_initialization(&scenario);
        return;
    }

    // Each scenario needs fresh OnceLock/LazyLock globals, including the load-only case.
    for scenario in ["new", "load", "configured", "probe_error"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "initializes_process_globals", "--nocapture"])
            .env(SCENARIO_ENV, scenario)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{scenario}:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

fn check_initialization(scenario: &str) {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Warn);

    let dir = tempfile::tempdir().unwrap();
    let expected_flags = if scenario == "configured" {
        let flags = FeatureFlags {
            write_segment_manifest: true,
            ..FeatureFlags::default()
        };
        init_feature_flags(flags);
        MULTI_MMAP_SUPPORT_CHECK_RESULT.set(false).unwrap();
        flags
    } else {
        assert!(MULTI_MMAP_SUPPORT_CHECK_RESULT.get().is_none());
        FeatureFlags::default()
    };

    if scenario == "probe_error" {
        // A directory at the probe's file path makes the probe fail on every platform.
        fs_err::create_dir(dir.path().join(".qdrant_fs_check")).unwrap();
    }

    let config = EdgeConfig::builder()
        .vector("v", EdgeVectorParams::builder(4, Distance::Dot).build())
        .wal_options(WalOptions {
            segment_capacity: 1024 * 1024,
            ..WalOptions::default()
        })
        .build();
    let shard = if scenario == "load" {
        EdgeShard::load(dir.path(), Some(config)).unwrap()
    } else {
        EdgeShard::new(dir.path(), config).unwrap()
    };

    assert_eq!(feature_flags(), expected_flags);
    assert!(MULTI_MMAP_SUPPORT_CHECK_RESULT.get().is_some());
    if matches!(scenario, "configured" | "probe_error") {
        assert_eq!(MULTI_MMAP_SUPPORT_CHECK_RESULT.get(), Some(&false));
        assert!(!*MULTI_MMAP_IS_SUPPORTED);
    }
    drop(shard);

    // Reopening must preserve the process-wide values without repeated initialization warnings.
    let shard = EdgeShard::load(dir.path(), None).unwrap();
    assert_eq!(feature_flags(), expected_flags);
    assert_eq!(INITIALIZATION_WARNINGS.load(Ordering::Relaxed), 0);
    drop(shard);
}
