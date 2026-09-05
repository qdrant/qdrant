use segment::segment::memory::{PayloadIndexMemoryReport, VectorMemoryReport};

use super::*;

#[test]
fn small_reports_use_the_calling_thread() {
    let paths = vec![Path::new("unused"); MIN_PARALLEL_PROBE_FILES - 1];
    let caller = std::thread::current().id();
    let threads = map_file_probes(&paths, |_| std::thread::current().id());
    assert_eq!(threads.len(), paths.len());
    assert!(threads.iter().all(|thread| *thread == caller));
}

#[test]
fn large_reports_release_their_workers() {
    use std::cell::RefCell;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    thread_local! {
        static WORKER_LIFETIME: RefCell<Option<Arc<mpsc::Sender<()>>>> = const { RefCell::new(None) };
    }

    let (sender, receiver) = mpsc::channel();
    let lifetime = Arc::new(sender);
    let paths = vec![Path::new("unused"); MIN_PARALLEL_PROBE_FILES];
    let caller = std::thread::current().id();
    let threads = map_file_probes(&paths, |_| {
        WORKER_LIFETIME.with(|guard| *guard.borrow_mut() = Some(lifetime.clone()));
        std::thread::current().id()
    });
    assert_eq!(threads.len(), paths.len());
    if std::thread::available_parallelism().unwrap().get() > 1 {
        assert!(threads.iter().all(|thread| *thread != caller));
    } else {
        assert!(threads.iter().all(|thread| *thread == caller));
        WORKER_LIFETIME.with(|guard| guard.borrow_mut().take());
    }
    // Scoped joins wait for worker functions; TLS destructors can finish later.
    // The channel closes when all workers have released their TLS references.
    drop(lifetime);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    );
}

fn segment_report() -> SegmentMemoryReport {
    SegmentMemoryReport {
        vectors: HashMap::new(),
        sparse_vectors: HashMap::new(),
        payload: ComponentMemoryUsage::empty(),
        payload_index: HashMap::new(),
        id_tracker: ComponentMemoryUsage::empty(),
    }
}

#[test]
fn empty_and_heap_only_reports() {
    let empty = report_from_segments(Vec::new());
    assert_eq!(empty.total.disk_bytes, 0);
    assert!(empty.warnings.is_empty());
    assert!(
        serde_json::to_value(empty)
            .unwrap()
            .get("warnings")
            .is_none()
    );

    let mut segment = segment_report();
    segment.payload = ComponentMemoryUsage::ram_only(123);
    segment.id_tracker = ComponentMemoryUsage::ram_only(456);
    let report = report_from_segments(vec![segment]);
    assert_eq!(report.total.ram_bytes, 579);
    assert_eq!(report.total.disk_bytes, 0);
    assert!(report.warnings.is_empty());
}

#[test]
fn storage_intent_only_changes_expected_cache() {
    let path = PathBuf::from("data");
    let probes = HashMap::from([(
        path.clone(),
        FileProbe {
            disk_bytes: 100,
            resident_bytes: 40,
        },
    )]);
    for intent in [FileStorageIntent::Cached, FileStorageIntent::OnDisk] {
        let report = measure_component(
            ComponentMemoryUsage::from_files_and_ram(vec![path.clone()], intent, 12),
            &probes,
        );
        assert_eq!(report.disk_bytes, 100);
        assert_eq!(report.cached_bytes, 40);
        assert_eq!(report.ram_bytes, 12);
        assert_eq!(
            report.expected_cache_bytes,
            if intent == FileStorageIntent::Cached {
                100
            } else {
                0
            }
        );
    }
}

#[test]
fn measures_and_merges_every_component() {
    let dir = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (1..=8)
        .map(|i| {
            let path = dir.path().join(i.to_string());
            fs_err::write(&path, vec![1; i * 4096]).unwrap();
            path
        })
        .collect();
    let make_segment = || {
        let usage = |i: usize| {
            ComponentMemoryUsage::from_files(vec![paths[i].clone()], FileStorageIntent::Cached)
        };
        let mut segment = segment_report();
        segment.vectors.insert(
            "dense".into(),
            VectorMemoryReport {
                storage: usage(0),
                index: usage(1),
                quantized: Some(usage(2)),
            },
        );
        segment.sparse_vectors.insert(
            "sparse".into(),
            VectorMemoryReport {
                storage: usage(3),
                index: usage(4),
                quantized: None,
            },
        );
        segment.payload = usage(5);
        segment
            .payload_index
            .insert("field".into(), PayloadIndexMemoryReport { usage: usage(6) });
        segment.id_tracker = usage(7);
        segment
    };
    let report = report_from_segments(vec![make_segment(), make_segment()]);
    assert!(report.warnings.is_empty());
    assert_eq!(report.vectors.len(), 1);
    assert_eq!(report.vectors[0].storage.disk_bytes, 2 * 4096);
    assert_eq!(report.vectors[0].index.disk_bytes, 4 * 4096);
    assert_eq!(
        report.vectors[0].quantized.as_ref().unwrap().disk_bytes,
        6 * 4096
    );
    assert_eq!(report.sparse_vectors[0].storage.disk_bytes, 8 * 4096);
    assert_eq!(report.sparse_vectors[0].index.disk_bytes, 10 * 4096);
    assert_eq!(report.payload.disk_bytes, 12 * 4096);
    assert_eq!(report.payload_index[0].usage.disk_bytes, 14 * 4096);
    assert_eq!(report.other.id_tracker.disk_bytes, 16 * 4096);
    assert_eq!(report.total.disk_bytes, 72 * 4096);
    assert_eq!(report.total.expected_cache_bytes, report.total.disk_bytes);
    assert!(report.total.cached_bytes <= report.total.disk_bytes);
}

#[test]
fn missing_file_warns_once_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let present = dir.path().join("present");
    fs_err::write(&present, [1; 4096]).unwrap();
    let missing = dir.path().join("missing");
    let make_segment = || {
        let mut segment = segment_report();
        segment.payload = ComponentMemoryUsage::from_files_and_ram(
            vec![present.clone(), missing.clone(), missing.clone()],
            FileStorageIntent::Cached,
            42,
        );
        segment
    };
    let report = report_from_segments(vec![make_segment(), make_segment()]);
    assert_eq!(report.total.disk_bytes, 8192);
    assert_eq!(report.total.ram_bytes, 84);
    assert_eq!(report.warnings.len(), 1);
    assert!(report.warnings[0].contains(&missing.display().to_string()));
    let merged = CollectionMemoryReport::merge_all(vec![report.clone(), report]);
    assert_eq!(merged.warnings.len(), 2);
}

#[test]
#[cfg(unix)]
fn failed_probe_retains_available_disk_size() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unreadable");
    fs_err::write(&path, [1; 4096]).unwrap();
    fs_err::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    if fs_err::File::open(&path).is_ok() {
        return; // Root can read files regardless of their permissions.
    }
    let mut warnings = Vec::new();
    let probes = probe_files(&[&path], &mut warnings);
    let probe = &probes[&path];
    assert_eq!(probe.disk_bytes, 4096);
    assert_eq!(probe.resident_bytes, 0);
    assert_eq!(warnings.len(), 1);
}
