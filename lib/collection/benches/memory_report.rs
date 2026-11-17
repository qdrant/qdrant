use std::collections::HashMap;
use std::io::Write;

use collection::common::memory_reporter::report_from_segments;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use segment::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent};
use segment::segment::memory::SegmentMemoryReport;

fn memory_report(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_report");
    for (name, segments, files_per_segment, file_bytes) in [
        ("one_file", 1, 1, 4096),
        ("small_files", 1, 32, 4096),
        ("below_parallel_threshold", 1, 127, 4096),
        ("parallel_threshold", 1, 128, 4096),
        ("many_segments", 16, 32, 4096),
        ("many_files", 16, 256, 4096),
        ("large_files", 1, 32, 64 * 1024 * 1024),
        ("large_files_many_segments", 16, 2, 64 * 1024 * 1024),
        ("large_parallel_report", 16, 32, 4 * 1024 * 1024),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![1; file_bytes.min(1024 * 1024)];
        let paths: Vec<_> = (0..segments * files_per_segment)
            .map(|i| {
                let path = dir.path().join(i.to_string());
                let mut file = fs_err::File::create(&path).unwrap();
                for _ in 0..file_bytes / data.len() {
                    file.write_all(&data).unwrap();
                }
                file.sync_all().unwrap();
                path
            })
            .collect();

        // File creation and metadata assembly are outside the measured interval.
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    paths
                        .chunks(files_per_segment)
                        .map(|paths| SegmentMemoryReport {
                            vectors: HashMap::new(),
                            sparse_vectors: HashMap::new(),
                            payload: ComponentMemoryUsage::from_files(
                                paths.to_vec(),
                                FileStorageIntent::Cached,
                            ),
                            payload_index: HashMap::new(),
                            id_tracker: ComponentMemoryUsage::ram_only(42),
                        })
                        .collect()
                },
                report_from_segments,
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, memory_report);
criterion_main!(benches);
