use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use rayon::prelude::*;
use segment::common::memory_usage::{ComponentMemoryUsage, FileStorageIntent};
use segment::segment::memory::SegmentMemoryReport;
use serde::{Deserialize, Serialize};

/// Memory usage stats for a single component, after page-cache measurement.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryUsageReport {
    /// Total bytes stored on disk (file sizes).
    pub disk_bytes: u64,
    /// Non-evictable heap RAM (in-memory data structures not backed by mmap).
    pub ram_bytes: u64,
    /// Evictable RAM: resident file pages from mmap (OS page cache).
    pub cached_bytes: u64,
    /// Bytes that should ideally be cached for best performance.
    /// Sum of file sizes for `Cached` intent files (mmap-accessed data).
    pub expected_cache_bytes: u64,
}

impl MemoryUsageReport {
    pub fn merge(&mut self, other: &MemoryUsageReport) {
        let Self {
            disk_bytes,
            ram_bytes,
            cached_bytes,
            expected_cache_bytes,
        } = other;
        self.disk_bytes += disk_bytes;
        self.ram_bytes += ram_bytes;
        self.cached_bytes += cached_bytes;
        self.expected_cache_bytes += expected_cache_bytes;
    }
}

/// Per named vector (dense or multi-dense).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamedVectorMemoryReport {
    pub name: String,
    pub storage: MemoryUsageReport,
    pub index: MemoryUsageReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantized: Option<MemoryUsageReport>,
}

/// Per named sparse vector.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamedSparseVectorMemoryReport {
    pub name: String,
    pub storage: MemoryUsageReport,
    pub index: MemoryUsageReport,
}

/// Per payload field index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamedPayloadIndexMemoryReport {
    pub name: String,
    pub usage: MemoryUsageReport,
}

/// Other components (id_tracker, etc.).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OtherMemoryReport {
    pub id_tracker: MemoryUsageReport,
}

/// Full collection memory report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectionMemoryReport {
    pub total: MemoryUsageReport,
    pub vectors: Vec<NamedVectorMemoryReport>,
    pub sparse_vectors: Vec<NamedSparseVectorMemoryReport>,
    pub payload: MemoryUsageReport,
    pub payload_index: Vec<NamedPayloadIndexMemoryReport>,
    pub other: OtherMemoryReport,
    /// Files or shards that could not be fully measured.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl CollectionMemoryReport {
    /// Merge multiple reports (from different shards) into one.
    pub fn merge_all(reports: Vec<CollectionMemoryReport>) -> CollectionMemoryReport {
        let mut result = CollectionMemoryReport::default();

        for report in reports {
            let CollectionMemoryReport {
                total,
                vectors,
                sparse_vectors,
                payload,
                payload_index,
                other: OtherMemoryReport { id_tracker },
                warnings,
            } = report;

            result.total.merge(&total);

            // Merge vectors by name
            for vec_report in vectors {
                let NamedVectorMemoryReport {
                    name,
                    storage,
                    index,
                    quantized,
                } = &vec_report;

                if let Some(existing) = result.vectors.iter_mut().find(|v| v.name == *name) {
                    existing.storage.merge(storage);
                    existing.index.merge(index);
                    match (&mut existing.quantized, quantized) {
                        (Some(a), Some(b)) => a.merge(b),
                        (None, Some(b)) => existing.quantized = Some(b.clone()),
                        _ => {}
                    }
                } else {
                    result.vectors.push(vec_report);
                }
            }

            // Merge sparse vectors by name
            for sv_report in sparse_vectors {
                let NamedSparseVectorMemoryReport {
                    name,
                    storage,
                    index,
                } = &sv_report;

                if let Some(existing) = result.sparse_vectors.iter_mut().find(|v| v.name == *name) {
                    existing.storage.merge(storage);
                    existing.index.merge(index);
                } else {
                    result.sparse_vectors.push(sv_report);
                }
            }

            result.payload.merge(&payload);

            for pi_report in payload_index {
                let NamedPayloadIndexMemoryReport { name, usage } = &pi_report;

                if let Some(existing) = result.payload_index.iter_mut().find(|p| p.name == *name) {
                    existing.usage.merge(usage);
                } else {
                    result.payload_index.push(pi_report);
                }
            }

            result.other.id_tracker.merge(&id_tracker);
            result.warnings.extend(warnings);
        }

        result
    }
}

/// Probe results for a single file.
#[derive(Debug, Clone, Copy)]
struct FileProbe {
    disk_bytes: u64,
    resident_bytes: u64,
}

/// Apply file probes and heap estimates to a component.
fn measure_component(
    usage: ComponentMemoryUsage,
    probes: &HashMap<PathBuf, FileProbe>,
) -> MemoryUsageReport {
    let mut report = MemoryUsageReport {
        ram_bytes: usage.extra_ram_bytes.unwrap_or(0),
        ..Default::default()
    };
    for entry in usage.files {
        let probe = &probes[&entry.path];
        report.disk_bytes += probe.disk_bytes;
        report.cached_bytes += probe.resident_bytes;
        if entry.intent == FileStorageIntent::Cached {
            report.expected_cache_bytes += probe.disk_bytes;
        }
    }
    report
}

/// Reuse workers across segments, shards and requests.
static FILE_PROBE_POOL: LazyLock<Option<rayon::ThreadPool>> = LazyLock::new(|| {
    // Bound scheduling overhead while retaining parallelism for large files.
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(32);
    if threads == 1 {
        return None;
    }
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("memory-probe-{i}"))
        .build()
        .ok()
});

/// Probe each path once, retaining disk sizes when residency is unavailable.
fn probe_files(paths: &[&Path], warnings: &mut Vec<String>) -> HashMap<PathBuf, FileProbe> {
    let probe = |path: &&Path| {
        #[cfg(unix)]
        let result = common::universal_io::MmapFile::probe_memory_stats(path);
        #[cfg(not(unix))]
        let result = fs_err::metadata(path).map(|meta| (meta.len(), 0));

        match result {
            Ok((disk_bytes, resident_bytes)) => (
                FileProbe {
                    disk_bytes,
                    resident_bytes,
                },
                None,
            ),
            Err(err) => (
                FileProbe {
                    disk_bytes: fs_err::metadata(path).map_or(0, |meta| meta.len()),
                    resident_bytes: 0,
                },
                Some(format!(
                    "Failed to probe memory stats for {}: {err}",
                    path.display()
                )),
            ),
        }
    };
    // Avoid initializing the pool for empty and single-file reports. If worker
    // creation fails, measuring sequentially still produces a complete report.
    let results: Vec<_> = if paths.len() > 1
        && let Some(pool) = &*FILE_PROBE_POOL
    {
        pool.install(|| paths.par_iter().map(probe).collect())
    } else {
        paths.iter().map(probe).collect()
    };
    paths
        .iter()
        .zip(results)
        .map(|(path, (probe, warning))| {
            warnings.extend(warning);
            (path.to_path_buf(), probe)
        })
        .collect()
}

fn segment_paths(report: &SegmentMemoryReport) -> impl Iterator<Item = &Path> {
    report
        .vectors
        .values()
        .chain(report.sparse_vectors.values())
        .flat_map(|vector| {
            [&vector.storage, &vector.index]
                .into_iter()
                .chain(&vector.quantized)
        })
        .chain([&report.payload, &report.id_tracker])
        .chain(report.payload_index.values().map(|index| &index.usage))
        .flat_map(|usage| usage.files.iter().map(|file| file.path.as_path()))
}

/// Measure all segment files after the caller has released segment locks.
pub fn report_from_segments(segment_reports: Vec<SegmentMemoryReport>) -> CollectionMemoryReport {
    let mut paths: Vec<_> = segment_reports.iter().flat_map(segment_paths).collect();
    paths.sort_unstable();
    paths.dedup();
    let mut warnings = Vec::new();
    let probes = probe_files(&paths, &mut warnings);
    let reports = segment_reports
        .into_iter()
        .map(|report| report_from_segment(report, &probes))
        .collect();
    let mut report = CollectionMemoryReport::merge_all(reports);
    report.warnings = warnings;
    report
}

/// Build a `CollectionMemoryReport` from a segment-level report.
fn report_from_segment(
    segment_report: SegmentMemoryReport,
    probes: &HashMap<PathBuf, FileProbe>,
) -> CollectionMemoryReport {
    use segment::segment::memory::{
        PayloadIndexMemoryReport, SegmentMemoryReport, VectorMemoryReport,
    };

    let SegmentMemoryReport {
        vectors,
        sparse_vectors,
        payload,
        payload_index,
        id_tracker,
    } = segment_report;

    let vectors = vectors
        .into_iter()
        .map(|(name, vr)| {
            let VectorMemoryReport {
                storage,
                index,
                quantized,
            } = vr;
            NamedVectorMemoryReport {
                name,
                storage: measure_component(storage, probes),
                index: measure_component(index, probes),
                quantized: quantized.map(|q| measure_component(q, probes)),
            }
        })
        .collect::<Vec<_>>();

    let sparse_vectors = sparse_vectors
        .into_iter()
        .map(|(name, vr)| {
            let VectorMemoryReport {
                storage,
                index,
                quantized: _,
            } = vr;
            NamedSparseVectorMemoryReport {
                name,
                storage: measure_component(storage, probes),
                index: measure_component(index, probes),
            }
        })
        .collect::<Vec<_>>();

    let payload_index = payload_index
        .into_iter()
        .map(|(name, pi)| {
            let PayloadIndexMemoryReport { usage } = pi;
            NamedPayloadIndexMemoryReport {
                name,
                usage: measure_component(usage, probes),
            }
        })
        .collect::<Vec<_>>();

    let payload = measure_component(payload, probes);
    let other = OtherMemoryReport {
        id_tracker: measure_component(id_tracker, probes),
    };

    let mut total = MemoryUsageReport::default();
    for v in &vectors {
        let NamedVectorMemoryReport {
            name: _,
            storage,
            index,
            quantized,
        } = v;
        total.merge(storage);
        total.merge(index);
        if let Some(q) = quantized {
            total.merge(q);
        }
    }
    for sv in &sparse_vectors {
        let NamedSparseVectorMemoryReport {
            name: _,
            storage,
            index,
        } = sv;
        total.merge(storage);
        total.merge(index);
    }
    total.merge(&payload);
    for pi in &payload_index {
        let NamedPayloadIndexMemoryReport { name: _, usage } = pi;
        total.merge(usage);
    }
    let OtherMemoryReport { id_tracker: ref id } = other;
    total.merge(id);

    CollectionMemoryReport {
        total,
        vectors,
        sparse_vectors,
        payload,
        payload_index,
        other,
        warnings: Vec::new(),
    }
}

#[cfg(test)]
mod tests;
