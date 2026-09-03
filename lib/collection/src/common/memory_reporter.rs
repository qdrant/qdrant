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
    /// Non-fatal issues encountered while building the report (e.g. a remote
    /// shard timed out). Present only when the response is partial.
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

/// Convert a `ComponentMemoryUsage` into a `MemoryUsageReport` using
/// precomputed file probes (and optional disk-only fallback on non-unix).
fn measure_component_with_probes(
    usage: segment::common::memory_usage::ComponentMemoryUsage,
    probes: &std::collections::HashMap<std::path::PathBuf, FileProbe>,
) -> MemoryUsageReport {
    use segment::common::memory_usage::{
        ComponentFileEntry, ComponentMemoryUsage, FileStorageIntent,
    };

    let ComponentMemoryUsage {
        files,
        extra_ram_bytes,
    } = usage;

    let ram_bytes = extra_ram_bytes.unwrap_or(0);
    let mut disk_bytes = 0u64;
    let mut cached_bytes = 0u64;
    let mut expected_cache_bytes = 0u64;

    for entry in &files {
        let ComponentFileEntry { path, intent } = entry;
        let (file_disk, file_resident) = match probes.get(path) {
            Some(probe) => (probe.disk_bytes, probe.resident_bytes),
            None => {
                // Non-unix / failed probe: disk size from metadata when possible.
                match fs_err::metadata(path) {
                    Ok(meta) => (meta.len(), 0),
                    Err(_) => continue,
                }
            }
        };
        disk_bytes += file_disk;
        match intent {
            FileStorageIntent::Cached => {
                expected_cache_bytes += file_disk;
                cached_bytes += file_resident;
            }
            FileStorageIntent::OnDisk => {
                cached_bytes += file_resident;
            }
        }
    }

    MemoryUsageReport {
        disk_bytes,
        ram_bytes,
        cached_bytes,
        expected_cache_bytes,
    }
}

/// Probe many files concurrently. Failures become warnings; successful probes
/// are returned in the map.
///
/// Concurrency is capped to `available_parallelism` so this composes with
/// per-file range splitting inside `probe_memory_stats` without thread explosion.
#[cfg(unix)]
fn parallel_probe_files(
    paths: &[std::path::PathBuf],
    warnings: &mut Vec<String>,
) -> std::collections::HashMap<std::path::PathBuf, FileProbe> {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common::universal_io::MmapFile;

    if paths.is_empty() {
        return HashMap::new();
    }

    if paths.len() == 1 {
        let path = &paths[0];
        return match MmapFile::probe_memory_stats(path) {
            Ok((disk_bytes, resident_bytes)) => HashMap::from([(
                path.clone(),
                FileProbe {
                    disk_bytes,
                    resident_bytes,
                },
            )]),
            Err(err) => {
                warnings.push(format!(
                    "Failed to probe memory stats for {}: {err}",
                    path.display()
                ));
                HashMap::new()
            }
        };
    }

    let next = AtomicUsize::new(0);
    let successes = Mutex::new(HashMap::new());
    let failures = Mutex::new(Vec::new());

    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, paths.len());

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= paths.len() {
                        break;
                    }
                    let path = &paths[idx];
                    match MmapFile::probe_memory_stats(path) {
                        Ok((disk_bytes, resident_bytes)) => {
                            successes.lock().unwrap().insert(
                                path.clone(),
                                FileProbe {
                                    disk_bytes,
                                    resident_bytes,
                                },
                            );
                        }
                        Err(err) => {
                            failures.lock().unwrap().push(format!(
                                "Failed to probe memory stats for {}: {err}",
                                path.display()
                            ));
                        }
                    }
                }
            });
        }
    });

    warnings.extend(failures.into_inner().unwrap());
    successes.into_inner().unwrap()
}

#[cfg(not(unix))]
fn parallel_probe_files(
    _paths: &[std::path::PathBuf],
    _warnings: &mut Vec<String>,
) -> std::collections::HashMap<std::path::PathBuf, FileProbe> {
    std::collections::HashMap::new()
}

/// Collect every file path referenced by a segment memory report.
fn collect_segment_paths(
    segment_report: &segment::segment::memory::SegmentMemoryReport,
) -> Vec<std::path::PathBuf> {
    use segment::segment::memory::{
        PayloadIndexMemoryReport, SegmentMemoryReport, VectorMemoryReport,
    };

    let mut paths = Vec::new();
    let SegmentMemoryReport {
        vectors,
        sparse_vectors,
        payload,
        payload_index,
        id_tracker,
    } = segment_report;

    for vr in vectors.values() {
        let VectorMemoryReport {
            storage,
            index,
            quantized,
        } = vr;
        paths.extend(storage.files.iter().map(|f| f.path.clone()));
        paths.extend(index.files.iter().map(|f| f.path.clone()));
        if let Some(q) = quantized {
            paths.extend(q.files.iter().map(|f| f.path.clone()));
        }
    }
    for vr in sparse_vectors.values() {
        let VectorMemoryReport {
            storage,
            index,
            quantized: _,
        } = vr;
        paths.extend(storage.files.iter().map(|f| f.path.clone()));
        paths.extend(index.files.iter().map(|f| f.path.clone()));
    }
    paths.extend(payload.files.iter().map(|f| f.path.clone()));
    for pi in payload_index.values() {
        let PayloadIndexMemoryReport { usage } = pi;
        paths.extend(usage.files.iter().map(|f| f.path.clone()));
    }
    paths.extend(id_tracker.files.iter().map(|f| f.path.clone()));

    paths.sort();
    paths.dedup();
    paths
}

/// Build a `CollectionMemoryReport` from a segment-level report.
///
/// Callers must invoke this **outside** segment locks: it performs page-cache
/// probing I/O that can take a long time on large files.
pub fn report_from_segment(
    segment_report: segment::segment::memory::SegmentMemoryReport,
) -> CollectionMemoryReport {
    use segment::segment::memory::{
        PayloadIndexMemoryReport, SegmentMemoryReport, VectorMemoryReport,
    };

    let mut warnings = Vec::new();
    let paths = collect_segment_paths(&segment_report);
    let probes = parallel_probe_files(&paths, &mut warnings);

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
                storage: measure_component_with_probes(storage, &probes),
                index: measure_component_with_probes(index, &probes),
                quantized: quantized.map(|q| measure_component_with_probes(q, &probes)),
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
                storage: measure_component_with_probes(storage, &probes),
                index: measure_component_with_probes(index, &probes),
            }
        })
        .collect::<Vec<_>>();

    let payload_index = payload_index
        .into_iter()
        .map(|(name, pi)| {
            let PayloadIndexMemoryReport { usage } = pi;
            NamedPayloadIndexMemoryReport {
                name,
                usage: measure_component_with_probes(usage, &probes),
            }
        })
        .collect::<Vec<_>>();

    let payload = measure_component_with_probes(payload, &probes);
    let other = OtherMemoryReport {
        id_tracker: measure_component_with_probes(id_tracker, &probes),
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
        warnings,
    }
}
