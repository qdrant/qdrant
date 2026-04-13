use serde::{Deserialize, Serialize};

use crate::operations::types::{CollectionError, CollectionResult};

/// Memory usage stats for a single component, after mincore measurement.
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
        }

        result
    }
}

/// Convert a `ComponentMemoryUsage` into a `MemoryUsageReport` by measuring
/// file residency via mincore and adding extra RAM.
#[cfg(unix)]
pub fn measure_component(
    usage: segment::common::memory_usage::ComponentMemoryUsage,
) -> CollectionResult<MemoryUsageReport> {
    use common::universal_io::MmapFile;
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

    for entry in files {
        let ComponentFileEntry { path, intent } = &entry;
        let (file_disk, file_resident) = MmapFile::probe_memory_stats(path).map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to probe memory stats for {}: {err}",
                path.display()
            ))
        })?;

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

    Ok(MemoryUsageReport {
        disk_bytes,
        ram_bytes,
        cached_bytes,
        expected_cache_bytes,
    })
}

/// Non-unix fallback: report disk sizes only, no residency info.
#[cfg(not(unix))]
pub fn measure_component(
    usage: segment::common::memory_usage::ComponentMemoryUsage,
) -> CollectionResult<MemoryUsageReport> {
    use segment::common::memory_usage::{
        ComponentFileEntry, ComponentMemoryUsage, FileStorageIntent,
    };

    let ComponentMemoryUsage {
        files,
        extra_ram_bytes,
    } = usage;

    let ram_bytes = extra_ram_bytes.unwrap_or(0);
    let mut disk_bytes = 0u64;
    let mut expected_cache_bytes = 0u64;

    for entry in files {
        let ComponentFileEntry { path, intent } = &entry;
        if let Ok(meta) = std::fs::metadata(path) {
            disk_bytes += meta.len();
            if *intent == FileStorageIntent::Cached {
                expected_cache_bytes += meta.len();
            }
        }
    }

    Ok(MemoryUsageReport {
        disk_bytes,
        ram_bytes,
        cached_bytes: 0,
        expected_cache_bytes,
    })
}

/// Build a `CollectionMemoryReport` from a segment-level report.
pub fn report_from_segment(
    segment_report: segment::segment::memory::SegmentMemoryReport,
) -> CollectionResult<CollectionMemoryReport> {
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
            Ok(NamedVectorMemoryReport {
                name,
                storage: measure_component(storage)?,
                index: measure_component(index)?,
                quantized: quantized.map(measure_component).transpose()?,
            })
        })
        .collect::<CollectionResult<Vec<_>>>()?;

    let sparse_vectors = sparse_vectors
        .into_iter()
        .map(|(name, vr)| {
            let VectorMemoryReport {
                storage,
                index,
                quantized: _,
            } = vr;
            Ok(NamedSparseVectorMemoryReport {
                name,
                storage: measure_component(storage)?,
                index: measure_component(index)?,
            })
        })
        .collect::<CollectionResult<Vec<_>>>()?;

    let payload_index = payload_index
        .into_iter()
        .map(|(name, pi)| {
            let PayloadIndexMemoryReport { usage } = pi;
            Ok(NamedPayloadIndexMemoryReport {
                name,
                usage: measure_component(usage)?,
            })
        })
        .collect::<CollectionResult<Vec<_>>>()?;

    let payload = measure_component(payload)?;
    let other = OtherMemoryReport {
        id_tracker: measure_component(id_tracker)?,
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

    Ok(CollectionMemoryReport {
        total,
        vectors,
        sparse_vectors,
        payload,
        payload_index,
        other,
    })
}
