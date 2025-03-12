use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};

use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTracker;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::{PointIdType, SeqNumberType};

const FILE_MAPPINGS: &str = "id_tracker.mappings";
const FILE_VERSIONS: &str = "id_tracker.versions";

type VersionChange = (PointIdType, SeqNumberType);

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum MappingChange {
    Insert(PointIdType, PointOffsetType),
    Delete(PointIdType),
}

/// Mutable in-memory ID tracker with simple file-based backing storage
///
/// This ID tracker simply persists all recorded point mapping and versions changes to disk by
/// appending these changes to a file. When loading, all mappings and versions are deduplicated in memory so
/// that only the latest mappings for a point are kept.
///
/// This structure may grow forever by collecting changes. It therefore relies on the optimization
/// processes in Qdrant to eventually vacuum the segment this ID  tracker belongs to.
/// Reoptimization will clear all collected changes and start from scratch.
///
/// This ID tracker primarily replaces [`SimpleIdTracker`], so that we can eliminate the use of
/// RocksDB.
#[derive(Debug)]
pub struct MutableIdTracker {
    segment_path: PathBuf,
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,

    /// List of point versions pending to be persisted, will be persisted on flush
    pending_versions: Mutex<Vec<VersionChange>>,

    /// List of point mappings pending to be persisted, will be persisted on flush
    pending_mappings: Mutex<Vec<MappingChange>>,
}

impl MutableIdTracker {
    pub fn open(segment_path: PathBuf) -> OperationResult<Self> {
        let (mappings_path, versions_path) =
            (mappings_path(&segment_path), versions_path(&segment_path));
        let (has_mappings, has_versions) = (mappings_path.is_file(), versions_path.is_file());

        // Warn or error about unlikely or problematic scenarios
        if !has_mappings && has_versions {
            debug_assert!(
                false,
                "Missing mappings file for ID tracker while versions file exists, storage may be corrupted!",
            );
            log::error!(
                "Missing mappings file for ID tracker while versions file exists, storage may be corrupted!",
            );
        }
        if has_mappings && !has_versions {
            log::warn!(
                "Missing versions file for ID tracker, WAL should recover point mappings and versions",
            );
        }

        let mut deleted = BitVec::new();
        let mut internal_to_external: Vec<PointIdType> = Default::default();
        let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
        let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();
        let mut internal_to_version: Vec<SeqNumberType> = Default::default();

        if has_mappings {
            load_mappings(
                &mappings_path,
                &mut deleted,
                &mut internal_to_external,
                &mut external_to_internal_num,
                &mut external_to_internal_uuid,
            )
            .map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker mappings: {err}"))
            })?;
        }

        if has_versions {
            load_versions(
                &versions_path,
                &internal_to_external,
                &external_to_internal_num,
                &external_to_internal_uuid,
                &mut internal_to_version,
            )
            .map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker versions: {err}"))
            })?;
        }

        #[cfg(debug_assertions)]
        for (idx, id) in external_to_internal_num.iter() {
            debug_assert!(
                internal_to_external[*id as usize] == PointIdType::NumId(*idx),
                "Internal id {id} is mapped to external id {}, but should be {}",
                internal_to_external[*id as usize],
                PointIdType::NumId(*idx)
            );
        }

        let mappings = PointMappings::new(
            deleted,
            internal_to_external,
            external_to_internal_num,
            external_to_internal_uuid,
        );

        Ok(Self {
            segment_path,
            internal_to_version,
            mappings,
            pending_versions: Mutex::new(vec![]),
            pending_mappings: Mutex::new(vec![]),
        })
    }
}

impl IdTracker for MutableIdTracker {
    fn internal_version(&self, internal_id: PointOffsetType) -> Option<SeqNumberType> {
        self.internal_to_version.get(internal_id as usize).copied()
    }

    fn set_internal_version(
        &mut self,
        internal_id: PointOffsetType,
        version: SeqNumberType,
    ) -> OperationResult<()> {
        if let Some(external_id) = self.external_id(internal_id) {
            if internal_id as usize >= self.internal_to_version.len() {
                #[cfg(debug_assertions)]
                {
                    if internal_id as usize > self.internal_to_version.len() + 1 {
                        log::info!(
                            "Resizing versions is initializing larger range {} -> {}",
                            self.internal_to_version.len(),
                            internal_id + 1,
                        );
                    }
                }
                self.internal_to_version.resize(internal_id as usize + 1, 0);
            }
            self.internal_to_version[internal_id as usize] = version;
            self.pending_versions.lock().push((external_id, version));
        }
        Ok(())
    }

    fn internal_id(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        self.mappings.internal_id(&external_id)
    }

    fn external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.mappings.external_id(internal_id)
    }

    fn set_link(
        &mut self,
        external_id: PointIdType,
        internal_id: PointOffsetType,
    ) -> OperationResult<()> {
        self.mappings.set_link(external_id, internal_id);
        self.pending_mappings
            .lock()
            .push(MappingChange::Insert(external_id, internal_id));
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        self.mappings.drop(external_id);
        self.pending_mappings
            .lock()
            .push(MappingChange::Delete(external_id));
        Ok(())
    }

    fn iter_external(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        self.mappings.iter_external()
    }

    fn iter_internal(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.mappings.iter_internal()
    }

    fn iter_from(
        &self,
        external_id: Option<PointIdType>,
    ) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_from(external_id)
    }

    fn iter_random(&self) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + '_> {
        self.mappings.iter_random()
    }

    fn total_point_count(&self) -> usize {
        self.mappings.total_point_count()
    }

    fn available_point_count(&self) -> usize {
        self.mappings.available_point_count()
    }

    fn deleted_point_count(&self) -> usize {
        self.total_point_count() - self.available_point_count()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        self.iter_internal()
    }

    /// Creates a flusher function, that persists the removed points in the mapping database
    /// and flushes the mapping to disk.
    /// This function should be called _before_ flushing the version database.
    fn mapping_flusher(&self) -> Flusher {
        let mappings_path = mappings_path(&self.segment_path);

        // Take out pending mappings to flush and replace it with a preallocated vector to avoid
        // frequent reallocation on a busy segment
        let pending_mappings = {
            let mut pending_mappings = self.pending_mappings.lock();
            let count = pending_mappings.len();
            mem::replace(&mut *pending_mappings, Vec::with_capacity(count))
        };

        Box::new(move || {
            if pending_mappings.is_empty() {
                return Ok(());
            }

            // Open file in append mode to write new changes to the end
            let file = File::options()
                .create(true)
                .append(true)
                .open(&mappings_path)?;
            let mut writer = BufWriter::new(file);

            write_mappings(&mut writer, &pending_mappings).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to persist ID tracker point mappings ({}): {err}",
                    mappings_path.display(),
                ))
            })?;

            Ok(())
        })
    }

    /// Creates a flusher function, that persists the removed points in the version database
    /// and flushes the version database to disk.
    /// This function should be called _after_ flushing the mapping database.
    fn versions_flusher(&self) -> Flusher {
        let versions_path = versions_path(&self.segment_path);

        // Take out pending versions to flush and replace it with a preallocated vector to avoid
        // frequent reallocation on a busy segment
        let pending_versions = {
            let mut pending_versions = self.pending_versions.lock();
            let count = pending_versions.len();
            mem::replace(&mut *pending_versions, Vec::with_capacity(count))
        };

        Box::new(move || {
            if pending_versions.is_empty() {
                return Ok(());
            }

            // Open file in append mode to write new changes to the end
            let file = File::options()
                .create(true)
                .append(true)
                .open(&versions_path)?;
            let mut writer = BufWriter::new(file);

            write_versions(&mut writer, &pending_versions).map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to persist ID tracker point versions ({}): {err}",
                    versions_path.display(),
                ))
            })?;

            Ok(())
        })
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn cleanup_versions(&mut self) -> OperationResult<()> {
        let mut to_remove = Vec::new();
        for internal_id in self.iter_internal() {
            if self.internal_version(internal_id).is_none() {
                if let Some(external_id) = self.external_id(internal_id) {
                    to_remove.push(external_id);
                } else {
                    debug_assert!(false, "internal id {internal_id} has no external id");
                }
            }
        }
        for external_id in to_remove {
            self.drop(external_id)?;
            #[cfg(debug_assertions)]
            {
                log::debug!("dropped version for point {external_id} without version");
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "mutable id tracker"
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![
            mappings_path(&self.segment_path),
            versions_path(&self.segment_path),
        ]
    }
}

fn mappings_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_MAPPINGS)
}

fn versions_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_VERSIONS)
}

fn load_mappings(
    mappings_path: &Path,
    deleted: &mut BitVec,
    internal_to_external: &mut Vec<PointIdType>,
    external_to_internal_num: &mut BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid: &mut BTreeMap<Uuid, PointOffsetType>,
) -> OperationResult<()> {
    let mappings_file = File::open(mappings_path)?;
    let mappings_reader = std::io::BufReader::new(mappings_file);

    for entry in std::io::BufRead::lines(mappings_reader) {
        let change = parse_mapping(entry)?;

        match change {
            MappingChange::Insert(external_id, internal_id) => {
                // Update internal to external mapping
                if internal_id as usize >= internal_to_external.len() {
                    internal_to_external
                        .resize(internal_id as usize + 1, PointIdType::NumId(u64::MAX));
                }
                let replaced_external_id = internal_to_external[internal_id as usize];
                internal_to_external[internal_id as usize] = external_id;

                // If point already exists, drop existing mapping
                if deleted
                    .get(internal_id as usize)
                    .is_some_and(|deleted| !deleted)
                {
                    // Fixing corrupted mapping - this id should be recovered from WAL
                    // This should not happen in normal operation, but it can happen if
                    // the database is corrupted.
                    log::warn!(
                        "removing duplicated external id {external_id} in internal id {replaced_external_id}",
                    );
                    debug_assert!(false, "should never have to remove");
                    match replaced_external_id {
                        PointIdType::NumId(num) => {
                            external_to_internal_num.remove(&num);
                        }
                        PointIdType::Uuid(uuid) => {
                            external_to_internal_uuid.remove(&uuid);
                        }
                    }
                }

                // Mark point entry as not deleted
                if internal_id as usize >= deleted.len() {
                    deleted.resize(internal_id as usize + 1, true);
                }
                deleted.set(internal_id as usize, false);

                // Set external to internal mapping
                match external_id {
                    PointIdType::NumId(num) => {
                        external_to_internal_num.insert(num, internal_id);
                    }
                    PointIdType::Uuid(uuid) => {
                        external_to_internal_uuid.insert(uuid, internal_id);
                    }
                }
            }
            MappingChange::Delete(external_id) => {
                // Remove external to internal mapping
                let internal_id = match external_id {
                    PointIdType::NumId(idx) => external_to_internal_num.remove(&idx),
                    PointIdType::Uuid(uuid) => external_to_internal_uuid.remove(&uuid),
                };
                let Some(internal_id) = internal_id else {
                    continue;
                };

                // Set internal to external mapping back to max int
                if (internal_id as usize) < internal_to_external.len() {
                    internal_to_external[internal_id as usize] = PointIdType::NumId(u64::MAX);
                }

                // Mark internal point as deleted
                if internal_id as usize >= deleted.len() {
                    deleted.resize(internal_id as usize + 1, true);
                }
                deleted.set(internal_id as usize, true);
            }
        }
    }

    Ok(())
}

fn load_versions(
    versions_path: &Path,
    internal_to_external: &[PointIdType],
    external_to_internal_num: &BTreeMap<u64, PointOffsetType>,
    external_to_internal_uuid: &BTreeMap<Uuid, PointOffsetType>,
    internal_to_version: &mut Vec<SeqNumberType>,
) -> OperationResult<()> {
    internal_to_version.reserve(internal_to_external.len());

    let versions_file = File::open(versions_path)?;
    let versions_reader = std::io::BufReader::new(versions_file);

    for entry in std::io::BufRead::lines(versions_reader) {
        let (external_id, version) = parse_version(entry)?;

        let internal_id = match external_id {
            PointIdType::NumId(num) => external_to_internal_num.get(&num).copied(),
            PointIdType::Uuid(uuid) => external_to_internal_uuid.get(&uuid).copied(),
        };

        let Some(internal_id) = internal_id else {
            log::debug!("Found version: {version} without internal id, external id: {external_id}");
            continue;
        };

        if internal_id as usize >= internal_to_version.len() {
            internal_to_version.resize(internal_id as usize + 1, 0);
        }
        internal_to_version[internal_id as usize] = version;
    }

    Ok(())
}

fn parse_mapping(change: std::io::Result<String>) -> OperationResult<MappingChange> {
    change
        .map_err(|err| {
            OperationError::service_error_light(format!(
                "ID tracker mapping entry is corrupt, cannot parse as string: {err}",
            ))
        })
        .and_then(|change| {
            serde_json::from_str(&change).map_err(|err| {
                OperationError::service_error_light(format!(
                    "ID tracker mapping entry is corrupt, cannot parse as JSON: {err}",
                ))
            })
        })
}

fn parse_version(change: std::io::Result<String>) -> OperationResult<VersionChange> {
    change
        .map_err(|err| {
            OperationError::service_error_light(format!(
                "Failed to parse ID tracker version entry as string, data may be corrupted: {err}"
            ))
        })
        .and_then(|change| {
            serde_json::from_str(&change).map_err(|err| {
                OperationError::service_error_light(format!(
                    "Failed to parse ID tracker version entry as JSON, data may be corrupted: {err}"
                ))
            })
        })
}

fn write_mappings<T>(writer: &mut BufWriter<T>, changes: &[MappingChange]) -> OperationResult<()>
where
    T: Write,
{
    for change in changes {
        let entry = serde_json::to_vec(change)?;
        debug_assert!(
            !entry.contains(&b'\n'),
            "serialized mapping change entry cannot contain new line",
        );
        writer.write_all(&entry)?;
        writer.write_all(b"\n")?;
    }

    // Explicitly flush writer to catch IO errors
    writer
        .flush()
        .map_err(|err| OperationError::service_error(format!("Failed to flush: {err}")))?;

    Ok(())
}

fn write_versions<T>(writer: &mut BufWriter<T>, changes: &[VersionChange]) -> OperationResult<()>
where
    T: Write,
{
    for change in changes {
        let entry = serde_json::to_vec(change)?;
        debug_assert!(
            !entry.contains(&b'\n'),
            "serialized version change entry cannot contain new line",
        );
        writer.write_all(&entry)?;
        writer.write_all(b"\n")?;
    }

    // Explicitly flush writer to catch IO errors
    writer
        .flush()
        .map_err(|err| OperationError::service_error(format!("Failed to flush: {err}")))?;

    Ok(())
}
