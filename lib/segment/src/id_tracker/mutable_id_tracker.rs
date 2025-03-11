use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::mem;
use std::path::{Path, PathBuf};

use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTracker;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::{PointIdType, SeqNumberType};

const FILE_MAPPINGS: &str = "id_tracker.mappings";
const FILE_VERSIONS: &str = "id_tracker.versions";

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum MappingChange {
    Insert(PointIdType, PointOffsetType),
    Delete(PointIdType),
}

type VersionChange = (PointIdType, SeqNumberType);

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
        let mut deleted = BitVec::new();
        let mut internal_to_external: Vec<PointIdType> = Default::default();
        let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
        let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();

        // Load point mappings
        let mappings_path = Self::mappings_path(&segment_path);
        if mappings_path.is_file() {
            let mappings_file = File::open(mappings_path)?;
            let mappings_reader = std::io::BufReader::new(mappings_file);

            for entry in std::io::BufRead::lines(mappings_reader) {
                let entry = entry.expect("failed to parse mapping change entry");
                let change: MappingChange =
                    serde_json::from_str(&entry).expect("failed to parse mapping change JSON");

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
                                "removing duplicated external id {external_id} in internal id {replaced_external_id}"
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
                            internal_to_external[internal_id as usize] =
                                PointIdType::NumId(u64::MAX);
                        }

                        // Mark internal point as deleted
                        if internal_id as usize >= deleted.len() {
                            deleted.resize(internal_id as usize + 1, true);
                        }
                        deleted.set(internal_id as usize, true);
                    }
                }
            }
        }

        // Load point versions
        let mut internal_to_version: Vec<SeqNumberType> =
            Vec::with_capacity(internal_to_external.len());
        let versions_path = Self::versions_path(&segment_path);
        if versions_path.is_file() {
            let versions_file = File::open(versions_path)?;
            let versions_reader = std::io::BufReader::new(versions_file);

            for entry in std::io::BufRead::lines(versions_reader) {
                let entry = entry.expect("failed to parse version change entry");
                let (external_id, version): VersionChange =
                    serde_json::from_str(&entry).expect("failed to parse version change JSON");

                let internal_id = match external_id {
                    PointIdType::NumId(num) => external_to_internal_num.get(&num).copied(),
                    PointIdType::Uuid(uuid) => external_to_internal_uuid.get(&uuid).copied(),
                };

                let Some(internal_id) = internal_id else {
                    log::debug!(
                        "Found version: {version} without internal id, external id: {external_id}"
                    );
                    continue;
                };

                if internal_id as usize >= internal_to_version.len() {
                    internal_to_version.resize(internal_id as usize + 1, 0);
                }
                internal_to_version[internal_id as usize] = version;
            }
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

    fn mappings_path(segment_path: &Path) -> PathBuf {
        segment_path.join(FILE_MAPPINGS)
    }

    fn versions_path(segment_path: &Path) -> PathBuf {
        segment_path.join(FILE_VERSIONS)
    }

    fn persist_mapping(&self, external_id: PointIdType, internal_id: PointOffsetType) {
        self.pending_mappings
            .lock()
            .push(MappingChange::Insert(external_id, internal_id));
    }

    fn delete_mapping(&self, external_id: PointIdType) {
        self.pending_mappings
            .lock()
            .push(MappingChange::Delete(external_id));
    }

    fn persist_version(&self, external_id: PointIdType, version: SeqNumberType) {
        self.pending_versions.lock().push((external_id, version));
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
            self.persist_version(external_id, version);
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
        self.persist_mapping(external_id, internal_id);
        Ok(())
    }

    fn drop(&mut self, external_id: PointIdType) -> OperationResult<()> {
        self.mappings.drop(external_id);
        self.delete_mapping(external_id);
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
        let mappings_path = Self::mappings_path(&self.segment_path);
        let pending_mappings = mem::take(&mut *self.pending_mappings.lock());
        Box::new(move || {
            if pending_mappings.is_empty() {
                return Ok(());
            }

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(mappings_path)?;
            let mut writer = BufWriter::new(file);

            for change in pending_mappings {
                let entry = serde_json::to_string(&change)?;
                debug_assert!(
                    !entry.contains('\n'),
                    "serialized mapping change entry cannot contain new line",
                );
                writer.write_all(entry.as_bytes())?;
                writer.write_all(b"\n")?;
            }

            writer.flush()?;
            drop(writer);

            Ok(())
        })
    }

    /// Creates a flusher function, that persists the removed points in the version database
    /// and flushes the version database to disk.
    /// This function should be called _after_ flushing the mapping database.
    fn versions_flusher(&self) -> Flusher {
        let versions_path = Self::versions_path(&self.segment_path);
        let pending_versions = mem::take(&mut *self.pending_versions.lock());
        Box::new(move || {
            if pending_versions.is_empty() {
                return Ok(());
            }

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(versions_path)?;
            let mut writer = BufWriter::new(file);

            for change in pending_versions {
                let entry = serde_json::to_string(&change)?;
                debug_assert!(
                    !entry.contains('\n'),
                    "serialized version change entry cannot contain new line",
                );
                writer.write_all(entry.as_bytes())?;
                writer.write_all(b"\n")?;
            }

            writer.flush()?;
            drop(writer);

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
            Self::mappings_path(&self.segment_path),
            Self::versions_path(&self.segment_path),
        ]
    }
}
