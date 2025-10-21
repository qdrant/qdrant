use std::collections::BTreeMap;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::mem;
use std::path::{Path, PathBuf};

use bitvec::prelude::{BitSlice, BitVec};
use byteorder::{ReadBytesExt, WriteBytesExt};
use common::types::PointOffsetType;
use fs_err::File;
use itertools::Itertools;
use memory::fadvise::OneshotFile;
use parking_lot::Mutex;
use uuid::Uuid;

use super::point_mappings::FileEndianess;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::point_mappings::PointMappings;
use crate::id_tracker::{DELETED_POINT_VERSION, IdTracker};
use crate::types::{PointIdType, SeqNumberType};

const FILE_MAPPINGS: &str = "mutable_id_tracker.mappings";
const FILE_VERSIONS: &str = "mutable_id_tracker.versions";

const VERSION_ELEMENT_SIZE: u64 = size_of::<SeqNumberType>() as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MappingChange {
    Insert(PointIdType, PointOffsetType),
    Delete(PointIdType),
}

impl MappingChange {
    fn change_type(&self) -> MappingChangeType {
        match self {
            Self::Insert(PointIdType::NumId(_), _) => MappingChangeType::InsertNum,
            Self::Insert(PointIdType::Uuid(_), _) => MappingChangeType::InsertUuid,
            Self::Delete(PointIdType::NumId(_)) => MappingChangeType::DeleteNum,
            Self::Delete(PointIdType::Uuid(_)) => MappingChangeType::DeleteUuid,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum MappingChangeType {
    InsertNum = 1,
    InsertUuid = 2,
    DeleteNum = 3,
    DeleteUuid = 4,
}

impl MappingChangeType {
    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            x if x == Self::InsertNum as u8 => Some(Self::InsertNum),
            x if x == Self::InsertUuid as u8 => Some(Self::InsertUuid),
            x if x == Self::DeleteNum as u8 => Some(Self::DeleteNum),
            x if x == Self::DeleteUuid as u8 => Some(Self::DeleteUuid),
            _ => None,
        }
    }

    /// Get size of the persisted operation in bytes
    ///
    /// +-----------------------+-----------------------+------------------+
    /// | MappingChangeType: u8 | Number/UUID: u64/u128 | Internal ID: u32 |
    /// +-----------------------+-----------------------+------------------+
    ///
    /// Deletion changes are serialized as follows:
    ///
    /// +-----------------------+-----------------------+
    /// | MappingChangeType: u8 | Number/UUID: u64/u128 |
    /// +-----------------------+-----------------------+
    const fn operation_size(self) -> usize {
        match self {
            Self::InsertNum => size_of::<u8>() + size_of::<u64>() + size_of::<u32>(),
            Self::InsertUuid => size_of::<u8>() + size_of::<u128>() + size_of::<u32>(),
            Self::DeleteNum => size_of::<u8>() + size_of::<u64>(),
            Self::DeleteUuid => size_of::<u8>() + size_of::<u128>(),
        }
    }
}

/// Mutable in-memory ID tracker with simple file based backing storage
///
/// This ID tracker simply persists all recorded point mapping and versions changes to disk by
/// appending these changes to a file. When loading, all mappings and versions are deduplicated in
/// memory so that only the latest mappings for a point are kept.
///
/// This structure may grow forever by collecting changes. It therefore relies on the optimization
/// processes in Qdrant to eventually vacuum the segment this ID tracker belongs to. Reoptimization
/// will clear all collected changes and start from scratch.
///
/// This ID tracker primarily replaces [`SimpleIdTracker`], so that we can eliminate the use of
/// RocksDB.
#[derive(Debug)]
pub struct MutableIdTracker {
    segment_path: PathBuf,
    internal_to_version: Vec<SeqNumberType>,
    pub(super) mappings: PointMappings,

    /// List of point versions pending to be persisted, will be persisted on flush
    pending_versions: Mutex<BTreeMap<PointOffsetType, SeqNumberType>>,

    /// List of point mappings pending to be persisted, will be persisted on flush
    pending_mappings: Mutex<Vec<MappingChange>>,
}

impl MutableIdTracker {
    pub fn open(segment_path: impl Into<PathBuf>) -> OperationResult<Self> {
        let segment_path = segment_path.into();

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
                "Missing versions file for ID tracker, assuming automatic point mappings and version recovery by WAL",
            );
        }

        let mappings = if has_mappings {
            load_mappings(&mappings_path).map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker mappings: {err}"))
            })?
        } else {
            PointMappings::default()
        };

        let internal_to_version = if has_versions {
            load_versions(&versions_path).map_err(|err| {
                OperationError::service_error(format!("Failed to load ID tracker versions: {err}"))
            })?
        } else {
            vec![]
        };

        // Compare internal point mappings and versions count, report warning if we don't
        debug_assert!(
            mappings.total_point_count() >= internal_to_version.len(),
            "can never have more versions than internal point mappings",
        );
        if mappings.total_point_count() != internal_to_version.len() {
            log::warn!(
                "Mutable ID tracker mappings and versions count mismatch, could have been partially flushed, assuming automatic recovery by WAL ({} mappings, {} versions)",
                mappings.total_point_count(),
                internal_to_version.len(),
            );
        }

        #[cfg(debug_assertions)]
        mappings.assert_mappings();

        Ok(Self {
            segment_path,
            internal_to_version,
            mappings,
            pending_versions: Default::default(),
            pending_mappings: Default::default(),
        })
    }

    pub fn segment_files(segment_path: &Path) -> Vec<PathBuf> {
        [mappings_path(segment_path), versions_path(segment_path)]
            .into_iter()
            .filter(|path| path.is_file())
            .collect()
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
        self.pending_versions.lock().insert(internal_id, version);
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
        let internal_id = self.mappings.drop(external_id);
        self.pending_mappings
            .lock()
            .push(MappingChange::Delete(external_id));
        if let Some(internal_id) = internal_id {
            self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;
        }
        Ok(())
    }

    fn drop_internal(&mut self, internal_id: PointOffsetType) -> OperationResult<()> {
        if let Some(external_id) = self.mappings.external_id(internal_id) {
            self.mappings.drop(external_id);
            self.pending_mappings
                .lock()
                .push(MappingChange::Delete(external_id));
        }

        self.set_internal_version(internal_id, DELETED_POINT_VERSION)?;

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

            store_mapping_changes(&mappings_path, pending_mappings)
        })
    }

    /// Creates a flusher function, that persists the removed points in the version database
    /// and flushes the version database to disk.
    /// This function should be called _after_ flushing the mapping database.
    fn versions_flusher(&self) -> Flusher {
        let versions_path = versions_path(&self.segment_path);
        let pending_versions = mem::take(&mut *self.pending_versions.lock());

        Box::new(move || {
            if pending_versions.is_empty() {
                return Ok(());
            }

            store_version_changes(&versions_path, pending_versions)
        })
    }

    fn is_deleted_point(&self, key: PointOffsetType) -> bool {
        self.mappings.is_deleted_point(key)
    }

    fn deleted_point_bitslice(&self) -> &BitSlice {
        self.mappings.deleted()
    }

    fn iter_internal_versions(
        &self,
    ) -> Box<dyn Iterator<Item = (PointOffsetType, SeqNumberType)> + '_> {
        Box::new(
            self.internal_to_version
                .iter()
                .enumerate()
                .map(|(i, version)| (i as PointOffsetType, *version)),
        )
    }

    fn name(&self) -> &'static str {
        "mutable id tracker"
    }

    #[inline]
    fn files(&self) -> Vec<PathBuf> {
        Self::segment_files(&self.segment_path)
    }
}

pub(crate) fn mappings_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_MAPPINGS)
}

fn versions_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_VERSIONS)
}

/// Store new mapping changes, appending them to the given file
fn store_mapping_changes(mappings_path: &Path, changes: Vec<MappingChange>) -> OperationResult<()> {
    // Create or open file in append mode to write new changes to the end
    let file = File::options()
        .create(true)
        .append(true)
        .open(mappings_path)?;
    let mut writer = BufWriter::new(file);

    write_mapping_changes(&mut writer, changes).map_err(|err| {
        OperationError::service_error(format!(
            "Failed to persist ID tracker point mappings ({}): {err}",
            mappings_path.display(),
        ))
    })?;

    // Explicitly fsync file contents to ensure durability
    writer.flush()?;
    let file = writer.into_inner().unwrap();
    file.sync_all().map_err(|err| {
        OperationError::service_error(format!("Failed to fsync ID tracker point mappings: {err}"))
    })?;

    Ok(())
}

/// Serializes pending point mapping changes into the given writer
///
/// ## File format
///
/// All entries have a variable size and are simply concatenated. Each entry has a 1-byte header
/// which specifies the change type and implies the length of the entry.
///
/// See [`read_entry`] and [`write_entry`] for more details.
fn write_mapping_changes<W: Write>(
    mut writer: W,
    changes: Vec<MappingChange>,
) -> OperationResult<()> {
    for change in changes {
        write_entry(&mut writer, change)?;
    }

    // Explicitly flush writer to catch IO errors
    writer.flush()?;

    Ok(())
}

/// Load point mappings from the given file
///
/// If the file ends with an incomplete entry, it is truncated from the file.
fn load_mappings(mappings_path: &Path) -> OperationResult<PointMappings> {
    let file = OneshotFile::open(mappings_path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mappings = read_mappings(&mut reader)?;

    let read_to = reader.stream_position()?;
    reader.into_inner().drop_cache()?;

    // If reader is not fully exhausted, there's an incomplete entry at the end, truncate the file
    // It can happen on crash while flushing. We must truncate the file here to not corrupt new
    // entries we append to the file
    debug_assert!(read_to <= file_len, "cannot read past the end of the file");
    if read_to < file_len {
        log::warn!(
            "Mutable ID tracker mappings file ends with incomplete entry, removing last {} bytes and assuming automatic recovery by WAL",
            file_len - read_to,
        );
        let file = File::options()
            .write(true)
            .truncate(false)
            .open(mappings_path)?;
        file.set_len(read_to)?;
        file.sync_all()?;
    }

    Ok(mappings)
}

/// Iterate over mapping changes from the given reader
///
/// Each non-errorous item is a tuple of the mapping change and the number of bytes read so far.
///
/// The iterator ends when the end of the file is reached, or when an error occurred.
///
/// ## Error
///
/// An error item is returned if reading a mapping change fails due to malformed data. Then the
/// iterator will not produce any more items.
fn read_mappings_iter<R>(mut reader: R) -> impl Iterator<Item = OperationResult<MappingChange>>
where
    R: Read + Seek,
{
    let mut position = reader.stream_position().unwrap_or(0);

    // Keep reading until end of file or error
    std::iter::from_fn(move || match read_entry(&mut reader) {
        Ok((entry, read_bytes)) => {
            position += read_bytes;
            Some(Ok(entry))
        }
        // Done reading if end of file is reached
        // Explicitly seek to after last read entry, allows to detect if full file was read
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            match reader.seek(io::SeekFrom::Start(position)) {
                Ok(_) => None,
                Err(err) => Some(Err(err.into())),
            }
        }
        // Propagate deserialization error
        Err(err) => Some(Err(err.into())),
    })
    // Can't read any more data reliably after first error
    .take_while_inclusive(|item| item.is_ok())
}

/// Read point mappings from the given reader
///
/// Returns loaded point mappings.
fn read_mappings<R>(reader: R) -> OperationResult<PointMappings>
where
    R: Read + Seek,
{
    let mut deleted = BitVec::new();
    let mut internal_to_external: Vec<PointIdType> = Default::default();
    let mut external_to_internal_num: BTreeMap<u64, PointOffsetType> = Default::default();
    let mut external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType> = Default::default();

    for change in read_mappings_iter(reader) {
        match change? {
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

    let mappings = PointMappings::new(
        deleted,
        internal_to_external,
        external_to_internal_num,
        external_to_internal_uuid,
    );

    Ok(mappings)
}

/// Deserialize a single mapping change entry from the given reader
///
/// This function reads exact one entry which means after calling this function, the reader
/// will be at the start of the next entry.
///
/// The number of bytes read is returned on successful read.
fn read_entry<R: Read>(reader: &mut R) -> io::Result<(MappingChange, u64)> {
    let change_type = reader.read_u8()?;
    let change_type = MappingChangeType::from_byte(change_type).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Corrupted ID tracker mapping storage, got malformed mapping change byte {change_type:#04X}"),
        )
    })?;

    // Size of persisted operation in bytes
    let operation_size = change_type.operation_size() as u64;

    match change_type {
        MappingChangeType::InsertNum => {
            let external_id = PointIdType::NumId(reader.read_u64::<FileEndianess>()?);
            let internal_id = reader.read_u32::<FileEndianess>()? as PointOffsetType;
            Ok((
                MappingChange::Insert(external_id, internal_id),
                operation_size,
            ))
        }
        MappingChangeType::InsertUuid => {
            let external_id =
                PointIdType::Uuid(Uuid::from_u128_le(reader.read_u128::<FileEndianess>()?));
            let internal_id = reader.read_u32::<FileEndianess>()? as PointOffsetType;
            Ok((
                MappingChange::Insert(external_id, internal_id),
                operation_size,
            ))
        }
        MappingChangeType::DeleteNum => {
            let external_id = PointIdType::NumId(reader.read_u64::<FileEndianess>()?);
            Ok((MappingChange::Delete(external_id), operation_size))
        }
        MappingChangeType::DeleteUuid => {
            let external_id =
                PointIdType::Uuid(Uuid::from_u128_le(reader.read_u128::<FileEndianess>()?));
            Ok((MappingChange::Delete(external_id), operation_size))
        }
    }
}

/// Serialize a single mapping change and write it into the given writer
///
/// # File format
///
/// Each change entry has a variable size. We first write a 1-byte header to define the change
/// type. The change type implies how long the entry is.
///
/// Insertion changes are serialized as follows:
///
/// +-----------------------+-----------------------+------------------+
/// | MappingChangeType: u8 | Number/UUID: u64/u128 | Internal ID: u32 |
/// +-----------------------+-----------------------+------------------+
///
/// Deletion changes are serialized as follows:
///
/// +-----------------------+-----------------------+
/// | MappingChangeType: u8 | Number/UUID: u64/u128 |
/// +-----------------------+-----------------------+
fn write_entry<W: Write>(mut writer: W, change: MappingChange) -> OperationResult<()> {
    // Byte to identity type of change
    writer.write_u8(change.change_type() as u8)?;

    // Serialize mapping change
    match change {
        MappingChange::Insert(PointIdType::NumId(external_id), internal_id) => {
            writer.write_u64::<FileEndianess>(external_id)?;
            writer.write_u32::<FileEndianess>(internal_id)?;
        }
        MappingChange::Insert(PointIdType::Uuid(external_id), internal_id) => {
            writer.write_u128::<FileEndianess>(external_id.to_u128_le())?;
            writer.write_u32::<FileEndianess>(internal_id)?;
        }
        MappingChange::Delete(PointIdType::NumId(external_id)) => {
            writer.write_u64::<FileEndianess>(external_id)?;
        }
        MappingChange::Delete(PointIdType::Uuid(external_id)) => {
            writer.write_u128::<FileEndianess>(external_id.to_u128_le())?;
        }
    }

    Ok(())
}

fn load_versions(versions_path: &Path) -> OperationResult<Vec<SeqNumberType>> {
    let file = File::open(versions_path)?;

    let file_len = file.metadata()?.len();
    if file_len % VERSION_ELEMENT_SIZE != 0 {
        log::warn!(
            "Corrupted ID tracker versions storage, file size not a multiple of a version, assuming automatic recovery by WAL"
        );
    }
    let version_count = file_len / VERSION_ELEMENT_SIZE;

    let mut reader = BufReader::new(file);

    Ok((0..version_count)
        .map(|_| reader.read_u64::<FileEndianess>())
        .collect::<Result<_, _>>()?)
}

/// Store new version changes, appending them to the given file
fn store_version_changes(
    versions_path: &Path,
    changes: BTreeMap<PointOffsetType, SeqNumberType>,
) -> OperationResult<()> {
    if changes.is_empty() {
        return Ok(());
    }

    // Create or open file
    let file = File::options()
        .create(true)
        .write(true)
        .truncate(false)
        .open(versions_path)?;

    // Grow file if necessary in one shot
    // Prevents potentially reallocating the file multiple times when progressively writing changes
    match file.metadata() {
        Ok(metadata) => {
            let (&max_internal_id, _) = changes.last_key_value().unwrap();
            let required_size = u64::from(max_internal_id + 1) * VERSION_ELEMENT_SIZE;
            if metadata.len() < required_size {
                file.set_len(required_size)?;
            }
        }
        Err(err) => {
            log::warn!(
                "Failed to get file length of mutable ID tracker versions file, ignoring: {err}"
            );
        }
    }

    let mut writer = BufWriter::new(file);

    write_version_changes(&mut writer, changes).map_err(|err| {
        OperationError::service_error(format!(
            "Failed to persist ID tracker point versions ({}): {err}",
            versions_path.display(),
        ))
    })?;

    // Explicitly fsync file contents to ensure durability
    writer.flush().map_err(|err| {
        OperationError::service_error(format!(
            "Failed to flush ID tracker point versions write buffer: {err}",
        ))
    })?;
    let file = writer.into_inner().map_err(|err| err.into_error())?;
    file.sync_all().map_err(|err| {
        OperationError::service_error(format!("Failed to fsync ID tracker point versions: {err}"))
    })?;

    Ok(())
}

/// Serializes pending point version changes into the given writer
fn write_version_changes<W>(
    mut writer: W,
    changes: BTreeMap<PointOffsetType, SeqNumberType>,
) -> OperationResult<()>
where
    W: Write + Seek,
{
    let mut position = writer.stream_position()?;

    // Write all changes, must be ordered by internal ID, see optimization note below
    for (internal_id, version) in changes {
        let offset = u64::from(internal_id) * VERSION_ELEMENT_SIZE;

        // Seek to correct position if not already at it
        //
        // This assumes we're using a BufWriter. We only explicitly seek if not at the correct
        // position already, because seeking is expensive. When we seek it automatically flushes
        // our buffered writes to durable storage even if our position didn't change. This
        // optimization significantly improves performance when writing a large batch of versions
        // by reducing the number of flushes and syscalls.
        // See: <https://doc.rust-lang.org/std/io/trait.Seek.html#tymethod.seek>
        //
        // We track the position ourselves because using `stream_position()` as getter also invokes
        // a seek, causing an explicit flush.
        //
        // Now we only flush if:
        // - we seek to a new position because there's a gap in versions to update
        // - our write buffer is full
        // - after writing all versions
        if offset != position {
            position = writer.seek(io::SeekFrom::Start(offset))?;
        }

        // Write version and update position
        writer.write_u64::<FileEndianess>(version)?;
        position += VERSION_ELEMENT_SIZE;
    }

    // Explicitly flush writer to catch IO errors
    writer.flush()?;

    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::{HashMap, HashSet};
    use std::io::Cursor;

    use fs_err as fs;
    use itertools::Itertools;
    #[cfg(feature = "rocksdb")]
    use rand::Rng;
    use rand::prelude::*;
    use tempfile::Builder;
    use uuid::Uuid;

    use super::*;
    use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
    use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
    #[cfg(feature = "rocksdb")]
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;

    const RAND_SEED: u64 = 42;
    const DEFAULT_VERSION: SeqNumberType = 42;

    #[test]
    fn test_iterator() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let mut id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

        id_tracker.set_link(200.into(), 0).unwrap();
        id_tracker.set_link(100.into(), 1).unwrap();
        id_tracker.set_link(150.into(), 2).unwrap();
        id_tracker.set_link(120.into(), 3).unwrap();
        id_tracker.set_link(180.into(), 4).unwrap();
        id_tracker.set_link(110.into(), 5).unwrap();
        id_tracker.set_link(115.into(), 6).unwrap();
        id_tracker.set_link(190.into(), 7).unwrap();
        id_tracker.set_link(177.into(), 8).unwrap();
        id_tracker.set_link(118.into(), 9).unwrap();

        let first_four = id_tracker.iter_from(None).take(4).collect_vec();

        assert_eq!(first_four.len(), 4);
        assert_eq!(first_four[0].0, 100.into());

        let last = id_tracker.iter_from(Some(first_four[3].0)).collect_vec();
        assert_eq!(last.len(), 7);
    }

    pub const TEST_POINTS: &[PointIdType] = &[
        PointIdType::NumId(100),
        PointIdType::Uuid(Uuid::from_u128(123_u128)),
        PointIdType::Uuid(Uuid::from_u128(156_u128)),
        PointIdType::NumId(150),
        PointIdType::NumId(120),
        PointIdType::Uuid(Uuid::from_u128(12_u128)),
        PointIdType::NumId(180),
        PointIdType::NumId(110),
        PointIdType::NumId(115),
        PointIdType::Uuid(Uuid::from_u128(673_u128)),
        PointIdType::NumId(190),
        PointIdType::NumId(177),
        PointIdType::Uuid(Uuid::from_u128(971_u128)),
    ];

    #[test]
    fn test_mixed_types_iterator() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let id_tracker = make_mutable_tracker(segment_dir.path());

        let sorted_from_tracker = id_tracker.iter_from(None).map(|(k, _)| k).collect_vec();

        let mut values = TEST_POINTS.to_vec();
        values.sort();

        assert_eq!(sorted_from_tracker, values);
    }

    #[test]
    fn test_load_store() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (old_mappings, old_versions) = {
            let id_tracker = make_mutable_tracker(segment_dir.path());
            (id_tracker.mappings, id_tracker.internal_to_version)
        };

        let mut loaded_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

        assert_eq!(
            old_versions.len(),
            loaded_id_tracker.internal_to_version.len(),
        );
        for i in 0..old_versions.len() {
            assert_eq!(
                old_versions.get(i),
                loaded_id_tracker.internal_to_version.get(i),
                "Version mismatch at index {i}",
            );
        }

        assert_eq!(old_mappings, loaded_id_tracker.mappings);

        loaded_id_tracker.drop(PointIdType::NumId(180)).unwrap();
    }

    /// Mutates an ID tracker and stores it to disk. Tests whether loading results in the exact same
    /// ID tracker.
    #[test]
    fn test_store_load_mutated() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let (dropped_points, custom_version) = {
            let mut id_tracker = make_mutable_tracker(segment_dir.path());

            let mut dropped_points = HashSet::new();
            let mut custom_version = HashMap::new();

            for (index, point) in TEST_POINTS.iter().enumerate() {
                if index % 2 == 0 {
                    continue;
                }

                if index % 3 == 0 {
                    id_tracker.drop(*point).unwrap();
                    dropped_points.insert(*point);
                    continue;
                }

                if index % 5 == 0 {
                    let new_version = rng.next_u64();
                    id_tracker
                        .set_internal_version(index as PointOffsetType, new_version)
                        .unwrap();
                    custom_version.insert(index as PointOffsetType, new_version);
                }
            }

            id_tracker.mapping_flusher()().unwrap();
            id_tracker.versions_flusher()().unwrap();

            (dropped_points, custom_version)
        };

        let id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
        for (index, point) in TEST_POINTS.iter().enumerate() {
            let internal_id = index as PointOffsetType;

            if dropped_points.contains(point) {
                assert!(id_tracker.is_deleted_point(internal_id));
                assert_eq!(id_tracker.external_id(internal_id), None);
                assert!(id_tracker.mappings.internal_id(point).is_none());

                continue;
            }

            // Check version
            let expect_version = custom_version
                .get(&internal_id)
                .copied()
                .unwrap_or(DEFAULT_VERSION);

            assert_eq!(
                id_tracker.internal_version(internal_id),
                Some(expect_version),
            );

            // Check that unmodified points still haven't changed.
            assert_eq!(
                id_tracker.external_id(index as PointOffsetType),
                Some(*point),
            );
        }
    }

    #[test]
    fn test_all_points_have_version() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let id_tracker = make_mutable_tracker(segment_dir.path());
        for i in id_tracker.iter_internal() {
            assert!(id_tracker.internal_version(i).is_some());
        }
    }

    #[test]
    fn test_point_deletion_correctness() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut id_tracker = make_mutable_tracker(segment_dir.path());

        let deleted_points = id_tracker.total_point_count() - id_tracker.available_point_count();

        let point_to_delete = PointIdType::NumId(100);

        assert!(id_tracker.iter_external().contains(&point_to_delete));

        assert_eq!(id_tracker.internal_id(point_to_delete), Some(0));

        id_tracker.drop(point_to_delete).unwrap();

        let point_exists = id_tracker.internal_id(point_to_delete).is_some()
            && id_tracker.iter_external().contains(&point_to_delete)
            && id_tracker.iter_from(None).any(|i| i.0 == point_to_delete);

        assert!(!point_exists);

        let new_deleted_points =
            id_tracker.total_point_count() - id_tracker.available_point_count();

        assert_eq!(new_deleted_points, deleted_points + 1);
    }

    #[test]
    fn test_point_deletion_persists_reload() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

        let point_to_delete = PointIdType::NumId(100);

        let old_mappings = {
            let mut id_tracker = make_mutable_tracker(segment_dir.path());
            let intetrnal_id = id_tracker
                .internal_id(point_to_delete)
                .expect("Point to delete exists.");
            assert!(!id_tracker.is_deleted_point(intetrnal_id));
            id_tracker.drop(point_to_delete).unwrap();
            id_tracker.mapping_flusher()().unwrap();
            id_tracker.versions_flusher()().unwrap();
            id_tracker.mappings
        };

        // Point should still be gone
        let id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
        assert_eq!(id_tracker.internal_id(point_to_delete), None);

        old_mappings
            .iter_internal_raw()
            .zip(id_tracker.mappings.iter_internal_raw())
            .for_each(
                |((old_internal, old_external), (new_internal, new_external))| {
                    assert_eq!(old_internal, new_internal);
                    assert_eq!(old_external, new_external);
                },
            );
    }

    /// Tests de/serializing of only single ID mappings.
    #[test]
    fn test_point_mappings_de_serialization_single() {
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        const SIZE: usize = 400_000;

        let mappings = CompressedPointMappings::random(&mut rng, SIZE as u32);

        for i in 0..SIZE {
            let mut buf = vec![];

            let internal_id = i as PointOffsetType;

            let expected_external = mappings.external_id(internal_id).unwrap();

            let change = MappingChange::Insert(expected_external, internal_id);

            write_entry(&mut buf, change).unwrap();

            let (got_change, bytes_read) = read_entry(&mut buf.as_slice()).unwrap();

            assert_eq!(change, got_change);
            assert!(bytes_read == 13 || bytes_read == 21);
        }
    }

    /// Some more special test cases for deserializing point mappings.
    #[test]
    fn test_point_mappings_deserializing_special() {
        // Empty reader creates empty mappings
        let buf = Cursor::new(b"");
        assert_eq!(read_mappings(buf).unwrap().total_point_count(), 0);

        // Corrupt if reading invalid type byte
        let buf = Cursor::new(b"\x00");
        assert!(
            read_mappings(buf)
                .unwrap_err()
                .to_string()
                .contains("Corrupted ID tracker mapping storage")
        );

        let buf = Cursor::new(b"malformed!");
        assert!(read_mappings(buf).is_err());

        // Empty if change is not fully written
        let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00");
        assert_eq!(read_mappings(buf).unwrap().total_point_count(), 0);

        // Exactly one entry
        let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00");
        assert_eq!(
            read_mappings(buf)
                .unwrap()
                .internal_id(&PointIdType::NumId(1)),
            Some(2)
        );

        // Exactly one entry and a malformed second one
        let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x01\x00");
        assert!(
            read_mappings(buf)
                .unwrap_err()
                .to_string()
                .contains("Corrupted ID tracker mapping storage")
        );

        // Exactly one entry and an incomplete second one
        let buf = Cursor::new(b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x01\x00");
        let mappings = read_mappings(buf).unwrap();
        assert_eq!(mappings.total_point_count(), 1);
        assert_eq!(mappings.internal_id(&PointIdType::NumId(1)), Some(0));
    }

    /// Test that `operation_size` returns the correct size
    ///
    /// Must have read exactly the same number of bytes from the stream as we return.
    #[test]
    fn test_operation_size() {
        let items = [
            (
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00".as_slice(),
                MappingChange::Insert(PointIdType::NumId(1), 2),
            ),
            (
                b"\x02\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00".as_slice(),
                MappingChange::Insert(
                    PointIdType::Uuid(Uuid::parse_str("10000000-0000-0000-0000-000000000000").unwrap()),
                    2,
                ),
            ),
            (
                b"\x03\x01\x00\x00\x00\x00\x00\x00\x00".as_slice(),
                MappingChange::Delete(PointIdType::NumId(1))
            ),
            (
                b"\x04\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00".as_slice(),
                MappingChange::Delete(PointIdType::Uuid(
                    Uuid::parse_str("10000000-0000-0000-0000-000000000000").unwrap(),
                )),
            ),
        ];

        // Test each change type
        for (buf, expected_change) in items {
            let mut buf = Cursor::new(buf);
            let (entry, bytes_read) = read_entry(&mut buf).unwrap();
            assert_eq!(entry, expected_change);
            assert_eq!(
                bytes_read,
                buf.stream_position().unwrap(),
                "read different number of bytes from stream than returned",
            );
        }
    }

    /// Test that we truncate a partially written entry at the end.
    #[test]
    fn test_point_mappings_truncation() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mappings_path = mappings_path(segment_dir.path());

        // Exactly one entry
        fs::write(
            &mappings_path,
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00",
        )
        .unwrap();
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);
        assert_eq!(
            load_mappings(&mappings_path)
                .unwrap()
                .internal_id(&PointIdType::NumId(1)),
            Some(2)
        );
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

        // One entry and and one extra byte, file must be truncated
        fs::write(
            &mappings_path,
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01",
        )
        .unwrap();
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 14);
        assert_eq!(
            load_mappings(&mappings_path)
                .unwrap()
                .internal_id(&PointIdType::NumId(1)),
            Some(2)
        );
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

        // One entry and an incomplete second one, file must be truncated
        fs::write(
            &mappings_path,
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00",
        )
        .unwrap();
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 16);
        assert_eq!(
            load_mappings(&mappings_path)
                .unwrap()
                .internal_id(&PointIdType::NumId(1)),
            Some(2)
        );
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 13);

        // Two entries and an incomplete third one, file must be truncated
        fs::write(
            &mappings_path,
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00",
        ).unwrap();
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 28);
        assert_eq!(
            load_mappings(&mappings_path)
                .unwrap()
                .internal_id(&PointIdType::NumId(1)),
            Some(2)
        );
        assert_eq!(fs::metadata(&mappings_path).unwrap().len(), 26);
    }

    fn make_in_memory_tracker_from_memory() -> InMemoryIdTracker {
        let mut id_tracker = InMemoryIdTracker::new();

        for value in TEST_POINTS.iter() {
            let internal_id = id_tracker.total_point_count() as PointOffsetType;
            id_tracker.set_link(*value, internal_id).unwrap();
            id_tracker
                .set_internal_version(internal_id, DEFAULT_VERSION)
                .unwrap()
        }

        id_tracker
    }

    fn make_mutable_tracker(path: &Path) -> MutableIdTracker {
        let mut id_tracker =
            MutableIdTracker::open(path).expect("failed to open mutable ID tracker");

        for value in TEST_POINTS.iter() {
            let internal_id = id_tracker.total_point_count() as PointOffsetType;
            id_tracker.set_link(*value, internal_id).unwrap();
            id_tracker
                .set_internal_version(internal_id, DEFAULT_VERSION)
                .unwrap()
        }

        id_tracker.mapping_flusher()().expect("failed to flush ID tracker mappings");
        id_tracker.versions_flusher()().expect("failed to flush ID tracker versions");

        id_tracker
    }

    #[test]
    fn test_id_tracker_equal() {
        let in_memory_id_tracker = make_in_memory_tracker_from_memory();

        let mutable_id_tracker_dir = Builder::new()
            .prefix("segment_dir_mutable")
            .tempdir()
            .unwrap();
        let mutable_id_tracker = make_mutable_tracker(mutable_id_tracker_dir.path());

        assert_eq!(
            in_memory_id_tracker.available_point_count(),
            mutable_id_tracker.available_point_count(),
        );
        assert_eq!(
            in_memory_id_tracker.total_point_count(),
            mutable_id_tracker.total_point_count(),
        );

        for (internal, external) in TEST_POINTS.iter().enumerate() {
            let internal = internal as PointOffsetType;

            assert_eq!(
                in_memory_id_tracker.internal_id(*external),
                mutable_id_tracker.internal_id(*external),
            );

            assert_eq!(
                in_memory_id_tracker
                    .internal_version(internal)
                    .unwrap_or_default(),
                mutable_id_tracker
                    .internal_version(internal)
                    .unwrap_or_default(),
            );

            assert_eq!(
                in_memory_id_tracker.external_id(internal),
                mutable_id_tracker.external_id(internal),
            );
        }
    }

    #[test]
    #[cfg(feature = "rocksdb")]
    fn simple_id_tracker_vs_mutable_tracker_congruence() {
        use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};

        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let db = open_db(segment_dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut mutable_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();
        let mut simple_id_tracker = SimpleIdTracker::open(db).unwrap();

        // Insert 100 random points into id_tracker

        let num_points = 200;
        let mut rng = StdRng::seed_from_u64(RAND_SEED);

        for _ in 0..num_points {
            // Generate num id in range from 0 to 100

            let point_id = PointIdType::NumId(rng.random_range(0..num_points as u64));

            let version = rng.random_range(0..1000);

            let internal_id_mmap = mutable_id_tracker.total_point_count() as PointOffsetType;
            let internal_id_simple = simple_id_tracker.total_point_count() as PointOffsetType;

            assert_eq!(internal_id_mmap, internal_id_simple);

            if mutable_id_tracker.internal_id(point_id).is_some() {
                mutable_id_tracker.drop(point_id).unwrap();
            }
            mutable_id_tracker
                .set_link(point_id, internal_id_mmap)
                .unwrap();
            mutable_id_tracker
                .set_internal_version(internal_id_mmap, version)
                .unwrap();

            if simple_id_tracker.internal_id(point_id).is_some() {
                simple_id_tracker.drop(point_id).unwrap();
            }
            simple_id_tracker
                .set_link(point_id, internal_id_simple)
                .unwrap();
            simple_id_tracker
                .set_internal_version(internal_id_simple, version)
                .unwrap();
        }

        fn check_trackers(a: &SimpleIdTracker, b: &MutableIdTracker) {
            for (external_id, internal_id) in a.iter_from(None) {
                assert_eq!(
                    a.internal_version(internal_id).unwrap(),
                    b.internal_version(internal_id).unwrap()
                );
                assert_eq!(a.external_id(internal_id), b.external_id(internal_id));
                assert_eq!(external_id, b.external_id(internal_id).unwrap());
                assert_eq!(
                    a.external_id(internal_id).unwrap(),
                    b.external_id(internal_id).unwrap()
                );
            }

            for (external_id, internal_id) in b.iter_from(None) {
                assert_eq!(
                    a.internal_version(internal_id).unwrap(),
                    b.internal_version(internal_id).unwrap()
                );
                assert_eq!(a.external_id(internal_id), b.external_id(internal_id));
                assert_eq!(external_id, a.external_id(internal_id).unwrap());
                assert_eq!(
                    a.external_id(internal_id).unwrap(),
                    b.external_id(internal_id).unwrap()
                );
            }
        }

        check_trackers(&simple_id_tracker, &mutable_id_tracker);

        // Persist and reload mutable tracker and test again
        mutable_id_tracker.mapping_flusher()().unwrap();
        mutable_id_tracker.versions_flusher()().unwrap();
        drop(mutable_id_tracker);
        let mutable_id_tracker = MutableIdTracker::open(segment_dir.path()).unwrap();

        check_trackers(&simple_id_tracker, &mutable_id_tracker);
    }
}
