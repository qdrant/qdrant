use std::collections::BTreeMap;
use std::io::{self, BufReader, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};

use byteorder::{ReadBytesExt, WriteBytesExt};
use common::types::PointOffsetType;
use fs_err::File;
use parking_lot::Mutex;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::point_mappings::FileEndianess;
use crate::types::SeqNumberType;

const FILE_VERSIONS: &str = "mutable_id_tracker.versions";

pub(super) const VERSION_ELEMENT_SIZE: u64 = size_of::<SeqNumberType>() as u64;

pub(super) fn versions_path(segment_path: &Path) -> PathBuf {
    segment_path.join(FILE_VERSIONS)
}

pub(super) fn load_versions(versions_path: &Path) -> OperationResult<Vec<SeqNumberType>> {
    let file = File::open(versions_path)?;

    let file_len = file.metadata()?.len();
    if file_len % VERSION_ELEMENT_SIZE != 0 {
        log::warn!(
            "Mutable ID tracker versions file has partial trailing entry, ignoring last {} bytes (will be cleaned up on next flush)",
            file_len % VERSION_ELEMENT_SIZE,
        );
    }
    let version_count = file_len / VERSION_ELEMENT_SIZE;

    let mut reader = BufReader::new(file);

    Ok((0..version_count)
        .map(|_| reader.read_u64::<FileEndianess>())
        .collect::<Result<_, _>>()?)
}

/// Store new version changes, appending them to the given file
pub(super) fn store_version_changes(
    versions_path: &Path,
    changes: &BTreeMap<PointOffsetType, SeqNumberType>,
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

    // Truncate partial trailing entry if present (e.g. from a previous crash mid-write).
    // Must be done before writing to prevent zero-fill from merging with partial bytes
    // into a corrupt-but-complete-looking entry when extending the file.
    let file_len = file.metadata()?.len();
    let valid_len = (file_len / VERSION_ELEMENT_SIZE) * VERSION_ELEMENT_SIZE;
    if file_len != valid_len {
        log::warn!(
            "Mutable ID tracker versions file has partial trailing entry ({} extra bytes), truncating",
            file_len - valid_len,
        );
        file.set_len(valid_len)?;
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
    changes: &BTreeMap<PointOffsetType, SeqNumberType>,
) -> OperationResult<()>
where
    W: Write + Seek,
{
    let mut position = writer.stream_position()?;

    // Write all changes, must be ordered by internal ID, see optimization note below
    for (&internal_id, &version) in changes {
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

pub(super) fn reconcile_persisted_version_changes(
    pending: &Mutex<BTreeMap<PointOffsetType, SeqNumberType>>,
    changes: BTreeMap<PointOffsetType, SeqNumberType>,
) {
    pending.lock().retain(|point_offset, pending_version| {
        changes
            .get(point_offset)
            .is_none_or(|persisted_version| pending_version != persisted_version)
    });
}
