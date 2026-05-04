use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{ReadBytesExt, WriteBytesExt};
use common::bitvec::{BitSliceExt as _, BitVec};
use common::types::PointOffsetType;
use uuid::Uuid;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::external_to_internal::CompressedExternalToInternal;
use crate::id_tracker::compressed::internal_to_external::CompressedInternalToExternal;
use crate::id_tracker::point_mappings::FileEndianess;
use crate::types::{ExtendedPointId, PointIdType};

pub const MAPPINGS_FILE_NAME: &str = "id_tracker.mappings";

pub(crate) fn mappings_path(base: &Path) -> PathBuf {
    base.join(MAPPINGS_FILE_NAME)
}

#[derive(Copy, Clone)]
#[repr(u8)]
enum ExternalIdType {
    Number = 0,
    Uuid = 1,
}

impl ExternalIdType {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            x if x == Self::Number as u8 => Some(Self::Number),
            x if x == Self::Uuid as u8 => Some(Self::Uuid),
            _ => None,
        }
    }

    fn from_point_id(point_id: &PointIdType) -> Self {
        match point_id {
            PointIdType::NumId(_) => Self::Number,
            PointIdType::Uuid(_) => Self::Uuid,
        }
    }
}

/// Loads a `CompressedPointMappings` from the given reader. Applies an optional filter of deleted items
/// to prevent allocating unneeded data.
pub(super) fn load_mapping<R: BufRead>(
    mut reader: R,
    deleted: Option<BitVec>,
) -> OperationResult<CompressedPointMappings> {
    // Deserialize the header
    let len = reader.read_u64::<FileEndianess>()? as usize;

    let mut deleted = deleted.unwrap_or_else(|| BitVec::repeat(false, len));

    deleted.truncate(len);

    let mut internal_to_external = CompressedInternalToExternal::with_capacity(len);
    let mut external_to_internal_num: Vec<(u64, PointOffsetType)> = Vec::new();
    let mut external_to_internal_uuid: Vec<(Uuid, PointOffsetType)> = Vec::new();

    // Deserialize the list entries
    for i in 0..len {
        let (internal_id, external_id) = read_entry(&mut reader)
            .map_err(|err| {
                OperationError::inconsistent_storage(format!("Immutable ID tracker failed to read next mapping, reading {} out of {len}, assuming malformed storage: {err}", i + 1))
            })?;

        // Need to push this regardless of point deletion as the vecs index represents the internal id
        // which would become wrong if we leave out entries.
        if internal_to_external.len() <= internal_id as usize {
            internal_to_external.resize(internal_id as usize + 1, PointIdType::NumId(0));
        }

        internal_to_external.set(internal_id, external_id);

        let point_deleted = deleted.get_bit(i).unwrap_or(false);
        if point_deleted {
            continue;
        }

        match external_id {
            ExtendedPointId::NumId(num) => {
                external_to_internal_num.push((num, internal_id));
            }
            ExtendedPointId::Uuid(uuid) => {
                external_to_internal_uuid.push((uuid, internal_id));
            }
        }
    }

    // Check that the file has been fully read.
    #[cfg(debug_assertions)] // Only for dev builds
    {
        debug_assert_eq!(reader.bytes().map(Result::unwrap).count(), 0,);
    }

    let external_to_internal = CompressedExternalToInternal::from_vectors(
        external_to_internal_num,
        external_to_internal_uuid,
    );

    Ok(CompressedPointMappings::new(
        deleted,
        internal_to_external,
        external_to_internal,
    ))
}

/// Loads a single entry from a reader. Expects the reader to be aligned so, that the next read
/// byte is the first byte of a new entry.
/// This function reads exact one entry which means after calling this function, the reader
/// will be at the start of the next entry.
pub(super) fn read_entry<R: Read>(
    mut reader: R,
) -> OperationResult<(PointOffsetType, ExtendedPointId)> {
    let point_id_type = reader.read_u8().map_err(|err| {
        OperationError::inconsistent_storage(format!(
            "failed to read point ID type from file: {err}"
        ))
    })?;

    let external_id = match ExternalIdType::from_byte(point_id_type) {
        None => {
            return Err(OperationError::inconsistent_storage(
                "invalid byte for point ID type",
            ));
        }
        Some(ExternalIdType::Number) => {
            let num = reader.read_u64::<FileEndianess>().map_err(|err| {
                OperationError::inconsistent_storage(format!(
                    "failed to read numeric point ID from file: {err}"
                ))
            })?;
            PointIdType::NumId(num)
        }
        Some(ExternalIdType::Uuid) => {
            let uuid_u128 = reader.read_u128::<FileEndianess>().map_err(|err| {
                OperationError::inconsistent_storage(format!(
                    "failed to read UUID point ID from file: {err}"
                ))
            })?;
            PointIdType::Uuid(Uuid::from_u128_le(uuid_u128))
        }
    };

    let internal_id = reader.read_u32::<FileEndianess>().map_err(|err| {
        OperationError::inconsistent_storage(format!(
            "failed to read internal point ID from file: {err}"
        ))
    })? as PointOffsetType;
    Ok((internal_id, external_id))
}

/// Serializes the `PointMappings` into the given writer using the file format specified below.
///
/// ## File format
/// In general the format looks like this:
/// +---------------------------+-----------------+
/// | Header (list length: u64) | List of entries |
/// +---------------------------+-----------------+
///
/// A single list entry:
/// +-----------------+-----------------------+------------------+
/// | PointIdType: u8 | Number/UUID: u64/u128 | Internal ID: u32 |
/// +-----------------+-----------------------+------------------+
/// A single entry is thus either 1+8+4=13 or 1+16+4=21 bytes in size depending
/// on the PointIdType.
pub(super) fn store_mapping<W: Write>(
    mappings: &CompressedPointMappings,
    mut writer: W,
) -> OperationResult<()> {
    let number_of_entries = mappings.total_point_count();

    // Serialize the header (=length).
    writer.write_u64::<FileEndianess>(number_of_entries as u64)?;

    // Serialize all entries
    for (internal_id, external_id) in mappings.iter_internal_raw() {
        write_entry(&mut writer, internal_id, external_id)?;
    }

    Ok(())
}

pub(super) fn write_entry<W: Write>(
    mut writer: W,
    internal_id: PointOffsetType,
    external_id: PointIdType,
) -> OperationResult<()> {
    // Byte to distinguish between Number and UUID
    writer.write_u8(ExternalIdType::from_point_id(&external_id) as u8)?;

    // Serializing External ID
    match external_id {
        PointIdType::NumId(num) => {
            // The PointID's number
            writer.write_u64::<FileEndianess>(num)?;
        }
        PointIdType::Uuid(uuid) => {
            // The PointID's UUID
            writer.write_u128::<FileEndianess>(uuid.to_u128_le())?;
        }
    }

    // Serializing Internal ID
    writer.write_u32::<FileEndianess>(internal_id)?;

    Ok(())
}
