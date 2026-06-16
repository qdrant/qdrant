use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};
use common::types::PointOffsetType;
use uuid::Uuid;

use crate::common::operation_error::OperationResult;
use crate::id_tracker::point_mappings::FileEndianess;
use crate::types::PointIdType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum MappingChange {
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
pub(super) enum MappingChangeType {
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

/// Deserialize a single mapping change entry from the given reader
///
/// This function reads exact one entry which means after calling this function, the reader
/// will be at the start of the next entry.
///
/// The number of bytes read is returned on successful read.
pub(super) fn read_entry<R: Read>(reader: &mut R) -> io::Result<(MappingChange, u64)> {
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
pub(super) fn write_entry<W: Write>(mut writer: W, change: MappingChange) -> OperationResult<()> {
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
