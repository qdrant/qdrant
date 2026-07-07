//! On-disk mapping format for the disk-resident id tracker: a random-access
//! layout that lets the tracker answer lookups without holding the mapping in
//! RAM.
//!
//! Two files hold the mapping (alongside the reused `id_tracker.versions` and
//! `id_tracker.deleted`):
//!
//! - [`I2E_FILE_NAME`] — internal→external as a fixed-width `u128` array plus an
//!   `is_uuid` bit per slot (a direct transcription of
//!   [`CompressedInternalToExternal`]). `external_id(offset)` is one 16-byte read.
//! - [`E2I_FILE_NAME`] — external→internal as two sorted, fixed-width runs
//!   (`(u64, u32)` for numeric ids, then `(u128, u32)` for UUIDs — the section
//!   order encodes "any UUID sorts after any numeric id"), preceded by a sparse
//!   block index (first key of every ~16 KiB block). A point lookup is an
//!   in-RAM sparse-index search plus one data-block read.
//!
//! [`CompressedInternalToExternal`]:
//!   crate::id_tracker::compressed::internal_to_external::CompressedInternalToExternal

use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use common::bitvec::{BitSlice, BitSliceExt as _, BitVec};
use common::types::PointOffsetType;
use uuid::Uuid;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::types::PointIdType;

type Endian = LittleEndian;

pub const I2E_FILE_NAME: &str = "id_tracker.i2e";
pub const E2I_FILE_NAME: &str = "id_tracker.e2i";

pub fn i2e_path(base: &Path) -> PathBuf {
    base.join(I2E_FILE_NAME)
}

pub fn e2i_path(base: &Path) -> PathBuf {
    base.join(E2I_FILE_NAME)
}

/// On-disk format version, stored in each file header for forward compatibility.
pub const ON_DISK_FORMAT_VERSION: u32 = 1;

const I2E_MAGIC: u64 = 0x5144_5F49_3245_0001;
const E2I_MAGIC: u64 = 0x5144_5F45_3249_0001;

/// Header size in bytes for the i2e file:
/// magic(8) + version(4) + reserved(4) + total(8) + reserved(8).
///
/// Headers and every section start are aligned to [`SECTION_ALIGN`] so the
/// files stay transmute-friendly if we ever mmap them (`u128` needs 16-byte
/// alignment in Rust).
pub const I2E_HEADER_SIZE: u64 = 32;
/// Header size in bytes for the e2i file:
/// magic(8) + version(4) + reserved(4) + num_count(8) + uuid_count(8) + num_bs(4) + uuid_bs(4)
/// + reserved(8).
pub const E2I_HEADER_SIZE: u64 = 48;

/// Alignment (bytes) of headers and section starts within both files.
pub const SECTION_ALIGN: u64 = 16;

/// Round `offset` up to the next [`SECTION_ALIGN`] boundary.
fn align_section(offset: u64) -> u64 {
    offset.next_multiple_of(SECTION_ALIGN)
}

/// Byte width of one numeric run entry: `u64` key + `u32` offset.
pub const NUM_ENTRY_SIZE: u64 = 12;
/// Byte width of one UUID run entry: `u128` key + `u32` offset.
pub const UUID_ENTRY_SIZE: u64 = 20;

/// Entries per numeric block, chosen so a block is ~16 KiB (the `DiskCache` block
/// size), i.e. one logical block ≈ one cache block ≈ one remote range read.
pub const NUM_BLOCK_ENTRIES: u32 = 1365; // * 12 = 16380 bytes
/// Entries per UUID block, same ~16 KiB target.
pub const UUID_BLOCK_ENTRIES: u32 = 819; // * 20 = 16380 bytes

/// Encode an external id into the `(u128, is_uuid)` representation shared with
/// [`CompressedInternalToExternal`](crate::id_tracker::compressed::internal_to_external::CompressedInternalToExternal).
fn encode_external(id: PointIdType) -> (u128, bool) {
    match id {
        PointIdType::NumId(num) => (u128::from(num), false),
        PointIdType::Uuid(uuid) => (uuid.as_u128(), true),
    }
}

/// Decode a `(u128, is_uuid)` slot back into a [`PointIdType`].
fn decode_external(value: u128, is_uuid: bool) -> PointIdType {
    if is_uuid {
        PointIdType::Uuid(Uuid::from_u128(value))
    } else {
        debug_assert!(
            value <= u128::from(u64::MAX),
            "numeric external id does not fit into u64",
        );
        PointIdType::NumId(value as u64)
    }
}

/// Pack a bit slice into bytes, LSB-first within each byte (bit `i` lives in byte
/// `i / 8` at position `i % 8`). Matches the single-bit read in the reader.
fn pack_bits(bits: &BitSlice) -> Vec<u8> {
    let mut bytes = vec![0u8; bits.len().div_ceil(8)];
    for i in 0..bits.len() {
        if bits.get_bit(i).unwrap_or(false) {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

/// Serialize the internal→external mapping (`id_tracker.i2e`).
///
/// Layout: header, then `total` little-endian `u128` values in offset order,
/// then the packed `is_uuid` bit array (`ceil(total / 8)` bytes).
pub fn store_i2e<W: Write>(
    mappings: &CompressedPointMappings,
    mut writer: W,
) -> OperationResult<()> {
    let total = mappings.total_point_count();

    writer.write_u64::<Endian>(I2E_MAGIC)?;
    writer.write_u32::<Endian>(ON_DISK_FORMAT_VERSION)?;
    writer.write_u32::<Endian>(0)?; // reserved
    writer.write_u64::<Endian>(total as u64)?;
    writer.write_u64::<Endian>(0)?; // reserved, pads header to SECTION_ALIGN

    let mut is_uuid = BitVec::with_capacity(total);
    for (offset, external_id) in mappings.iter_internal_raw() {
        debug_assert_eq!(offset as usize, is_uuid.len(), "i2e entries out of order");
        let (value, uuid_flag) = encode_external(external_id);
        writer.write_u128::<Endian>(value)?;
        is_uuid.push(uuid_flag);
    }

    writer.write_all(&pack_bits(&is_uuid))?;
    Ok(())
}

/// Serialize the external→internal mapping (`id_tracker.e2i`).
///
/// Runs are taken from `mappings.iter_from(None)`, which yields live points in
/// numeric-then-UUID order, each run internally sorted ascending — exactly the
/// on-disk run order. Deleted-at-build points are already excluded there.
pub fn store_e2i<W: Write>(
    mappings: &CompressedPointMappings,
    mut writer: W,
) -> OperationResult<()> {
    let mut num: Vec<(u64, PointOffsetType)> = Vec::new();
    let mut uuid: Vec<(u128, PointOffsetType)> = Vec::new();
    for (external_id, offset) in mappings.iter_from(None) {
        match external_id {
            PointIdType::NumId(n) => num.push((n, offset)),
            PointIdType::Uuid(u) => uuid.push((u.as_u128(), offset)),
        }
    }
    debug_assert!(num.is_sorted_by_key(|(k, _)| *k), "numeric run not sorted");
    debug_assert!(uuid.is_sorted_by_key(|(k, _)| *k), "uuid run not sorted");

    writer.write_u64::<Endian>(E2I_MAGIC)?;
    writer.write_u32::<Endian>(ON_DISK_FORMAT_VERSION)?;
    writer.write_u32::<Endian>(0)?; // reserved
    writer.write_u64::<Endian>(num.len() as u64)?;
    writer.write_u64::<Endian>(uuid.len() as u64)?;
    writer.write_u32::<Endian>(NUM_BLOCK_ENTRIES)?;
    writer.write_u32::<Endian>(UUID_BLOCK_ENTRIES)?;
    writer.write_u64::<Endian>(0)?; // reserved, pads header to SECTION_ALIGN

    // Sections are padded so each starts at a SECTION_ALIGN boundary; the pad
    // sizes here must mirror the offsets computed in `E2iHeader::parse`.
    let write_section_pad = |writer: &mut W, end: u64| -> OperationResult<()> {
        writer.write_all(&vec![0u8; (align_section(end) - end) as usize])?;
        Ok(())
    };

    // Sparse block index: first key of every block, for each run.
    for chunk in num.chunks(NUM_BLOCK_ENTRIES as usize) {
        writer.write_u64::<Endian>(chunk[0].0)?;
    }
    let num_sparse_end =
        E2I_HEADER_SIZE + num.len().div_ceil(NUM_BLOCK_ENTRIES as usize) as u64 * 8;
    write_section_pad(&mut writer, num_sparse_end)?;
    for chunk in uuid.chunks(UUID_BLOCK_ENTRIES as usize) {
        writer.write_u128::<Endian>(chunk[0].0)?;
    }

    // Runs. The uuid sparse section ends SECTION_ALIGN-aligned (16-byte
    // entries), so the numeric run needs no leading pad.
    for (key, offset) in &num {
        writer.write_u64::<Endian>(*key)?;
        writer.write_u32::<Endian>(*offset)?;
    }
    let num_run_end = align_section(num_sparse_end)
        + uuid.len().div_ceil(UUID_BLOCK_ENTRIES as usize) as u64 * 16
        + num.len() as u64 * NUM_ENTRY_SIZE;
    write_section_pad(&mut writer, num_run_end)?;
    for (key, offset) in &uuid {
        writer.write_u128::<Endian>(*key)?;
        writer.write_u32::<Endian>(*offset)?;
    }

    Ok(())
}

fn inconsistent(msg: impl Into<String>) -> OperationError {
    OperationError::inconsistent_storage(msg.into())
}

/// Parsed i2e header plus the byte offset where the `is_uuid` bit array begins.
#[derive(Copy, Clone, Debug)]
pub struct I2eHeader {
    pub total: u64,
    /// Byte offset of the `u128` data array (right after the header).
    pub data_offset: u64,
    /// Byte offset of the packed `is_uuid` bit array.
    pub is_uuid_offset: u64,
}

impl I2eHeader {
    pub fn parse(bytes: &[u8]) -> OperationResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("i2e header: {e}")))?;
        if magic != I2E_MAGIC {
            return Err(inconsistent("i2e file has invalid magic"));
        }
        let version = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("i2e header: {e}")))?;
        if version != ON_DISK_FORMAT_VERSION {
            return Err(inconsistent(format!(
                "unsupported i2e on-disk format version {version}"
            )));
        }
        let _reserved = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("i2e header: {e}")))?;
        let total = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("i2e header: {e}")))?;
        let _reserved2 = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("i2e header: {e}")))?;

        let data_offset = I2E_HEADER_SIZE;
        let is_uuid_offset = data_offset + total * 16;
        Ok(Self {
            total,
            data_offset,
            is_uuid_offset,
        })
    }

    /// Decode one `(u128, is_uuid)` slot from the raw 16 data bytes and the byte
    /// holding this slot's `is_uuid` bit.
    pub fn decode_slot(offset: PointOffsetType, data16: &[u8], is_uuid_byte: u8) -> PointIdType {
        let value = u128::from_le_bytes(data16.try_into().expect("16 data bytes"));
        let is_uuid = (is_uuid_byte >> (offset as usize % 8)) & 1 == 1;
        decode_external(value, is_uuid)
    }
}

/// Parsed e2i header plus the computed byte offsets of every section.
#[derive(Clone, Debug)]
pub struct E2iHeader {
    pub num_count: u64,
    pub uuid_count: u64,
    pub num_block_size: u32,
    pub uuid_block_size: u32,
    pub num_sparse_offset: u64,
    pub uuid_sparse_offset: u64,
    pub num_run_offset: u64,
    pub uuid_run_offset: u64,
}

impl E2iHeader {
    pub fn num_blocks(&self) -> u64 {
        self.num_count
            .div_ceil(u64::from(self.num_block_size).max(1))
    }

    pub fn uuid_blocks(&self) -> u64 {
        self.uuid_count
            .div_ceil(u64::from(self.uuid_block_size).max(1))
    }

    /// Byte offset where the sparse index (num + uuid) ends and runs begin.
    pub fn index_end(&self) -> u64 {
        self.num_run_offset
    }

    pub fn parse(bytes: &[u8]) -> OperationResult<Self> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        if magic != E2I_MAGIC {
            return Err(inconsistent("e2i file has invalid magic"));
        }
        let version = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        if version != ON_DISK_FORMAT_VERSION {
            return Err(inconsistent(format!(
                "unsupported e2i on-disk format version {version}"
            )));
        }
        let _reserved = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        let num_count = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        let uuid_count = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        let num_block_size = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        let uuid_block_size = cursor
            .read_u32::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;
        let _reserved2 = cursor
            .read_u64::<Endian>()
            .map_err(|e| inconsistent(format!("e2i header: {e}")))?;

        if num_block_size == 0 || uuid_block_size == 0 {
            return Err(inconsistent("e2i block size is zero"));
        }

        // Every section starts SECTION_ALIGN-aligned; must mirror the pads
        // written in `store_e2i`.
        let mut header = Self {
            num_count,
            uuid_count,
            num_block_size,
            uuid_block_size,
            num_sparse_offset: E2I_HEADER_SIZE,
            uuid_sparse_offset: 0,
            num_run_offset: 0,
            uuid_run_offset: 0,
        };
        header.uuid_sparse_offset =
            align_section(header.num_sparse_offset + header.num_blocks() * 8);
        header.num_run_offset = header.uuid_sparse_offset + header.uuid_blocks() * 16;
        header.uuid_run_offset = align_section(header.num_run_offset + num_count * NUM_ENTRY_SIZE);
        Ok(header)
    }
}

/// Decode a numeric run block (`(u64 key, u32 offset)` entries) from raw bytes.
pub fn decode_num_block(bytes: &[u8]) -> Vec<(u128, PointOffsetType)> {
    bytes
        .as_chunks::<{ NUM_ENTRY_SIZE as usize }>()
        .0
        .iter()
        .map(|entry| {
            let key = u64::from_le_bytes(entry[0..8].try_into().unwrap());
            let offset = u32::from_le_bytes(entry[8..12].try_into().unwrap());
            (u128::from(key), offset)
        })
        .collect()
}

/// Decode a UUID run block (`(u128 key, u32 offset)` entries) from raw bytes.
pub fn decode_uuid_block(bytes: &[u8]) -> Vec<(u128, PointOffsetType)> {
    bytes
        .as_chunks::<{ UUID_ENTRY_SIZE as usize }>()
        .0
        .iter()
        .map(|entry| {
            let key = u128::from_le_bytes(entry[0..16].try_into().unwrap());
            let offset = u32::from_le_bytes(entry[16..20].try_into().unwrap());
            (key, offset)
        })
        .collect()
}
