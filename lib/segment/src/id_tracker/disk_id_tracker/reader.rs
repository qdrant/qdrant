//! Shared lazy read core over the [on-disk format](super::on_disk_format)
//! mapping files (`i2e` + `e2i`).
//!
//! Holds only the two headers and the e2i sparse block index in RAM; every
//! lookup reads at most one data block through the backing [`UniversalRead`]
//! handle. Both the writable [`DiskIdTracker`](super::DiskIdTracker) and the
//! read-only [`ReadOnlyDiskIdTracker`](super::read_only::ReadOnlyDiskIdTracker)
//! embed this and layer their own deletion/version handling on top.
//!
//! Deletion is deliberately NOT applied here: `lookup`/`external_id`/`iter_from`
//! return build-time-live entries, and each tracker filters with its own deleted
//! source (a resident bitvec for the writable tracker, a lazy `get_bit` for the
//! reader).

use std::io::Cursor;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt};
use common::bitvec::{BitSlice, BitSliceExt as _};
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{
    OkNotFound, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFs,
};
use itertools::Itertools as _;
use rand::distr::{Distribution as _, Uniform};
use uuid::Uuid;

use super::on_disk_format::{
    E2I_HEADER_SIZE, E2iHeader, I2E_HEADER_SIZE, I2eHeader, NUM_ENTRY_SIZE, UUID_ENTRY_SIZE,
    decode_num_block, decode_uuid_block, e2i_path, i2e_path,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::PointIdType;

type Endian = LittleEndian;

/// Lazy read core over the `i2e`/`e2i` files.
#[derive(Debug)]
pub struct DiskMappingReader<S: UniversalRead> {
    i2e: S,
    e2i: S,
    i2e_header: I2eHeader,
    e2i_header: E2iHeader,
    /// First numeric key of every numeric block.
    num_sparse: Vec<u64>,
    /// First UUID key (`as_u128`) of every UUID block.
    uuid_sparse: Vec<u128>,
}

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Open the `i2e`/`e2i` handles and read the headers + sparse index. No
    /// per-point data is read.
    ///
    /// Errors if the segment is not in the on-disk format (`i2e` absent). Use
    /// [`try_open`](Self::try_open) to probe without erroring.
    pub fn open(fs: &S::Fs, segment_path: &Path) -> OperationResult<Self> {
        Self::try_open(fs, segment_path)?.ok_or_else(|| {
            OperationError::service_error(format!(
                "on-disk id tracker mapping ({}) not found",
                i2e_path(segment_path).display(),
            ))
        })
    }

    /// Like [`open`](Self::open), but returns `Ok(None)` when the defining `i2e`
    /// file is absent — i.e. the segment is not in the on-disk format. Any other
    /// missing/corrupt file is a hard error. Opening the `i2e` handle also serves
    /// as the format probe, so no separate existence check is issued.
    pub fn try_open(fs: &S::Fs, segment_path: &Path) -> OperationResult<Option<Self>> {
        let options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Global,
        };

        let Some(i2e) = fs
            .open(i2e_path(segment_path), options, Default::default())
            .ok_not_found()?
        else {
            return Ok(None);
        };
        let i2e_header_bytes = i2e.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: 0,
            length: I2E_HEADER_SIZE,
        })?;
        let i2e_header = I2eHeader::parse(i2e_header_bytes.as_ref())?;

        let e2i = fs.open(e2i_path(segment_path), options, Default::default())?;
        let e2i_header_bytes = e2i.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: 0,
            length: E2I_HEADER_SIZE,
        })?;
        let e2i_header = E2iHeader::parse(e2i_header_bytes.as_ref())?;

        // Read the whole sparse index (numeric then UUID) in a single range.
        let index_bytes = e2i.read::<common::generic_consts::Random, u8>(ReadRange {
            byte_offset: E2I_HEADER_SIZE,
            length: e2i_header.index_end() - E2I_HEADER_SIZE,
        })?;
        let mut cursor = Cursor::new(index_bytes.as_ref());
        let mut num_sparse = Vec::with_capacity(e2i_header.num_blocks() as usize);
        for _ in 0..e2i_header.num_blocks() {
            num_sparse.push(cursor.read_u64::<Endian>()?);
        }
        // Skip the alignment pad between the numeric and UUID sparse sections.
        cursor.set_position(e2i_header.uuid_sparse_offset - E2I_HEADER_SIZE);
        let mut uuid_sparse = Vec::with_capacity(e2i_header.uuid_blocks() as usize);
        for _ in 0..e2i_header.uuid_blocks() {
            uuid_sparse.push(cursor.read_u128::<Endian>()?);
        }

        Ok(Some(Self {
            i2e,
            e2i,
            i2e_header,
            e2i_header,
            num_sparse,
            uuid_sparse,
        }))
    }

    /// Total number of internal ids (including build-time-deleted slots).
    pub fn total_point_count(&self) -> u64 {
        self.i2e_header.total
    }

    fn read_num_block(&self, block: u64) -> OperationResult<Vec<(u128, PointOffsetType)>> {
        let bs = u64::from(self.e2i_header.num_block_size);
        let start = block * bs;
        let count = (self.e2i_header.num_count - start).min(bs);
        let bytes = self
            .e2i
            .read::<common::generic_consts::Random, u8>(ReadRange {
                byte_offset: self.e2i_header.num_run_offset + start * NUM_ENTRY_SIZE,
                length: count * NUM_ENTRY_SIZE,
            })?;
        Ok(decode_num_block(bytes.as_ref()))
    }

    fn read_uuid_block(&self, block: u64) -> OperationResult<Vec<(u128, PointOffsetType)>> {
        let bs = u64::from(self.e2i_header.uuid_block_size);
        let start = block * bs;
        let count = (self.e2i_header.uuid_count - start).min(bs);
        let bytes = self
            .e2i
            .read::<common::generic_consts::Random, u8>(ReadRange {
                byte_offset: self.e2i_header.uuid_run_offset + start * UUID_ENTRY_SIZE,
                length: count * UUID_ENTRY_SIZE,
            })?;
        Ok(decode_uuid_block(bytes.as_ref()))
    }

    fn lookup_num(&self, key: u64) -> OperationResult<Option<PointOffsetType>> {
        if self.e2i_header.num_count == 0 {
            return Ok(None);
        }
        let block = self
            .num_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        let entries = self.read_num_block(block)?;
        Ok(entries
            .binary_search_by_key(&u128::from(key), |(k, _)| *k)
            .ok()
            .map(|idx| entries[idx].1))
    }

    fn lookup_uuid(&self, key: u128) -> OperationResult<Option<PointOffsetType>> {
        if self.e2i_header.uuid_count == 0 {
            return Ok(None);
        }
        let block = self
            .uuid_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        let entries = self.read_uuid_block(block)?;
        Ok(entries
            .binary_search_by_key(&key, |(k, _)| *k)
            .ok()
            .map(|idx| entries[idx].1))
    }

    /// External→internal lookup ignoring deletion (the caller applies its own
    /// deleted source). `Ok(None)` if the id is absent; storage errors propagate.
    pub fn lookup(&self, external_id: PointIdType) -> OperationResult<Option<PointOffsetType>> {
        match external_id {
            PointIdType::NumId(num) => self.lookup_num(num),
            PointIdType::Uuid(uuid) => self.lookup_uuid(uuid.as_u128()),
        }
    }

    /// Internal→external lookup ignoring deletion. `Ok(None)` for out-of-range
    /// offsets; storage errors propagate.
    pub fn external_id(&self, offset: PointOffsetType) -> OperationResult<Option<PointIdType>> {
        if u64::from(offset) >= self.i2e_header.total {
            return Ok(None);
        }
        self.read_external_id(offset).map(Some)
    }

    fn read_external_id(&self, offset: PointOffsetType) -> OperationResult<PointIdType> {
        let data_offset = self.i2e_header.data_offset + u64::from(offset) * 16;
        let data = self
            .i2e
            .read::<common::generic_consts::Random, u8>(ReadRange {
                byte_offset: data_offset,
                length: 16,
            })?;
        let is_uuid_byte = self
            .i2e
            .read::<common::generic_consts::Random, u8>(ReadRange {
                byte_offset: self.i2e_header.is_uuid_offset + u64::from(offset) / 8,
                length: 1,
            })?;
        Ok(I2eHeader::decode_slot(
            offset,
            data.as_ref(),
            is_uuid_byte.as_ref()[0],
        ))
    }

    /// Start index (within the numeric run) of the first key `>= key`.
    fn num_start_index(&self, key: u64) -> OperationResult<u64> {
        if self.e2i_header.num_count == 0 {
            return Ok(0);
        }
        let block = self
            .num_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        let entries = self.read_num_block(block)?;
        let within = entries.partition_point(|(k, _)| *k < u128::from(key)) as u64;
        Ok(block * u64::from(self.e2i_header.num_block_size) + within)
    }

    /// Start index (within the UUID run) of the first key `>= key`.
    fn uuid_start_index(&self, key: u128) -> OperationResult<u64> {
        if self.e2i_header.uuid_count == 0 {
            return Ok(0);
        }
        let block = self
            .uuid_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        let entries = self.read_uuid_block(block)?;
        let within = entries.partition_point(|(k, _)| *k < key) as u64;
        Ok(block * u64::from(self.e2i_header.uuid_block_size) + within)
    }

    /// Ordered iterator over the e2i runs starting at `external_id`, yielding
    /// build-time-live points. Deletion is applied by the caller.
    pub fn iter_from(&self, external_id: Option<PointIdType>) -> E2iIter<'_, S> {
        E2iIter::new(self, external_id)
    }
}

/// Random-order iterator over live `(external_id, offset)` pairs, mirroring
/// [`CompressedPointMappings::iter_random`]: random internal offsets are drawn
/// (rejection-sampled and deduplicated so each is visited at most once), the
/// `deleted` set is applied, and the external id for each surviving offset is
/// read lazily from `i2e`.
///
/// Like the in-RAM reference, this is aimed at small `take(limit)` sampling; a
/// full drain pays coupon-collector cost (and one `i2e` read per yielded point).
/// `deleted` is passed in because the writable and read-only trackers hold their
/// deleted set differently.
///
/// [`CompressedPointMappings::iter_random`]:
///   crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings::iter_random
pub fn iter_random<'a, S: UniversalRead>(
    reader: &'a DiskMappingReader<S>,
    deleted: &'a BitSlice,
) -> Box<dyn Iterator<Item = (PointIdType, PointOffsetType)> + 'a> {
    let total = reader.total_point_count() as usize;
    if total == 0 {
        return Box::new(std::iter::empty());
    }

    let uniform = Uniform::new(0, total).expect("total point count is non-zero");
    let sampled = uniform.sample_iter(rand::rng()).unique().take(total);
    Box::new(sampled.filter_map(move |offset| {
        if deleted.get_bit(offset).unwrap_or(false) {
            return None;
        }
        let offset = offset as PointOffsetType;
        // Sampling iteration stays best-effort: log and skip on a storage error.
        match reader.external_id(offset) {
            Ok(external_id) => external_id.map(|external_id| (external_id, offset)),
            Err(err) => {
                log::error!("disk id tracker random iteration lookup failed: {err}");
                None
            }
        }
    }))
}

/// Which run the [`E2iIter`] is currently walking.
enum Phase {
    Num,
    Uuid,
    Done,
}

/// Lazy, block-streaming iterator over the e2i runs (numeric then UUID).
pub struct E2iIter<'a, S: UniversalRead> {
    reader: &'a DiskMappingReader<S>,
    phase: Phase,
    /// Index within the current run.
    index: u64,
    buffer: Vec<(u128, PointOffsetType)>,
    buffer_block: Option<u64>,
}

impl<'a, S: UniversalRead> E2iIter<'a, S> {
    fn new(reader: &'a DiskMappingReader<S>, external_id: Option<PointIdType>) -> Self {
        let (phase, index) = match external_id {
            None => (Phase::Num, 0),
            Some(PointIdType::NumId(key)) => match reader.num_start_index(key) {
                Ok(index) => (Phase::Num, index),
                Err(err) => {
                    log::error!("disk id tracker iter_from(num) failed: {err}");
                    (Phase::Done, 0)
                }
            },
            Some(PointIdType::Uuid(uuid)) => match reader.uuid_start_index(uuid.as_u128()) {
                Ok(index) => (Phase::Uuid, index),
                Err(err) => {
                    log::error!("disk id tracker iter_from(uuid) failed: {err}");
                    (Phase::Done, 0)
                }
            },
        };
        Self {
            reader,
            phase,
            index,
            buffer: Vec::new(),
            buffer_block: None,
        }
    }

    /// Ensure `self.buffer` holds `block`, reading it if needed.
    fn ensure_block(
        &mut self,
        block: u64,
        read: impl Fn(&DiskMappingReader<S>, u64) -> OperationResult<Vec<(u128, PointOffsetType)>>,
    ) -> bool {
        if self.buffer_block == Some(block) {
            return true;
        }
        match read(self.reader, block) {
            Ok(entries) => {
                self.buffer = entries;
                self.buffer_block = Some(block);
                true
            }
            Err(err) => {
                log::error!("disk id tracker block read failed: {err}");
                false
            }
        }
    }
}

impl<S: UniversalRead> Iterator for E2iIter<'_, S> {
    type Item = (PointIdType, PointOffsetType);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.phase {
                Phase::Num => {
                    if self.index >= self.reader.e2i_header.num_count {
                        self.phase = Phase::Uuid;
                        self.index = 0;
                        self.buffer_block = None;
                        continue;
                    }
                    let bs = u64::from(self.reader.e2i_header.num_block_size);
                    let block = self.index / bs;
                    if !self.ensure_block(block, DiskMappingReader::read_num_block) {
                        self.phase = Phase::Done;
                        return None;
                    }
                    let (key, offset) = self.buffer[(self.index - block * bs) as usize];
                    self.index += 1;
                    return Some((PointIdType::NumId(key as u64), offset));
                }
                Phase::Uuid => {
                    if self.index >= self.reader.e2i_header.uuid_count {
                        self.phase = Phase::Done;
                        return None;
                    }
                    let bs = u64::from(self.reader.e2i_header.uuid_block_size);
                    let block = self.index / bs;
                    if !self.ensure_block(block, DiskMappingReader::read_uuid_block) {
                        self.phase = Phase::Done;
                        return None;
                    }
                    let (key, offset) = self.buffer[(self.index - block * bs) as usize];
                    self.index += 1;
                    return Some((PointIdType::Uuid(Uuid::from_u128(key)), offset));
                }
                Phase::Done => return None,
            }
        }
    }
}
