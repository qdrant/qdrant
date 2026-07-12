//! Point lookups: sparse-index binary search plus one data-block read.

use ahash::AHashMap;
use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};

use super::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::on_disk_format::{
    NUM_ENTRY_SIZE, UUID_ENTRY_SIZE, decode_external, decode_num_block, decode_uuid_block,
};
use crate::types::PointIdType;

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Byte range of numeric run block `block` within `e2i`.
    fn num_block_range(&self, block: u64) -> ReadRange {
        let bs = u64::from(self.e2i_header.num_block_size);
        let start = block * bs;
        let count = (self.e2i_header.num_count - start).min(bs);
        ReadRange {
            byte_offset: self.e2i_header.num_run_offset + start * NUM_ENTRY_SIZE,
            length: count * NUM_ENTRY_SIZE,
        }
    }

    /// Byte range of UUID run block `block` within `e2i`.
    fn uuid_block_range(&self, block: u64) -> ReadRange {
        let bs = u64::from(self.e2i_header.uuid_block_size);
        let start = block * bs;
        let count = (self.e2i_header.uuid_count - start).min(bs);
        ReadRange {
            byte_offset: self.e2i_header.uuid_run_offset + start * UUID_ENTRY_SIZE,
            length: count * UUID_ENTRY_SIZE,
        }
    }

    pub(super) fn read_num_block(
        &self,
        block: u64,
    ) -> OperationResult<Vec<(u128, PointOffsetType)>> {
        let bytes = self
            .e2i
            .read::<common::generic_consts::Random, u8>(self.num_block_range(block))?;
        Ok(decode_num_block(bytes.as_ref()))
    }

    pub(super) fn read_uuid_block(
        &self,
        block: u64,
    ) -> OperationResult<Vec<(u128, PointOffsetType)>> {
        let bytes = self
            .e2i
            .read::<common::generic_consts::Random, u8>(self.uuid_block_range(block))?;
        Ok(decode_uuid_block(bytes.as_ref()))
    }

    /// Sparse-index block that can contain numeric key `key`, or `None` when
    /// the numeric run is empty.
    fn num_block_of(&self, key: u64) -> Option<u64> {
        if self.e2i_header.num_count == 0 {
            return None;
        }
        let block = self
            .num_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        Some(block)
    }

    /// Sparse-index block that can contain UUID key `key`, or `None` when the
    /// UUID run is empty.
    fn uuid_block_of(&self, key: u128) -> Option<u64> {
        if self.e2i_header.uuid_count == 0 {
            return None;
        }
        let block = self
            .uuid_sparse
            .partition_point(|&first| first <= key)
            .saturating_sub(1) as u64;
        Some(block)
    }

    fn lookup_num(&self, key: u64) -> OperationResult<Option<PointOffsetType>> {
        let Some(block) = self.num_block_of(key) else {
            return Ok(None);
        };
        let entries = self.read_num_block(block)?;
        Ok(entries
            .binary_search_by_key(&u128::from(key), |(k, _)| *k)
            .ok()
            .map(|idx| entries[idx].1))
    }

    fn lookup_uuid(&self, key: u128) -> OperationResult<Option<PointOffsetType>> {
        let Some(block) = self.uuid_block_of(key) else {
            return Ok(None);
        };
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

    /// Batch counterpart of [`lookup`](Self::lookup): keys are grouped by the
    /// e2i block that can contain them, every unique block is read once through
    /// a single pipelined [`read_batch`](UniversalRead::read_batch) pass, and
    /// each key is binary-searched within its block.
    ///
    /// Results are returned in input order; absent ids yield `None`. Deletion
    /// is NOT applied (same contract as `lookup`); storage errors propagate.
    pub fn lookup_batch(
        &self,
        external_ids: &[PointIdType],
    ) -> OperationResult<Vec<Option<PointOffsetType>>> {
        let mut results: Vec<Option<PointOffsetType>> = vec![None; external_ids.len()];

        // Group `(input index, key)` by `(run, block)`, in first-seen order so
        // the read schedule is deterministic.
        let mut group_slots: AHashMap<(bool, u64), usize> = AHashMap::new();
        let mut groups: Vec<BlockLookupGroup> = Vec::new();
        for (idx, &external_id) in external_ids.iter().enumerate() {
            let (is_uuid, key, block) = match external_id {
                PointIdType::NumId(num) => {
                    let Some(block) = self.num_block_of(num) else {
                        continue;
                    };
                    (false, u128::from(num), block)
                }
                PointIdType::Uuid(uuid) => {
                    let key = uuid.as_u128();
                    let Some(block) = self.uuid_block_of(key) else {
                        continue;
                    };
                    (true, key, block)
                }
            };
            let slot = *group_slots.entry((is_uuid, block)).or_insert_with(|| {
                groups.push(BlockLookupGroup {
                    is_uuid,
                    block,
                    keys: Vec::new(),
                });
                groups.len() - 1
            });
            groups[slot].keys.push((idx, key));
        }

        let ranges = groups.iter().enumerate().map(|(slot, group)| {
            let range = if group.is_uuid {
                self.uuid_block_range(group.block)
            } else {
                self.num_block_range(group.block)
            };
            (slot, range)
        });
        self.e2i
            .read_batch::<common::generic_consts::Random, u8, usize>(ranges, |slot, bytes| {
                let BlockLookupGroup {
                    is_uuid,
                    block: _,
                    keys,
                } = &groups[slot];
                let entries = if *is_uuid {
                    decode_uuid_block(bytes)
                } else {
                    decode_num_block(bytes)
                };
                for &(idx, key) in keys {
                    results[idx] = entries
                        .binary_search_by_key(&key, |(k, _)| *k)
                        .ok()
                        .map(|pos| entries[pos].1);
                }
                Ok(())
            })?;

        Ok(results)
    }

    /// Internal→external lookup ignoring deletion. `Ok(None)` for out-of-range
    /// offsets; storage errors propagate.
    pub fn external_id(&self, offset: PointOffsetType) -> OperationResult<Option<PointIdType>> {
        if u64::from(offset) >= self.i2e_header.total {
            return Ok(None);
        }
        self.read_external_id(offset).map(Some)
    }

    /// Batch counterpart of [`external_id`](Self::external_id): all 16-byte
    /// data slots are scheduled in one pipelined
    /// [`read_batch`](UniversalRead::read_batch) pass over `i2e`; the
    /// `is_uuid` flag comes from the RAM-resident bitmap, so no extra reads.
    ///
    /// Results are returned in input order; out-of-range offsets yield `None`.
    /// Deletion is NOT applied; storage errors propagate.
    pub fn external_ids_batch(
        &self,
        offsets: &[PointOffsetType],
    ) -> OperationResult<Vec<Option<PointIdType>>> {
        let mut results: Vec<Option<PointIdType>> = vec![None; offsets.len()];

        let data_ranges = offsets
            .iter()
            .enumerate()
            .filter(|&(_, &offset)| u64::from(offset) < self.i2e_header.total)
            .map(|(idx, &offset)| {
                let range = ReadRange {
                    byte_offset: self.i2e_header.data_offset + u64::from(offset) * 16,
                    length: 16,
                };
                (idx, range)
            });

        self.i2e
            .read_batch::<common::generic_consts::Random, u8, usize>(data_ranges, |idx, bytes| {
                let value = u128::from_le_bytes(bytes.try_into().expect("16 data bytes"));
                results[idx] = Some(decode_external(value, self.is_uuid.contains(offsets[idx])));
                Ok(())
            })?;

        Ok(results)
    }

    /// One 16-byte i2e read; the `is_uuid` flag comes from the RAM-resident
    /// bitmap.
    fn read_external_id(&self, offset: PointOffsetType) -> OperationResult<PointIdType> {
        let data_offset = self.i2e_header.data_offset + u64::from(offset) * 16;
        let data = self
            .i2e
            .read::<common::generic_consts::Random, u8>(ReadRange {
                byte_offset: data_offset,
                length: 16,
            })?;
        let value = u128::from_le_bytes(data.as_ref().try_into().expect("16 data bytes"));
        Ok(decode_external(value, self.is_uuid.contains(offset)))
    }

    /// Start index (within the numeric run) of the first key `>= key`.
    pub(super) fn num_start_index(&self, key: u64) -> OperationResult<u64> {
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
    pub(super) fn uuid_start_index(&self, key: u128) -> OperationResult<u64> {
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
}

/// Keys routed to one e2i block during [`DiskMappingReader::lookup_batch`]:
/// the block is read once and every key is binary-searched within it.
struct BlockLookupGroup {
    is_uuid: bool,
    block: u64,
    /// `(input index, key)` pairs to search within the block.
    keys: Vec<(usize, u128)>,
}
