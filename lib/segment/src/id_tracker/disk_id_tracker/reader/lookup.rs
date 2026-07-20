//! Point lookups: sparse-index binary search plus one data-block read.

use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};
use uuid::Uuid;

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
        let bytes = self.e2i.read::<Random, u8>(self.num_block_range(block))?;
        Ok(decode_num_block(bytes.as_ref()))
    }

    pub(super) fn read_uuid_block(
        &self,
        block: u64,
    ) -> OperationResult<Vec<(u128, PointOffsetType)>> {
        let bytes = self.e2i.read::<Random, u8>(self.uuid_block_range(block))?;
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

    /// Batch counterpart of [`lookup`](Self::lookup): one pipelined
    /// [`read_batch`](UniversalRead::read_batch) read per id for the e2i block
    /// that can contain it, then a binary search within that block.
    ///
    /// Each resolved `(id, offset)` is handed to `on_found` as its block
    /// completes; absent ids are dropped. Delivery is in read-completion order,
    /// not input order — the id is rebuilt from `is_uuid` + key, so callers
    /// never need the input position. Deletion is NOT applied (same contract as
    /// `lookup`); storage errors and `on_found` errors propagate, aborting the
    /// pass.
    ///
    /// Ids are not grouped by block up front: a block is one ~16 KiB DiskCache
    /// block, so reads landing in the same block are deduplicated by the cache
    /// (piggybacked while in flight, a plain hit once resident). Grouping here
    /// would only re-implement that at the cost of an up-front index.
    pub fn lookup_batch(
        &self,
        external_ids: impl IntoIterator<Item = PointIdType>,
        mut on_found: impl FnMut(PointIdType, PointOffsetType) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // Each read is tagged with `(is_uuid, key)` so the callback can pick the
        // decoder, binary-search, and rebuild the id. Ids outside every block
        // are dropped here; the range iterator stays lazy (no collect).
        let ranges = external_ids
            .into_iter()
            .filter_map(|external_id| match external_id {
                PointIdType::NumId(num) => {
                    let block = self.num_block_of(num)?;
                    Some(((false, u128::from(num)), self.num_block_range(block)))
                }
                PointIdType::Uuid(uuid) => {
                    let key = uuid.as_u128();
                    let block = self.uuid_block_of(key)?;
                    Some(((true, key), self.uuid_block_range(block)))
                }
            });

        self.e2i
            .read_batch(ranges, Random, |(is_uuid, key), bytes| {
                let entries = if is_uuid {
                    decode_uuid_block(bytes)
                } else {
                    decode_num_block(bytes)
                };
                if let Ok(pos) = entries.binary_search_by_key(&key, |(k, _)| *k) {
                    let id = if is_uuid {
                        PointIdType::Uuid(Uuid::from_u128(key))
                    } else {
                        PointIdType::NumId(key as u64)
                    };
                    on_found(id, entries[pos].1)?;
                }
                Ok(())
            })
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
    /// Each resolved `(offset, id)` is handed to `on_found` as its read
    /// completes (read-completion order, not input order); out-of-range
    /// offsets are skipped. The input is walked once and nothing is buffered.
    /// Deletion is NOT applied; storage errors propagate.
    pub fn external_ids_batch(
        &self,
        offsets: impl IntoIterator<Item = PointOffsetType>,
        mut on_found: impl FnMut(PointOffsetType, PointIdType),
    ) -> OperationResult<()> {
        // Each read is tagged with its offset so the callback can pair it with
        // the decoded id; the range iterator stays lazy (no collect).
        let ranges = offsets
            .into_iter()
            .filter(|&offset| u64::from(offset) < self.i2e_header.total)
            .map(|offset| {
                let range = ReadRange {
                    byte_offset: self.i2e_header.data_offset + u64::from(offset) * 16,
                    length: 16,
                };
                (offset, range)
            });

        self.i2e.read_batch(ranges, Random, |offset, bytes| {
            let value = u128::from_le_bytes(bytes.try_into().expect("16 data bytes"));
            on_found(
                offset,
                decode_external(value, self.is_uuid.contains(offset)),
            );
            Ok(())
        })
    }

    /// One 16-byte i2e read; the `is_uuid` flag comes from the RAM-resident
    /// bitmap.
    fn read_external_id(&self, offset: PointOffsetType) -> OperationResult<PointIdType> {
        let data_offset = self.i2e_header.data_offset + u64::from(offset) * 16;
        let data = self.i2e.read::<Random, u8>(ReadRange {
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
