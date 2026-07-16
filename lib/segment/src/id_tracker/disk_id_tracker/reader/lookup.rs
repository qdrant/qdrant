//! Point lookups: sparse-index binary search plus one data-block read.

use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};

use super::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::disk_id_tracker::on_disk_format::{
    NUM_ENTRY_SIZE, UUID_ENTRY_SIZE, decode_external, decode_num_block, decode_uuid_block,
};
use crate::types::PointIdType;

impl<S: UniversalRead> DiskMappingReader<S> {
    pub(super) fn read_num_block(
        &self,
        block: u64,
    ) -> OperationResult<Vec<(u128, PointOffsetType)>> {
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

    pub(super) fn read_uuid_block(
        &self,
        block: u64,
    ) -> OperationResult<Vec<(u128, PointOffsetType)>> {
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
