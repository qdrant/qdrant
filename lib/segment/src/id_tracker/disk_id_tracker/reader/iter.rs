//! Ordered (e2i-run streaming) and random-order iteration over the mapping.

use common::bitvec::{BitSlice, BitSliceExt as _};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use itertools::Itertools as _;
use rand::distr::{Distribution as _, Uniform};
use uuid::Uuid;

use super::DiskMappingReader;
use crate::common::operation_error::OperationResult;
use crate::types::PointIdType;

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Ordered iterator over the e2i runs starting at `external_id`, yielding
    /// build-time-live points. Deletion is applied by the caller.
    pub fn iter_from(&self, external_id: Option<PointIdType>) -> E2iIter<'_, S> {
        E2iIter::new(self, external_id)
    }
}

/// Random-order iterator over live `(external_id, offset)` pairs: each offset
/// is visited at most once, and a full drain covers every live point.
///
/// Aimed at small `take(limit)` sampling — a full drain pays coupon-collector
/// cost plus one `i2e` read per yielded point. Best-effort: a storage error is
/// logged and the offset skipped.
pub fn iter_random<'a, S: UniversalRead>(
    reader: &'a DiskMappingReader<S>,
    deleted: &'a BitSlice,
) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + 'a {
    let total = reader.total_point_count() as usize;

    // `Uniform` rejects an empty range; the `take(total)` below keeps the
    // iterator empty when there are no points, so the dummy 0..1 range is
    // never sampled.
    let uniform = Uniform::new(0, total.max(1)).expect("sampling range is non-empty");
    let sampled = uniform.sample_iter(rand::rng()).unique().take(total);
    sampled.filter_map(move |offset| {
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
    })
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
