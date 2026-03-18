use std::collections::BTreeMap;

use bitvec::prelude::BitVec;
use common::types::PointOffsetType;
use itertools::Either;
use uuid::Uuid;

use crate::types::PointIdType;

/// A compressed representation of
///
/// - `external_to_internal_num: BTreeMap<u64, PointOffsetType>`
/// - `external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>`
///
/// The main idea is to use sorted vector instead of BTreeMap.
/// This structure doesn't require random insertions, so we can sort it once and then use binary search.
///
/// There is, however, a requirement to remove elements, so we will use a BitVec to mark removed elements.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct CompressedExternalToInternal {
    num_ids: Vec<(u64, PointOffsetType)>,
    num_ids_removed: BitVec,
    uuids: Vec<(Uuid, PointOffsetType)>,
    uuids_removed: BitVec,
    count_removed: usize,
}

impl CompressedExternalToInternal {
    pub fn from_vectors(
        external_to_internal_num: Vec<(u64, PointOffsetType)>,
        external_to_internal_uuid: Vec<(Uuid, PointOffsetType)>,
    ) -> Self {
        let mut num_ids = external_to_internal_num;
        let mut uuids = external_to_internal_uuid;

        num_ids.shrink_to_fit();
        uuids.shrink_to_fit();

        num_ids.sort_unstable();
        uuids.sort_unstable();

        let num_ids_removed = BitVec::repeat(false, num_ids.len());
        let uuids_removed = BitVec::repeat(false, uuids.len());

        let num_removed = 0;

        Self {
            num_ids,
            num_ids_removed,
            uuids,
            uuids_removed,
            count_removed: num_removed,
        }
    }
    pub fn from_maps(
        external_to_internal_num: BTreeMap<u64, PointOffsetType>,
        external_to_internal_uuid: BTreeMap<Uuid, PointOffsetType>,
    ) -> Self {
        let mut num_ids: Vec<_> = external_to_internal_num.into_iter().collect();

        let mut uuids: Vec<_> = external_to_internal_uuid.into_iter().collect();

        num_ids.sort_unstable();
        uuids.sort_unstable();

        let num_ids_removed = BitVec::repeat(false, num_ids.len());
        let uuids_removed = BitVec::repeat(false, uuids.len());

        let num_removed = 0;

        Self {
            num_ids,
            num_ids_removed,
            uuids,
            uuids_removed,
            count_removed: num_removed,
        }
    }

    pub fn len(&self) -> usize {
        self.num_ids.len() + self.uuids.len() - self.count_removed
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, external_id: &PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => {
                let idx = self
                    .num_ids
                    .binary_search_by_key(num, |(num, _)| *num)
                    .ok()?;
                if self.num_ids_removed[idx] {
                    None
                } else {
                    Some(self.num_ids[idx].1)
                }
            }
            PointIdType::Uuid(uuid) => {
                let idx = self
                    .uuids
                    .binary_search_by_key(uuid, |(uuid, _)| *uuid)
                    .ok()?;
                if self.uuids_removed[idx] {
                    None
                } else {
                    Some(self.uuids[idx].1)
                }
            }
        }
    }

    pub fn remove(&mut self, external_id: &PointIdType) -> Option<PointOffsetType> {
        match external_id {
            PointIdType::NumId(num) => {
                let idx = self
                    .num_ids
                    .binary_search_by_key(num, |(num, _)| *num)
                    .ok()?;
                if self.num_ids_removed[idx] {
                    None
                } else {
                    self.num_ids_removed.set(idx, true);
                    self.count_removed += 1;
                    Some(self.num_ids[idx].1)
                }
            }
            PointIdType::Uuid(uuid) => {
                let idx = self
                    .uuids
                    .binary_search_by_key(uuid, |(uuid, _)| *uuid)
                    .ok()?;
                if self.uuids_removed[idx] {
                    None
                } else {
                    self.uuids_removed.set(idx, true);
                    self.count_removed += 1;
                    Some(self.uuids[idx].1)
                }
            }
        }
    }

    fn num_iter(&self) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        self.num_ids
            .iter()
            .enumerate()
            .filter_map(move |(idx, (num, internal_id))| {
                if self.num_ids_removed[idx] {
                    None
                } else {
                    Some((PointIdType::NumId(*num), *internal_id))
                }
            })
    }

    fn uuid_iter(&self) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        self.uuids
            .iter()
            .enumerate()
            .filter_map(move |(idx, (uuid, internal_id))| {
                if self.uuids_removed[idx] {
                    None
                } else {
                    Some((PointIdType::Uuid(*uuid), *internal_id))
                }
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        let num_iter = self.num_iter();
        let uuid_iter = self.uuid_iter();
        num_iter.chain(uuid_iter)
    }

    pub fn iter_from(
        &self,
        point_id: PointIdType,
    ) -> impl Iterator<Item = (PointIdType, PointOffsetType)> + '_ {
        match point_id {
            PointIdType::NumId(num_id) => {
                // Iterator over range of num ids and then over all UUID, as we assume that
                // any UUID is bigger than any num id.

                let num_id_iterator_from = self
                    .num_ids
                    .binary_search_by_key(&num_id, |(num, _)| *num)
                    .unwrap_or_else(|x| x);

                let num_id_iter = (num_id_iterator_from..self.num_ids.len()).filter_map(|idx| {
                    let (point_id, point_offset) = self.num_ids[idx];
                    let is_removed = self.num_ids_removed[idx];
                    if is_removed {
                        None
                    } else {
                        Some((PointIdType::NumId(point_id), point_offset))
                    }
                });

                let uuid_iter = self.uuid_iter();
                Either::Right(num_id_iter.chain(uuid_iter))
            }
            PointIdType::Uuid(uuid) => {
                // Just iterate over range of uuids

                let uuid_iterator_from = self
                    .uuids
                    .binary_search_by_key(&uuid, |(uuid, _)| *uuid)
                    .unwrap_or_else(|x| x);

                let uuid_iter = (uuid_iterator_from..self.uuids.len()).filter_map(|idx| {
                    let (point_id, point_offset) = self.uuids[idx];
                    let is_removed = self.uuids_removed[idx];
                    if is_removed {
                        None
                    } else {
                        Some((PointIdType::Uuid(point_id), point_offset))
                    }
                });

                Either::Left(uuid_iter)
            }
        }
    }
}
