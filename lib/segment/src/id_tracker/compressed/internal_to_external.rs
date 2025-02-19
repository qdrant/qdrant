use bitvec::prelude::BitVec;
use common::types::PointOffsetType;
use uuid::Uuid;

use crate::types::PointIdType;

/// A compressed representation of `internal_to_external: Vec<PointIdType>`.
///
/// The main idea is instead of `PointIdType` enum (which is 24 bytes) we use one
/// Vec<u128> and bitmask which defines if the id is u64 or UUID.
#[derive(Clone, PartialEq, Default, Debug)]
pub struct CompressedInternalToExternal {
    data: Vec<u128>,
    is_uuid: BitVec,
}

impl CompressedInternalToExternal {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            is_uuid: BitVec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn resize(&mut self, new_len: usize, value: PointIdType) {
        let stored_value = match value {
            PointIdType::NumId(num_id) => u128::from(num_id),
            PointIdType::Uuid(uuid) => uuid.as_u128(),
        };

        let is_uuid = matches!(value, PointIdType::Uuid(_));

        self.data.resize(new_len, stored_value);
        self.is_uuid.resize(new_len, is_uuid);
    }

    pub fn set(&mut self, internal_id: PointOffsetType, value: PointIdType) {
        let index = internal_id as usize;
        let stored_value = match value {
            PointIdType::NumId(num_id) => u128::from(num_id),
            PointIdType::Uuid(uuid) => uuid.as_u128(),
        };

        let is_uuid = matches!(value, PointIdType::Uuid(_));

        self.data[index] = stored_value;
        self.is_uuid.set(index, is_uuid);
    }

    pub fn from_slice(slice: &[PointIdType]) -> Self {
        let mut data = Vec::with_capacity(slice.len());
        let mut is_uuid = BitVec::with_capacity(slice.len());

        for id in slice {
            match id {
                PointIdType::NumId(num_id) => {
                    data.push(u128::from(*num_id));
                    is_uuid.push(false);
                }
                PointIdType::Uuid(uuid) => {
                    data.push(uuid.as_u128());
                    is_uuid.push(true);
                }
            }
        }

        Self { data, is_uuid }
    }

    pub fn get(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        let index = internal_id as usize;
        let data = self.data.get(index)?;
        let is_uuid = *self.is_uuid.get(index)?;

        if is_uuid {
            Some(PointIdType::Uuid(Uuid::from_u128(*data)))
        } else {
            Some(PointIdType::NumId(*data as u64))
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = PointIdType> + '_ {
        self.data
            .iter()
            .zip(self.is_uuid.iter())
            .map(|(data, is_uuid)| {
                if *is_uuid {
                    PointIdType::Uuid(Uuid::from_u128(*data))
                } else {
                    PointIdType::NumId(*data as u64)
                }
            })
    }
}
