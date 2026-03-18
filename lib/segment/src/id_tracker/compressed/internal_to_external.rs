use bitvec::prelude::BitVec;
use common::types::PointOffsetType;
use uuid::Uuid;

use crate::types::PointIdType;

/// A compressed representation of `internal_to_external: Vec<PointIdType>`.
///
/// The main idea is instead of `PointIdType` enum (which is 24 bytes) we use one
/// Vec<u128> and bitmask which defines if the id is u64 or UUID (which is ~16 bytes).
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
            debug_assert!(
                *data <= u128::from(u64::MAX),
                "type mismatch, external ID does not fit u64",
            );
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
                    debug_assert!(
                        *data <= u128::from(u64::MAX),
                        "type mismatch, external ID does not fit u64",
                    );
                    PointIdType::NumId(*data as u64)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn create_uuid() -> Uuid {
        Uuid::new_v4()
    }

    #[test]
    fn with_capacity_creates_empty_struct() {
        let compressed = CompressedInternalToExternal::with_capacity(10);
        assert_eq!(compressed.len(), 0);
        assert!(compressed.is_empty());
    }

    #[test]
    fn resize_changes_length_and_fills_with_value() {
        let mut compressed = CompressedInternalToExternal::with_capacity(0);
        let uuid = create_uuid();
        compressed.resize(5, PointIdType::Uuid(uuid));
        assert_eq!(compressed.len(), 5);
        for i in 0..5 {
            assert_eq!(
                compressed.get(i as PointOffsetType),
                Some(PointIdType::Uuid(uuid))
            );
        }
    }

    #[test]
    fn set_updates_value_at_index() {
        let mut compressed = CompressedInternalToExternal::with_capacity(1);
        let uuid = create_uuid();
        compressed.resize(1, PointIdType::NumId(42));
        compressed.set(0, PointIdType::Uuid(uuid));
        assert_eq!(compressed.get(0), Some(PointIdType::Uuid(uuid)));
    }

    #[test]
    fn from_slice_creates_struct_from_slice() {
        let uuid = create_uuid();
        let slice = vec![PointIdType::NumId(42), PointIdType::Uuid(uuid)];
        let compressed = CompressedInternalToExternal::from_slice(&slice);
        assert_eq!(compressed.len(), 2);
        assert_eq!(compressed.get(0), Some(PointIdType::NumId(42)));
        assert_eq!(compressed.get(1), Some(PointIdType::Uuid(uuid)));
    }

    #[test]
    fn get_returns_none_for_out_of_bounds() {
        let compressed = CompressedInternalToExternal::with_capacity(0);
        assert_eq!(compressed.get(0), None);
    }

    #[test]
    fn iter_returns_all_elements() {
        let uuid = create_uuid();
        let slice = vec![PointIdType::NumId(42), PointIdType::Uuid(uuid)];
        let compressed = CompressedInternalToExternal::from_slice(&slice);
        let collected: Vec<PointIdType> = compressed.iter().collect();
        assert_eq!(collected, slice);
    }
}
