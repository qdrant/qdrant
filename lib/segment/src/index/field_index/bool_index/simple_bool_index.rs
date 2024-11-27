use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use self::memory::{BoolMemory, BooleanItem};
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::map_index::IdIter;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchValue, PayloadKeyType, ValueVariants};

mod memory {
    use bitvec::vec::BitVec;
    use common::types::PointOffsetType;

    pub struct BooleanItem {
        value: u8,
    }

    impl BooleanItem {
        const HAS_TRUE: u8 = 0b0000_0001;
        const HAS_FALSE: u8 = 0b0000_0010;

        pub fn empty() -> Self {
            Self { value: 0 }
        }

        pub fn has_true(&self) -> bool {
            self.value & Self::HAS_TRUE != 0
        }

        pub fn has_false(&self) -> bool {
            self.value & Self::HAS_FALSE != 0
        }

        pub fn set(&mut self, flag: u8, value: bool) {
            if value {
                self.value |= flag;
            } else {
                self.value &= !flag;
            }
        }

        pub fn from_bools(has_true: bool, has_false: bool) -> Self {
            let mut item = Self::empty();
            item.set(Self::HAS_TRUE, has_true);
            item.set(Self::HAS_FALSE, has_false);
            item
        }

        pub fn as_bytes(&self) -> [u8; 1] {
            [self.value]
        }
    }

    impl From<u8> for BooleanItem {
        fn from(value: u8) -> Self {
            Self { value }
        }
    }

    pub struct BoolMemory {
        trues: BitVec,
        falses: BitVec,
        trues_count: usize,
        falses_count: usize,
        indexed_count: usize,
    }

    impl BoolMemory {
        pub fn new() -> Self {
            Self {
                trues: BitVec::new(),
                falses: BitVec::new(),
                trues_count: 0,
                falses_count: 0,
                indexed_count: 0,
            }
        }

        pub fn get(&self, id: PointOffsetType) -> BooleanItem {
            debug_assert!(self.trues.len() == self.falses.len());

            let has_true = self.trues.get(id as usize).map(|v| *v).unwrap_or(false);
            let has_false = self.falses.get(id as usize).map(|v| *v).unwrap_or(false);

            BooleanItem::from_bools(has_true, has_false)
        }

        pub fn set_or_insert(&mut self, id: PointOffsetType, item: &BooleanItem) {
            if (id as usize) >= self.trues.len() {
                self.trues.resize(id as usize + 1, false);
                self.falses.resize(id as usize + 1, false);
            }

            debug_assert!(self.trues.len() == self.falses.len());

            let has_true = item.has_true();
            let had_true = self.trues.replace(id as usize, has_true);
            match (had_true, has_true) {
                (false, true) => self.trues_count += 1,
                (true, false) => self.trues_count -= 1,
                _ => {}
            }

            let has_false = item.has_false();
            let had_false = self.falses.replace(id as usize, has_false);
            match (had_false, has_false) {
                (false, true) => self.falses_count += 1,
                (true, false) => self.falses_count -= 1,
                _ => {}
            }

            let was_indexed = had_true || had_false;
            let is_indexed = has_true || has_false;

            match (was_indexed, is_indexed) {
                (false, true) => {
                    self.indexed_count += 1;
                }
                (true, false) => {
                    self.indexed_count = self.indexed_count.saturating_sub(1);
                }
                _ => {}
            }
        }

        /// Removes the point from the index and tries to shrink the vectors if possible. If the index is not within bounds, does nothing
        pub fn remove(&mut self, id: PointOffsetType) {
            if (id as usize) >= self.trues.len() {
                return;
            }

            let had_true = self.trues.replace(id as usize, false);
            let had_false = self.falses.replace(id as usize, false);

            if had_true {
                self.trues_count -= 1;
            }
            if had_false {
                self.falses_count -= 1;
            }

            if had_false || had_true {
                self.indexed_count -= 1;
            }
        }

        pub fn trues_count(&self) -> usize {
            self.trues_count
        }

        pub fn falses_count(&self) -> usize {
            self.falses_count
        }

        pub fn indexed_count(&self) -> usize {
            self.indexed_count
        }

        pub fn iter_has_true(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
            self.trues.iter_ones().map(|v| v as PointOffsetType)
        }

        pub fn iter_has_false(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
            self.falses.iter_ones().map(|v| v as PointOffsetType)
        }
    }
}

/// Payload index for boolean values, persisted in a RocksDB column family
pub struct BoolIndex {
    memory: BoolMemory,
    db_wrapper: DatabaseColumnScheduledDeleteWrapper,
}

impl BoolIndex {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> BoolIndex {
        let store_cf_name = Self::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            &store_cf_name,
        ));
        Self {
            memory: BoolMemory::new(),
            db_wrapper,
        }
    }

    pub fn builder(db: Arc<RwLock<DB>>, field_name: &str) -> BoolIndexBuilder {
        BoolIndexBuilder(Self::new(db, field_name))
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_binary")
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.memory.indexed_count(),
            points_values_count: self.memory.trues_count() + self.memory.falses_count(),
            histogram_bucket_size: None,
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        let binary_item = self.memory.get(point_id);
        usize::from(binary_item.has_true()) + usize::from(binary_item.has_false())
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.values_count(point_id) == 0
    }

    /// Check if the point has a true value
    pub fn values_has_true(&self, point_id: PointOffsetType) -> bool {
        self.memory.get(point_id).has_true()
    }

    /// Check if the point has a false value
    pub fn values_has_false(&self, point_id: PointOffsetType) -> bool {
        self.memory.get(point_id).has_false()
    }

    pub fn iter_values_map(&self) -> impl Iterator<Item = (bool, IdIter<'_>)> + '_ {
        vec![
            (false, Box::new(self.memory.iter_has_false()) as IdIter),
            (true, Box::new(self.memory.iter_has_true()) as IdIter),
        ]
        .into_iter()
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (bool, usize)> + '_ {
        vec![
            (false, self.memory.falses_count()),
            (true, self.memory.trues_count()),
        ]
        .into_iter()
    }
}

pub struct BoolIndexBuilder(BoolIndex);

impl FieldIndexBuilderTrait for BoolIndexBuilder {
    type FieldIndexType = BoolIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.0.db_wrapper.recreate_column_family()
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

impl PayloadFieldIndex for BoolIndex {
    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        }

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let idx = PointOffsetType::from_be_bytes(key.as_ref().try_into().unwrap());

            debug_assert_eq!(value.len(), 1);

            let item = BooleanItem::from(value[0]);
            self.memory.set_or_insert(idx, &item);
        }
        Ok(true)
    }

    fn cleanup(self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }

    fn flusher(&self) -> crate::common::Flusher {
        self.db_wrapper.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                if *value {
                    Some(Box::new(self.memory.iter_has_true()))
                } else {
                    Some(Box::new(self.memory.iter_has_false()))
                }
            }
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let count = if *value {
                    self.memory.trues_count()
                } else {
                    self.memory.falses_count()
                };

                let estimation = CardinalityEstimation::exact(count)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone())));

                Some(estimation)
            }
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let make_block = |count, value, key: PayloadKeyType| {
            if count > threshold {
                Some(PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key,
                        Match::Value(MatchValue {
                            value: ValueVariants::Bool(value),
                        }),
                    ),
                    cardinality: count,
                })
            } else {
                None
            }
        };

        // just two possible blocks: true and false
        let iter = [
            make_block(self.memory.trues_count(), true, key.clone()),
            make_block(self.memory.falses_count(), false, key),
        ]
        .into_iter()
        .flatten();

        Box::new(iter)
    }

    fn count_indexed_points(&self) -> usize {
        self.memory.indexed_count()
    }
}

impl ValueIndexer for BoolIndex {
    type ValueType = bool;

    fn add_many(&mut self, id: PointOffsetType, values: Vec<bool>) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let has_true = values.iter().any(|v| *v);
        let has_false = values.iter().any(|v| !*v);

        let item = BooleanItem::from_bools(has_true, has_false);

        self.memory.set_or_insert(id, &item);

        self.db_wrapper.put(id.to_be_bytes(), item.as_bytes())?;

        Ok(())
    }

    fn get_value(value: &serde_json::Value) -> Option<bool> {
        value.as_bool()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.memory.remove(id);
        self.db_wrapper.remove(id.to_be_bytes())?;
        Ok(())
    }
}
