use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use self::memory::{BinaryItem, BinaryMemory};
use super::{CardinalityEstimation, PayloadFieldIndex, PrimaryCondition, ValueIndexer};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, Match, MatchValue, PayloadKeyType, ValueVariants};

mod memory {
    use bitvec::vec::BitVec;
    use common::types::PointOffsetType;

    pub struct BinaryItem {
        value: u8,
    }

    impl BinaryItem {
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

    impl From<u8> for BinaryItem {
        fn from(value: u8) -> Self {
            Self { value }
        }
    }

    pub struct BinaryMemory {
        trues: BitVec,
        falses: BitVec,
        trues_count: usize,
        falses_count: usize,
        indexed_count: usize,
    }

    impl BinaryMemory {
        pub fn new() -> Self {
            Self {
                trues: BitVec::new(),
                falses: BitVec::new(),
                trues_count: 0,
                falses_count: 0,
                indexed_count: 0,
            }
        }

        pub fn get(&self, id: PointOffsetType) -> BinaryItem {
            debug_assert!(self.trues.len() == self.falses.len());

            let has_true = self.trues.get(id as usize).map(|v| *v).unwrap_or(false);
            let has_false = self.falses.get(id as usize).map(|v| *v).unwrap_or(false);

            BinaryItem::from_bools(has_true, has_false)
        }

        pub fn set_or_insert(&mut self, id: PointOffsetType, item: &BinaryItem) {
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

            self.indexed_count += 1;
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

pub struct BinaryIndex {
    memory: BinaryMemory,
    db_wrapper: DatabaseColumnWrapper,
}

impl BinaryIndex {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> BinaryIndex {
        let store_cf_name = Self::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            memory: BinaryMemory::new(),
            db_wrapper,
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{}_binary", field)
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
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
        binary_item.has_true() as usize + binary_item.has_false() as usize
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
}

impl PayloadFieldIndex for BinaryIndex {
    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        }

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let idx = PointOffsetType::from_be_bytes(key.as_ref().try_into().unwrap());

            debug_assert_eq!(value.len(), 1);

            let item = BinaryItem::from(value[0]);
            self.memory.set_or_insert(idx, &item);
        }
        Ok(true)
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }

    fn flusher(&self) -> crate::common::Flusher {
        self.db_wrapper.flusher()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                if *value {
                    Ok(Box::new(self.memory.iter_has_true()))
                } else {
                    Ok(Box::new(self.memory.iter_has_false()))
                }
            }
            _ => Err(OperationError::service_error("failed to filter")),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
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
                    .with_primary_clause(PrimaryCondition::Condition(condition.clone()));

                Ok(estimation)
            }
            _ => Err(OperationError::service_error(
                "failed to estimate cardinality",
            )),
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = super::PayloadBlockCondition> + '_> {
        let make_block = |count, value, key: PayloadKeyType| {
            if count > threshold {
                Some(super::PayloadBlockCondition {
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

impl ValueIndexer<bool> for BinaryIndex {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<bool>) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        let has_true = values.iter().any(|v| *v);
        let has_false = values.iter().any(|v| !*v);

        let item = BinaryItem::from_bools(has_true, has_false);

        self.memory.set_or_insert(id, &item);

        self.db_wrapper.put(id.to_be_bytes(), item.as_bytes())?;

        Ok(())
    }

    fn get_value(&self, value: &serde_json::Value) -> Option<bool> {
        value.as_bool()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.memory.remove(id);
        self.db_wrapper.remove(id.to_be_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rstest::rstest;
    use serde_json::json;
    use tempfile::{Builder, TempDir};

    use super::BinaryIndex;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::index::field_index::{PayloadFieldIndex, ValueIndexer};

    const FIELD_NAME: &str = "bool_field";
    const DB_NAME: &str = "test_db";

    fn new_binary_index() -> (TempDir, BinaryIndex) {
        let tmp_dir = Builder::new().prefix(DB_NAME).tempdir().unwrap();
        let db = open_db_with_existing_cf(tmp_dir.path()).unwrap();
        let index = BinaryIndex::new(db, FIELD_NAME);
        index.recreate().unwrap();
        (tmp_dir, index)
    }

    fn match_bool(value: bool) -> crate::types::FieldCondition {
        crate::types::FieldCondition::new_match(
            FIELD_NAME.to_string(),
            crate::types::Match::Value(crate::types::MatchValue {
                value: crate::types::ValueVariants::Bool(value),
            }),
        )
    }

    fn bools_fixture() -> Vec<serde_json::Value> {
        vec![
            json!(true),
            json!(false),
            json!([true, false]),
            json!([false, true]),
            json!([true, true]),
            json!([false, false]),
            json!([true, false, true]),
            serde_json::Value::Null,
            json!(1),
            json!("test"),
            json!([false]),
            json!([true]),
        ]
    }

    fn filter(given: serde_json::Value, match_on: bool, expected_count: usize) {
        let (_tmp_dir, mut index) = new_binary_index();

        index.add_point(0, &[&given]).unwrap();

        let count = index.filter(&match_bool(match_on)).unwrap().count();

        assert_eq!(count, expected_count);
    }

    #[rstest]
    #[case(json!(true), 1)]
    #[case(json!(false), 0)]
    #[case(json!([true]), 1)]
    #[case(json!([false]), 0)]
    #[case(json!([true, false]), 1)]
    #[case(json!([false, true]), 1)]
    #[case(json!([false, false]), 0)]
    #[case(json!([true, true]), 1)]
    fn filter_true(#[case] given: serde_json::Value, #[case] expected_count: usize) {
        filter(given, true, expected_count)
    }

    #[rstest]
    #[case(json!(true), 0)]
    #[case(json!(false), 1)]
    #[case(json!([true]), 0)]
    #[case(json!([false]), 1)]
    #[case(json!([true, false]), 1)]
    #[case(json!([false, true]), 1)]
    #[case(json!([false, false]), 1)]
    #[case(json!([true, true]), 0)]
    fn filter_false(#[case] given: serde_json::Value, #[case] expected_count: usize) {
        filter(given, false, expected_count)
    }

    #[test]
    fn load_from_disk() {
        let (_tmp_dir, mut index) = new_binary_index();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value]).unwrap();
            });

        index.flusher()().unwrap();
        let db = index.db_wrapper.database;

        let mut new_index = BinaryIndex::new(db, FIELD_NAME);
        assert!(new_index.load().unwrap());

        let point_offsets = new_index.filter(&match_bool(false)).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1, 2, 3, 5, 6, 10]);

        let point_offsets = new_index.filter(&match_bool(true)).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![0, 2, 3, 4, 6, 11]);
    }

    #[rstest]
    #[case(json!(false), json!(true))]
    #[case(json!([false, true]), json!(true))]
    /// Try to modify from falsy to only true
    fn modify_value(#[case] before: serde_json::Value, #[case] after: serde_json::Value) {
        let (_tmp_dir, mut index) = new_binary_index();

        let idx = 1000;
        index.add_point(idx, &[&before]).unwrap();

        let point_offsets = index.filter(&match_bool(false)).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![idx]);

        index.add_point(idx, &[&after]).unwrap();

        let point_offsets = index.filter(&match_bool(true)).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![idx]);
        let point_offsets = index.filter(&match_bool(false)).unwrap().collect_vec();
        assert!(point_offsets.is_empty());
    }

    #[test]
    fn indexed_count() {
        let (_tmp_dir, mut index) = new_binary_index();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value]).unwrap();
            });

        assert_eq!(index.count_indexed_points(), 9);
    }

    #[test]
    fn payload_blocks() {
        let (_tmp_dir, mut index) = new_binary_index();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value]).unwrap();
            });

        let blocks = index
            .payload_blocks(0, FIELD_NAME.to_string())
            .collect_vec();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].cardinality, 6);
        assert_eq!(blocks[1].cardinality, 6);
    }

    #[test]
    fn estimate_cardinality() {
        let (_tmp_dir, mut index) = new_binary_index();

        bools_fixture()
            .into_iter()
            .enumerate()
            .for_each(|(i, value)| {
                index.add_point(i as u32, &[&value]).unwrap();
            });

        let cardinality = index.estimate_cardinality(&match_bool(true)).unwrap();
        assert_eq!(cardinality.exp, 6);

        let cardinality = index.estimate_cardinality(&match_bool(false)).unwrap();
        assert_eq!(cardinality.exp, 6);
    }
}
