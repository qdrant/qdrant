use std::sync::Arc;

use bitvec::vec::BitVec;
use parking_lot::RwLock;
use rocksdb::DB;

use super::{CardinalityEstimation, PayloadFieldIndex, PrimaryCondition, ValueIndexer};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::entry::entry_point::OperationResult;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, Match, MatchValue, PayloadKeyType, PointOffsetType, ValueVariants,
};

pub(self) struct BinaryMemory {
    trues: BitVec,
    falses: BitVec,
    indexed_count: usize,
}

/// Due to being able to store multi-values, the binary index is not a simple bitset, but rather a pair of bitsets, one for true values and one for false values.
enum BinaryItem {
    True,
    False,
    Both,
    None,
}

impl BinaryMemory {
    pub fn new() -> Self {
        Self {
            trues: BitVec::new(),
            falses: BitVec::new(),
            indexed_count: 0,
        }
    }

    pub fn get(&self, id: PointOffsetType) -> BinaryItem {
        debug_assert!(self.trues.len() == self.falses.len());
        if (id as usize) < self.trues.len() {
            unsafe {
                // SAFETY: we just checked that the id is within bounds
                match (
                    self.trues.get_unchecked(id as usize).as_ref(),
                    self.falses.get_unchecked(id as usize).as_ref(),
                ) {
                    (true, true) => BinaryItem::Both,
                    (true, false) => BinaryItem::True,
                    (false, true) => BinaryItem::False,
                    (false, false) => BinaryItem::None,
                }
            }
        } else {
            BinaryItem::None
        }
    }

    pub fn set_or_insert(&mut self, id: PointOffsetType, value: bool) {
        if (id as usize) >= self.trues.len() {
            self.trues.resize(id as usize + 1, false);
            self.falses.resize(id as usize + 1, false);
            self.indexed_count += 1;
        }

        debug_assert!(self.trues.len() == self.falses.len());

        unsafe {
            // SAFETY: we just resized the vectors to be at least as long as the id
            if value {
                self.trues.set_unchecked(id as usize, true);
            } else {
                self.falses.set_unchecked(id as usize, true);
            }
        }
    }

    /// Removes the point from the index and tries to shrink the vectors if possible. If the index is not within bounds, does nothing
    pub fn remove(&mut self, id: PointOffsetType) {
        if (id as usize) < self.trues.len() {
            self.trues.set(id as usize, false);
            self.falses.set(id as usize, false);
            self.indexed_count -= 1;
        }

        // shrink the vectors if possible
        let last_populated_index = self.trues.last_one().max(self.falses.last_one());
        match last_populated_index {
            Some(index) if index < self.trues.len() - 1 => {
                self.trues.truncate(index + 1);
                self.falses.truncate(index + 1);
            }
            None => {
                self.trues.clear();
                self.falses.clear();
            }
            _ => {}
        }
    }

    pub fn count_trues(&self) -> usize {
        self.trues.count_ones()
    }

    pub fn count_falses(&self) -> usize {
        self.falses.count_ones()
    }

    pub fn indexed_count(&self) -> usize {
        self.indexed_count
    }

    pub fn iter(&self) -> BinaryMemoryIterator {
        BinaryMemoryIterator {
            memory: self,
            ptr: 0,
            end: self
                .trues
                .last_one()
                .max(self.falses.last_one())
                .unwrap_or(0),
        }
    }
}

struct BinaryMemoryIterator<'a> {
    memory: &'a BinaryMemory,
    ptr: usize,
    end: usize,
}

impl<'a> Iterator for BinaryMemoryIterator<'a> {
    type Item = BinaryItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            return None;
        }

        let item = self.memory.get(self.ptr as PointOffsetType);
        self.ptr += 1;

        Some(item)
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
        format!("{field}_binary")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.memory.indexed_count(),
            points_values_count: self.memory.count_falses() + self.memory.count_falses(),
            histogram_bucket_size: None,
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self.memory.get(point_id) {
            BinaryItem::Both => 2,
            BinaryItem::True | BinaryItem::False => 1,
            BinaryItem::None => 0,
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        matches!(self.memory.get(point_id), BinaryItem::None)
    }
}

impl PayloadFieldIndex for BinaryIndex {
    fn indexed_points(&self) -> usize {
        self.memory.indexed_count()
    }

    fn load(&mut self) -> crate::entry::entry_point::OperationResult<bool> {
        todo!()
    }

    fn clear(self) -> crate::entry::entry_point::OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }

    fn flusher(&self) -> crate::common::Flusher {
        self.db_wrapper.flusher()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a crate::types::FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Bool(value),
            })) => {
                let iter = self
                    .memory
                    .iter()
                    .zip(0u32..) // enumerate but with u32
                    .filter_map(|(stored, point_id)| match stored {
                        BinaryItem::Both | BinaryItem::True if *value => Some(point_id),
                        BinaryItem::Both | BinaryItem::False if !*value => Some(point_id),
                        _ => None,
                    });

                Some(Box::new(iter))
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
                    self.memory.count_trues()
                } else {
                    self.memory.count_falses()
                };

                let estimation = CardinalityEstimation::exact(count)
                    .with_primary_clause(PrimaryCondition::Condition(condition.clone()));

                Some(estimation)
            }
            _ => None,
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
            make_block(self.memory.count_trues(), true, key.clone()),
            make_block(self.memory.count_falses(), false, key),
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
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<bool>,
    ) -> crate::entry::entry_point::OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        if values.iter().any(|v| *v) {
            self.memory.set_or_insert(id, true);
        }

        if values.iter().any(|v| !v) {
            self.memory.set_or_insert(id, false);
        }

        Ok(())
    }

    fn get_value(&self, value: &serde_json::Value) -> Option<bool> {
        value.as_bool()
    }

    fn remove_point(
        &mut self,
        id: PointOffsetType,
    ) -> crate::entry::entry_point::OperationResult<()> {
        self.memory.remove(id);
        Ok(())
    }
}
