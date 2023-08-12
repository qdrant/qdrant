use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::hash::Hash;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;
use smol_str::SmolStr;

use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, PointOffsetType, ValueVariants,
};
use crate::vector_storage::div_ceil;

/// HashMap-based type of index
pub struct MutableMapIndex<N: Hash + Eq + Clone + Display + FromStr> {
    map: HashMap<N, BTreeSet<PointOffsetType>>,
    point_to_values: Vec<Vec<N>>,
    /// Amount of point which have at least one indexed payload value
    indexed_points: usize,
    values_count: usize,
    db_wrapper: DatabaseColumnWrapper,
}

pub enum MapIndexEnum<N: Hash + Eq + Clone + Display + FromStr> {
    Mutable(MutableMapIndex<N>),
}

impl<N: Hash + Eq + Clone + Display + FromStr> MutableMapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        let store_cf_name = Self::storage_cf_name(field_name);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: Default::default(),
            point_to_values: Vec::new(),
            indexed_points: 0,
            values_count: 0,
            db_wrapper,
        }
    }

    fn add_many_to_map<Q>(&mut self, idx: PointOffsetType, values: Vec<Q>) -> OperationResult<()>
    where
        Q: Into<N>,
    {
        if values.is_empty() {
            return Ok(());
        }

        self.values_count += values.len();
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize(idx as usize + 1, Vec::new())
        }
        self.point_to_values[idx as usize] = values.into_iter().map(|v| v.into()).collect();
        for value in &self.point_to_values[idx as usize] {
            let entry = self.map.entry(value.clone()).or_default();
            entry.insert(idx);

            let db_record = Self::encode_db_record(value, idx);
            self.db_wrapper.put(db_record, [])?;
        }
        self.indexed_points += 1;
        Ok(())
    }

    fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(());
        }

        let removed_values = std::mem::take(&mut self.point_to_values[idx as usize]);

        if !removed_values.is_empty() {
            self.indexed_points -= 1;
        }
        self.values_count -= removed_values.len();

        for value in &removed_values {
            if let Some(vals) = self.map.get_mut(value) {
                vals.remove(&idx);
            }
            let key = Self::encode_db_record(value, idx);
            self.db_wrapper.remove(key)?;
        }

        Ok(())
    }
}

impl<N: Hash + Eq + Clone + Display + FromStr> MapIndex<N> for MutableMapIndex<N> {
    fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    fn load_from_db(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        }
        self.indexed_points = 0;
        for (record, _) in self.db_wrapper.lock_db().iter()? {
            let record = std::str::from_utf8(&record).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;
            let (value, idx) = Self::decode_db_record(record)?;
            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize(idx as usize + 1, Vec::new())
            }
            if self.point_to_values[idx as usize].is_empty() {
                self.indexed_points += 1;
            }
            self.values_count += 1;
            self.point_to_values[idx as usize].push(value.clone());
            self.map.entry(value).or_default().insert(idx);
        }
        Ok(true)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        self.point_to_values.get(idx as usize).map(|v| v.as_slice())
    }

    fn get_indexed_points(&self) -> usize {
        self.indexed_points
    }

    fn get_values_count(&self) -> usize {
        self.values_count
    }

    fn get_unique_values_count(&self) -> usize {
        self.map.len()
    }

    fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map.get(value).map(|p| p.len())
    }

    fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        self.map
            .get(value)
            .map(|ids| Box::new(ids.iter().copied()) as Box<dyn Iterator<Item = PointOffsetType>>)
            .unwrap_or_else(|| Box::new(iter::empty::<PointOffsetType>()))
    }

    fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.map.keys())
    }

    fn except_iterator<'a, Q>(
        &'a self,
        excluded: &'a [Q],
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>
    where
        Q: PartialEq<N>,
    {
        Box::new(
            self.get_values_iterator()
                .filter(|key| !excluded.iter().any(|e| e.eq(*key)))
                .flat_map(|key| self.get_iterator(key))
                .unique(),
        )
    }
}

impl<N: Hash + Eq + Clone + Display + FromStr> MapIndexEnum<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str) -> Self {
        MapIndexEnum::Mutable(MutableMapIndex::new(db, field_name))
    }
}

impl<N: Hash + Eq + Clone + Display + FromStr> MapIndex<N> for MapIndexEnum<N> {
    fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        match self {
            MapIndexEnum::Mutable(index) => index.get_db_wrapper(),
        }
    }

    fn load_from_db(&mut self) -> OperationResult<bool> {
        match self {
            MapIndexEnum::Mutable(index) => index.load_from_db(),
        }
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        match self {
            MapIndexEnum::Mutable(index) => index.get_values(idx),
        }
    }

    fn get_indexed_points(&self) -> usize {
        match self {
            MapIndexEnum::Mutable(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndexEnum::Mutable(index) => index.get_values_count(),
        }
    }

    fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndexEnum::Mutable(index) => index.get_unique_values_count(),
        }
    }

    fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            MapIndexEnum::Mutable(index) => index.get_points_with_value_count(value),
        }
    }

    fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            MapIndexEnum::Mutable(index) => index.get_iterator(value),
        }
    }

    fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        match self {
            MapIndexEnum::Mutable(index) => index.get_values_iterator(),
        }
    }

    fn except_iterator<'a, Q>(
        &'a self,
        excluded: &'a [Q],
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>
    where
        Q: PartialEq<N>,
    {
        match self {
            MapIndexEnum::Mutable(index) => index.except_iterator(excluded),
        }
    }
}

pub trait MapIndex<N: Hash + Eq + Clone + Display + FromStr> {
    fn get_db_wrapper(&self) -> &DatabaseColumnWrapper;

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_map")
    }

    fn recreate(&self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    fn load_from_db(&mut self) -> OperationResult<bool>;

    fn flusher(&self) -> Flusher {
        self.get_db_wrapper().flusher()
    }

    fn match_cardinality<Q>(&self, value: &Q) -> CardinalityEstimation
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        let values_count = self.get_points_with_value_count(value).unwrap_or(0);

        CardinalityEstimation::exact(values_count)
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<&[N]>;

    fn get_indexed_points(&self) -> usize;

    fn get_values_count(&self) -> usize;

    fn get_unique_values_count(&self) -> usize;

    fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq;

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_indexed_points(),
            points_values_count: self.get_values_count(),
            histogram_bucket_size: None,
        }
    }

    fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq;

    fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_>;

    fn encode_db_record(value: &N, idx: PointOffsetType) -> String {
        format!("{value}/{idx}")
    }

    fn decode_db_record(s: &str) -> OperationResult<(N, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let value_str = &s[..separator_pos];
        let value =
            N::from_str(value_str).map_err(|_| OperationError::service_error(DECODE_ERR))?;
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((value, idx))
    }

    fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.get_values(point_id).map(|x| x.len()).unwrap_or(0)
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_values(point_id)
            .map(|x| x.is_empty())
            .unwrap_or(true)
    }

    /// Estimates cardinality for `except` clause
    ///
    /// # Arguments
    ///
    /// * 'excluded' - values, which are not considered as matching
    ///
    /// # Returns
    ///
    /// * `CardinalityEstimation` - estimation of cardinality
    fn except_cardinality<Q, I>(&self, excluded: impl Iterator<Item = I>) -> CardinalityEstimation
    where
        I: std::borrow::Borrow<Q>,
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        // Minimal case: we exclude as many points as possible.
        // In this case, excluded points do not have any other values except excluded ones.
        // So the first step - we estimate how many other points is needed to fit unused values.

        // Example:
        // Values: 20, 20
        // Unique values: 5
        // Total points: 100
        // Total values: 110
        // total_excluded_value_count = 40
        // non_excluded_values_count = 110 - 40 = 70
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 70 / 3 = 24
        // min = max(24, 100 - 40) = 60
        // exp = ...
        // max = min(20, 70) = 20

        // Values: 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 120
        // non_excluded_values_count = 200 - 120 = 80
        // max_values_per_point = 5 - 2 = 3
        // min_not_excluded_by_values = 80 / 3 = 27
        // min = max(27, 100 - 120) = 27
        // exp = ...
        // max = min(60, 80) = 60

        // Values: 60, 60, 60
        // Unique values: 5
        // Total points: 100
        // Total values: 200
        // total_excluded_value_count = 180
        // non_excluded_values_count = 200 - 180 = 20
        // max_values_per_point = 5 - 3 = 2
        // min_not_excluded_by_values = 20 / 2 = 10
        // min = max(10, 100 - 180) = 10
        // exp = ...
        // max = min(60, 20) = 20

        // todo
        let excluded_value_counts: Vec<_> = excluded
            .map(|val| self.get_points_with_value_count(val.borrow()).unwrap_or(0))
            .collect();
        let total_excluded_value_count: usize = excluded_value_counts.iter().sum();

        debug_assert!(total_excluded_value_count <= self.get_values_count());

        let non_excluded_values_count = self
            .get_values_count()
            .saturating_sub(total_excluded_value_count);
        let max_values_per_point = self
            .get_unique_values_count()
            .saturating_sub(excluded_value_counts.len());

        if max_values_per_point == 0 {
            // All points are excluded, so we can't select any point
            debug_assert_eq!(non_excluded_values_count, 0);
            return CardinalityEstimation::exact(0);
        }

        // Minimal amount of points, required to fit all unused values.
        // Cardinality can't be less than this value.
        let min_not_excluded_by_values = div_ceil(non_excluded_values_count, max_values_per_point);

        let min = min_not_excluded_by_values.max(
            self.get_indexed_points()
                .saturating_sub(total_excluded_value_count),
        );

        // Maximum scenario: selected points overlap as much as possible.
        // From one side, all excluded values should be assigned to the same point
        // => we can take the value with the maximum amount of points.
        // From another side, all other values should be enough to fill all other points.

        let max_excluded_value_count = excluded_value_counts.iter().max().copied().unwrap_or(0);

        let max = self
            .get_indexed_points()
            .saturating_sub(max_excluded_value_count)
            .min(non_excluded_values_count);

        // Expected case: we assume that all points are filled equally.
        // So we can estimate the probability of the point to have non-excluded value.
        let exp = number_of_selected_points(self.get_indexed_points(), non_excluded_values_count)
            .max(min)
            .min(max);

        CardinalityEstimation {
            primary_clauses: vec![],
            min,
            exp,
            max,
        }
    }

    fn except_iterator<'a, Q>(
        &'a self,
        excluded: &'a [Q],
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>
    where
        Q: PartialEq<N>;
}

impl PayloadFieldIndex for MapIndexEnum<SmolStr> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn load(&mut self) -> OperationResult<bool> {
        self.load_from_db()
    }

    fn clear(self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => Some(self.get_iterator(keyword.as_str())),
            Some(Match::Any(MatchAny {
                any: AnyVariants::Keywords(keywords),
            })) => Some(Box::new(
                keywords
                    .iter()
                    .flat_map(|keyword| self.get_iterator(keyword.as_str()))
                    .unique(),
            )),
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Some(self.except_iterator(keywords)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => {
                let mut estimation = self.match_cardinality(keyword.as_str());
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Some(estimation)
            }
            Some(Match::Any(MatchAny {
                any: AnyVariants::Keywords(keywords),
            })) => {
                let estimations = keywords
                    .iter()
                    .map(|keyword| self.match_cardinality(keyword.as_str()))
                    .collect::<Vec<_>>();
                Some(combine_should_estimations(
                    &estimations,
                    self.get_indexed_points(),
                ))
            }
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Some(self.except_cardinality::<str, &str>(keywords.iter().map(|k| k.as_str()))),
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.get_values_iterator()
                .map(|value| (value, self.get_points_with_value_count(value).unwrap_or(0)))
                .filter(move |(_value, count)| *count > threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), value.to_owned().into()),
                    cardinality: count,
                }),
        )
    }
}

impl PayloadFieldIndex for MapIndexEnum<IntPayloadType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn load(&mut self) -> OperationResult<bool> {
        self.load_from_db()
    }

    fn clear(self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => Some(self.get_iterator(integer)),
            Some(Match::Any(MatchAny {
                any: AnyVariants::Integers(integers),
            })) => Some(Box::new(
                integers
                    .iter()
                    .flat_map(|integer| self.get_iterator(integer))
                    .unique(),
            )),
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => Some(self.except_iterator(integers)),
            _ => None,
        }
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => {
                let mut estimation = self.match_cardinality(integer);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Some(estimation)
            }
            Some(Match::Any(MatchAny {
                any: AnyVariants::Integers(integers),
            })) => {
                let estimations = integers
                    .iter()
                    .map(|integer| self.match_cardinality(integer))
                    .collect::<Vec<_>>();
                Some(combine_should_estimations(
                    &estimations,
                    self.get_indexed_points(),
                ))
            }
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => Some(
                self.except_cardinality::<IntPayloadType, IntPayloadType>(integers.iter().cloned()),
            ),
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.get_values_iterator()
                .map(|value| (value, self.get_points_with_value_count(value).unwrap_or(0)))
                .filter(move |(_value, count)| *count >= threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), value.to_owned().into()),
                    cardinality: count,
                }),
        )
    }
}

impl ValueIndexer<String> for MapIndexEnum<SmolStr> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        match self {
            MapIndexEnum::Mutable(index) => index.add_many_to_map(id, values),
        }
    }

    fn get_value(&self, value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndexEnum::Mutable(index) => index.remove_point(id),
        }
    }
}

impl ValueIndexer<IntPayloadType> for MapIndexEnum<IntPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        match self {
            MapIndexEnum::Mutable(index) => index.add_many_to_map(id, values),
        }
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndexEnum::Mutable(index) => index.remove_point(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::Debug;
    use std::iter::FromIterator;
    use std::path::Path;

    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;

    const FIELD_NAME: &str = "test";

    fn save_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index = MapIndexEnum::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME);
        index.recreate().unwrap();
        for (idx, values) in data.iter().enumerate() {
            match &mut index {
                MapIndexEnum::Mutable(index) => index
                    .add_many_to_map(idx as PointOffsetType, values.clone())
                    .unwrap(),
            }
        }
        index.flusher()().unwrap();
    }

    fn load_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index = MapIndexEnum::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME);
        index.load_from_db().unwrap();
        for (idx, values) in data.iter().enumerate() {
            let index_values: HashSet<N> = HashSet::from_iter(
                index
                    .get_values(idx as PointOffsetType)
                    .unwrap()
                    .iter()
                    .cloned(),
            );
            let check_values: HashSet<N> = HashSet::from_iter(values.iter().cloned());
            assert_eq!(index_values, check_values);
        }
    }

    #[test]
    fn test_int_disk_map_index() {
        let data = vec![
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![13, 14, 15, 16, 17, 18],
            vec![19, 20, 21, 22, 23, 24],
            vec![25],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index(&data, temp_dir.path());
        load_map_index(&data, temp_dir.path());
    }

    #[test]
    fn test_string_disk_map_index() {
        let data = vec![
            vec![
                String::from("AABB"),
                String::from("UUFF"),
                String::from("IIBB"),
            ],
            vec![
                String::from("PPMM"),
                String::from("QQXX"),
                String::from("YYBB"),
            ],
            vec![
                String::from("FFMM"),
                String::from("IICC"),
                String::from("IIBB"),
            ],
            vec![
                String::from("AABB"),
                String::from("UUFF"),
                String::from("IIBB"),
            ],
            vec![String::from("PPGG")],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index(&data, temp_dir.path());
        load_map_index(&data, temp_dir.path());
    }
}
