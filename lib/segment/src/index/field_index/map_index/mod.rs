pub mod immutable_map_index;
pub mod mutable_map_index;

use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use immutable_map_index::ImmutableMapIndex;
use itertools::Itertools;
use mutable_map_index::MutableMapIndex;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;
use smol_str::SmolStr;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, ValueVariants,
};
use crate::vector_storage::div_ceil;

pub enum MapIndex<N: Hash + Eq + Clone + Display + FromStr> {
    Mutable(MutableMapIndex<N>),
    Immutable(ImmutableMapIndex<N>),
}

impl<N: Hash + Eq + Clone + Display + FromStr + Default> MapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str, is_appendable: bool) -> Self {
        if is_appendable {
            MapIndex::Mutable(MutableMapIndex::new(db, field_name))
        } else {
            MapIndex::Immutable(ImmutableMapIndex::new(db, field_name))
        }
    }

    fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        match self {
            MapIndex::Mutable(index) => index.get_db_wrapper(),
            MapIndex::Immutable(index) => index.get_db_wrapper(),
        }
    }

    fn load_from_db(&mut self) -> OperationResult<bool> {
        match self {
            MapIndex::Mutable(index) => index.load_from_db(),
            MapIndex::Immutable(index) => index.load_from_db(),
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        match self {
            MapIndex::Mutable(index) => index.get_values(idx),
            MapIndex::Immutable(index) => index.get_values(idx),
        }
    }

    fn get_indexed_points(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_indexed_points(),
            MapIndex::Immutable(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_values_count(),
            MapIndex::Immutable(index) => index.get_values_count(),
        }
    }

    fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_unique_values_count(),
            MapIndex::Immutable(index) => index.get_unique_values_count(),
        }
    }

    fn get_points_with_value_count<Q>(&self, value: &Q) -> Option<usize>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            MapIndex::Mutable(index) => index.get_points_with_value_count(value),
            MapIndex::Immutable(index) => index.get_points_with_value_count(value),
        }
    }

    fn get_iterator<Q>(&self, value: &Q) -> Box<dyn Iterator<Item = PointOffsetType> + '_>
    where
        Q: ?Sized,
        N: std::borrow::Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            MapIndex::Mutable(index) => index.get_iterator(value),
            MapIndex::Immutable(index) => index.get_iterator(value),
        }
    }

    fn get_values_iterator(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        match self {
            MapIndex::Mutable(index) => index.get_values_iterator(),
            MapIndex::Immutable(index) => index.get_values_iterator(),
        }
    }

    pub fn storage_cf_name(field: &str) -> String {
        format!("{field}_map")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

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

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_indexed_points(),
            points_values_count: self.get_values_count(),
            histogram_bucket_size: None,
        }
    }

    pub fn encode_db_record(value: &N, idx: PointOffsetType) -> String {
        format!("{value}/{idx}")
    }

    pub fn decode_db_record(s: &str) -> OperationResult<(N, PointOffsetType)> {
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

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.get_values(point_id).map(|x| x.len()).unwrap_or(0)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
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

impl PayloadFieldIndex for MapIndex<SmolStr> {
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
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => Ok(self.get_iterator(keyword.as_str())),
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Keywords(keywords) => Ok(Box::new(
                    keywords
                        .iter()
                        .flat_map(|keyword| self.get_iterator(keyword.as_str()))
                        .unique(),
                )),
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Ok(Box::new(vec![].into_iter()))
                    } else {
                        Err(OperationError::service_error(
                            "failed to estimate cardinality",
                        ))
                    }
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Ok(self.except_iterator(keywords)),
            _ => Err(OperationError::service_error("failed to filter")),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => {
                let mut estimation = self.match_cardinality(keyword.as_str());
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Ok(estimation)
            }
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Keywords(keywords) => {
                    let estimations = keywords
                        .iter()
                        .map(|keyword| self.match_cardinality(keyword.as_str()))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Ok(estimation
                        .with_primary_clause(PrimaryCondition::Condition(condition.clone())))
                }
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Ok(CardinalityEstimation::exact(0)
                            .with_primary_clause(PrimaryCondition::Condition(condition.clone())))
                    } else {
                        Err(OperationError::service_error(
                            "failed to estimate cardinality",
                        ))
                    }
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Ok(self.except_cardinality::<str, &str>(keywords.iter().map(|k| k.as_str()))),
            _ => Err(OperationError::service_error(
                "failed to estimate cardinality",
            )),
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

impl PayloadFieldIndex for MapIndex<IntPayloadType> {
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
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => Ok(self.get_iterator(integer)),
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Keywords(keywords) => {
                    if keywords.is_empty() {
                        Ok(Box::new(vec![].into_iter()))
                    } else {
                        Err(OperationError::service_error(
                            "failed to estimate cardinality",
                        ))
                    }
                }
                AnyVariants::Integers(integers) => Ok(Box::new(
                    integers
                        .iter()
                        .flat_map(|integer| self.get_iterator(integer))
                        .unique(),
                )),
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => Ok(self.except_iterator(integers)),
            _ => Err(OperationError::service_error("failed to filter")),
        }
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => {
                let mut estimation = self.match_cardinality(integer);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                Ok(estimation)
            }
            Some(Match::Any(MatchAny { any: any_variants })) => match any_variants {
                AnyVariants::Keywords(keywords) => {
                    if keywords.is_empty() {
                        Ok(CardinalityEstimation::exact(0)
                            .with_primary_clause(PrimaryCondition::Condition(condition.clone())))
                    } else {
                        Err(OperationError::service_error(
                            "failed to estimate cardinality",
                        ))
                    }
                }
                AnyVariants::Integers(integers) => {
                    let estimations = integers
                        .iter()
                        .map(|integer| self.match_cardinality(integer))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Ok(estimation
                        .with_primary_clause(PrimaryCondition::Condition(condition.clone())))
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => {
                Ok(self
                    .except_cardinality::<IntPayloadType, IntPayloadType>(integers.iter().cloned()))
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

impl ValueIndexer<String> for MapIndex<SmolStr> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
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
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
        }
    }
}

impl ValueIndexer<IntPayloadType> for MapIndex<IntPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
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
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
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

    fn save_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug + Default>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index =
            MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME, true);
        index.recreate().unwrap();
        for (idx, values) in data.iter().enumerate() {
            match &mut index {
                MapIndex::Mutable(index) => index
                    .add_many_to_map(idx as PointOffsetType, values.clone())
                    .unwrap(),
                _ => panic!("Wrong index type"),
            }
        }
        index.flusher()().unwrap();
    }

    fn load_map_index<N: Hash + Eq + Clone + Display + FromStr + Debug + Default>(
        data: &[Vec<N>],
        path: &Path,
    ) {
        let mut index =
            MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME, true);
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
