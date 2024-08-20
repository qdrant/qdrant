use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::{BuildHasher, Hash};
use std::iter;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use ahash::HashMap;
use common::mmap_hashmap::Key;
use common::types::PointOffsetType;
use indexmap::IndexSet;
use itertools::Itertools;
use mmap_map_index::MmapMapIndex;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;
use smol_str::SmolStr;
use uuid::Uuid;

use self::immutable_map_index::ImmutableMapIndex;
use self::mutable_map_index::MutableMapIndex;
use super::mmap_point_to_values::MmapValue;
use super::FieldIndexBuilderTrait;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::index::field_index::stat_tools::number_of_selected_points;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, UuidIntType, ValueVariants,
};

pub mod immutable_map_index;
pub mod mmap_map_index;
pub mod mutable_map_index;

pub type IdRefIter<'a> = Box<dyn Iterator<Item = &'a PointOffsetType> + 'a>;

pub trait MapIndexKey: Key + MmapValue + Eq + Display + Debug {
    type Owned: Borrow<Self> + Hash + Eq + Clone + FromStr + Default;

    fn to_owned(&self) -> Self::Owned;
}

impl MapIndexKey for str {
    type Owned = SmolStr;

    fn to_owned(&self) -> Self::Owned {
        SmolStr::from(self)
    }
}

impl MapIndexKey for IntPayloadType {
    type Owned = IntPayloadType;

    fn to_owned(&self) -> Self::Owned {
        *self
    }
}

impl MapIndexKey for UuidIntType {
    type Owned = UuidIntType;

    fn to_owned(&self) -> Self::Owned {
        *self
    }
}

pub enum MapIndex<N: MapIndexKey + ?Sized> {
    Mutable(MutableMapIndex<N>),
    Immutable(ImmutableMapIndex<N>),
    Mmap(Box<MmapMapIndex<N>>),
}

impl<N: MapIndexKey + ?Sized> MapIndex<N> {
    pub fn new(db: Arc<RwLock<DB>>, field_name: &str, is_appendable: bool) -> Self {
        if is_appendable {
            MapIndex::Mutable(MutableMapIndex::new(db, field_name))
        } else {
            MapIndex::Immutable(ImmutableMapIndex::new(db, field_name))
        }
    }

    pub fn new_mmap(path: &Path) -> OperationResult<Self> {
        Ok(MapIndex::Mmap(Box::new(MmapMapIndex::load(path)?)))
    }

    pub fn builder(db: Arc<RwLock<DB>>, field_name: &str) -> MapIndexBuilder<N> {
        MapIndexBuilder(MapIndex::Mutable(MutableMapIndex::new(db, field_name)))
    }

    pub fn mmap_builder(path: &Path) -> MapIndexMmapBuilder<N> {
        MapIndexMmapBuilder {
            path: path.to_owned(),
            point_to_values: Default::default(),
            values_to_points: Default::default(),
        }
    }

    fn load_from_db(&mut self) -> OperationResult<bool> {
        match self {
            MapIndex::Mutable(index) => index.load_from_db(),
            MapIndex::Immutable(index) => index.load_from_db(),
            // mmap index is always loaded
            MapIndex::Mmap(_) => Ok(true),
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&N) -> bool) -> bool {
        match self {
            MapIndex::Mutable(index) => index.check_values_any(idx, check_fn),
            MapIndex::Immutable(index) => index.check_values_any(idx, check_fn),
            MapIndex::Mmap(index) => index.check_values_any(idx, check_fn),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = N::Referenced<'_>> + '_>> {
        match self {
            MapIndex::Mutable(index) => Some(Box::new(
                index.get_values(idx)?.map(|v| N::into_referenced(v)),
            )),
            MapIndex::Immutable(index) => Some(Box::new(
                index.get_values(idx)?.map(|v| N::into_referenced(v)),
            )),
            MapIndex::Mmap(index) => Some(Box::new(index.get_values(idx)?)),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            MapIndex::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Immutable(index) => index.values_count(idx).unwrap_or_default(),
            MapIndex::Mmap(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    fn get_indexed_points(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_indexed_points(),
            MapIndex::Immutable(index) => index.get_indexed_points(),
            MapIndex::Mmap(index) => index.get_indexed_points(),
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_values_count(),
            MapIndex::Immutable(index) => index.get_values_count(),
            MapIndex::Mmap(index) => index.get_values_count(),
        }
    }

    pub fn get_unique_values_count(&self) -> usize {
        match self {
            MapIndex::Mutable(index) => index.get_unique_values_count(),
            MapIndex::Immutable(index) => index.get_unique_values_count(),
            MapIndex::Mmap(index) => index.get_unique_values_count(),
        }
    }

    fn get_count_for_value(&self, value: &N) -> Option<usize> {
        match self {
            MapIndex::Mutable(index) => index.get_count_for_value(value),
            MapIndex::Immutable(index) => index.get_count_for_value(value),
            MapIndex::Mmap(index) => index.get_count_for_value(value),
        }
    }

    fn get_iterator(&self, value: &N) -> IdRefIter<'_> {
        match self {
            MapIndex::Mutable(index) => index.get_iterator(value),
            MapIndex::Immutable(index) => index.get_iterator(value),
            MapIndex::Mmap(index) => index.get_iterator(value),
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        match self {
            MapIndex::Mutable(index) => index.iter_values(),
            MapIndex::Immutable(index) => index.iter_values(),
            MapIndex::Mmap(index) => index.iter_values(),
        }
    }

    pub fn iter_counts_per_value(&self) -> Box<dyn Iterator<Item = (&N, usize)> + '_> {
        match self {
            MapIndex::Mutable(index) => Box::new(index.iter_counts_per_value()),
            MapIndex::Immutable(index) => Box::new(index.iter_counts_per_value()),
            MapIndex::Mmap(index) => Box::new(index.iter_counts_per_value()),
        }
    }

    pub fn iter_values_map(&self) -> Box<dyn Iterator<Item = (&N, IdRefIter<'_>)> + '_> {
        match self {
            MapIndex::Mutable(index) => Box::new(index.iter_values_map()),
            MapIndex::Immutable(index) => Box::new(index.iter_values_map()),
            MapIndex::Mmap(index) => Box::new(index.iter_values_map()),
        }
    }

    pub fn storage_cf_name(field: &str) -> String {
        format!("{field}_map")
    }

    fn flusher(&self) -> Flusher {
        match self {
            MapIndex::Mutable(index) => index.get_db_wrapper().flusher(),
            MapIndex::Immutable(index) => index.get_db_wrapper().flusher(),
            MapIndex::Mmap(index) => index.flusher(),
        }
    }

    fn match_cardinality(&self, value: &N) -> CardinalityEstimation {
        let values_count = self.get_count_for_value(value).unwrap_or(0);

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

    pub fn decode_db_record(s: &str) -> OperationResult<(N::Owned, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let value_str = &s[..separator_pos];
        let value =
            N::Owned::from_str(value_str).map_err(|_| OperationError::service_error(DECODE_ERR))?;
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((value, idx))
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    fn clear(self) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.get_db_wrapper().recreate_column_family(),
            MapIndex::Immutable(index) => index.get_db_wrapper().recreate_column_family(),
            MapIndex::Mmap(index) => index.clear(),
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.remove_point(id),
            MapIndex::Immutable(index) => index.remove_point(id),
            MapIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            MapIndex::Mutable(_) => Vec::new(),
            MapIndex::Immutable(_) => Vec::new(),
            MapIndex::Mmap(index) => index.files(),
        }
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
    fn except_cardinality<'a>(
        &'a self,
        excluded: impl Iterator<Item = &'a N>,
    ) -> CardinalityEstimation {
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
            .map(|val| self.get_count_for_value(val.borrow()).unwrap_or(0))
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
        let min_not_excluded_by_values = non_excluded_values_count.div_ceil(max_values_per_point);

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

    fn except_set<'a, K, A>(
        &'a self,
        excluded: &'a IndexSet<K, A>,
    ) -> Box<dyn Iterator<Item = PointOffsetType> + 'a>
    where
        A: BuildHasher,
        K: Borrow<N> + Hash + Eq,
    {
        Box::new(
            self.iter_values()
                .filter(|key| !excluded.contains((*key).borrow()))
                .flat_map(|key| self.get_iterator(key.borrow()).copied())
                .unique(),
        )
    }
}

pub struct MapIndexBuilder<N: MapIndexKey + ?Sized>(MapIndex<N>);

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexBuilder<N>
where
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        match &mut self.0 {
            MapIndex::Mutable(index) => index.get_db_wrapper().recreate_column_family(),
            MapIndex::Immutable(_) => unreachable!(),
            MapIndex::Mmap(_) => unreachable!(),
        }
    }

    fn add_point(&mut self, id: PointOffsetType, values: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, values)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

pub struct MapIndexMmapBuilder<N: MapIndexKey + ?Sized> {
    path: PathBuf,
    point_to_values: Vec<Vec<N::Owned>>,
    values_to_points: HashMap<N::Owned, Vec<PointOffsetType>>,
}

impl<N: MapIndexKey + ?Sized> FieldIndexBuilderTrait for MapIndexMmapBuilder<N>
where
    MapIndex<N>: PayloadFieldIndex + ValueIndexer,
    <MapIndex<N> as ValueIndexer>::ValueType: Into<N::Owned>,
{
    type FieldIndexType = MapIndex<N>;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        let mut flatten_values: Vec<_> = vec![];
        for value in payload.iter() {
            let payload_values = <MapIndex<N> as ValueIndexer>::get_values(value);
            flatten_values.extend(payload_values);
        }
        let flatten_values: Vec<N::Owned> = flatten_values.into_iter().map(Into::into).collect();

        if self.point_to_values.len() <= id as usize {
            self.point_to_values.resize_with(id as usize + 1, Vec::new);
        }

        self.point_to_values[id as usize].extend(flatten_values.clone());
        for value in flatten_values {
            self.values_to_points.entry(value).or_default().push(id);
        }
        Ok(())
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(MapIndex::Mmap(Box::new(MmapMapIndex::build(
            &self.path,
            self.point_to_values,
            self.values_to_points,
        )?)))
    }
}

impl PayloadFieldIndex for MapIndex<str> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn load(&mut self) -> OperationResult<bool> {
        self.load_from_db()
    }

    fn clear(self) -> OperationResult<()> {
        self.clear()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Keyword(keyword),
            })) => Some(Box::new(self.get_iterator(keyword.as_str()).copied())),
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Keywords(keywords) => Some(Box::new(
                    keywords
                        .iter()
                        .flat_map(|keyword| self.get_iterator(keyword.as_str()).copied())
                        .unique(),
                )),
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(Box::new(iter::empty()))
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Some(self.except_set(keywords)),
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
                    Some(
                        estimation
                            .with_primary_clause(PrimaryCondition::Condition(condition.clone())),
                    )
                }
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(
                            CardinalityEstimation::exact(0).with_primary_clause(
                                PrimaryCondition::Condition(condition.clone()),
                            ),
                        )
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Keywords(keywords),
            })) => Some(self.except_cardinality(keywords.iter().map(|k| k.as_str()))),
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(|value| (value, self.get_count_for_value(value).unwrap_or(0)))
                .filter(move |(_value, count)| *count > threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), value.to_string().into()),
                    cardinality: count,
                }),
        )
    }
}

impl PayloadFieldIndex for MapIndex<UuidIntType> {
    fn count_indexed_points(&self) -> usize {
        self.get_indexed_points()
    }

    fn load(&mut self) -> OperationResult<bool> {
        self.load_from_db()
    }

    fn clear(self) -> OperationResult<()> {
        self.clear()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::Keyword(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();

            if let Ok(uuid) = Uuid::from_str(keyword) {
                return Some(Box::new(self.get_iterator(&uuid.as_u128()).copied()));
            }
        }

        None
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::Keyword(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();
            if let Ok(uuid) = Uuid::from_str(keyword) {
                let mut estimation = self.match_cardinality(&uuid.as_u128());
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                return Some(estimation);
            }
        }

        None
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(|value| (value, self.get_count_for_value(value).unwrap_or(0)))
                .filter(move |(_value, count)| *count >= threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key.clone(),
                        Uuid::from_u128(*value).to_string().into(),
                    ),
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
        self.clear()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        match &condition.r#match {
            Some(Match::Value(MatchValue {
                value: ValueVariants::Integer(integer),
            })) => Some(Box::new(self.get_iterator(integer).copied())),
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Keywords(keywords) => {
                    if keywords.is_empty() {
                        Some(Box::new(vec![].into_iter()))
                    } else {
                        None
                    }
                }
                AnyVariants::Integers(integers) => Some(Box::new(
                    integers
                        .iter()
                        .flat_map(|integer| self.get_iterator(integer).copied())
                        .unique(),
                )),
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => Some(self.except_set(integers)),
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
            Some(Match::Any(MatchAny { any: any_variants })) => match any_variants {
                AnyVariants::Keywords(keywords) => {
                    if keywords.is_empty() {
                        Some(
                            CardinalityEstimation::exact(0).with_primary_clause(
                                PrimaryCondition::Condition(condition.clone()),
                            ),
                        )
                    } else {
                        None
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
                    Some(
                        estimation
                            .with_primary_clause(PrimaryCondition::Condition(condition.clone())),
                    )
                }
            },
            Some(Match::Except(MatchExcept {
                except: AnyVariants::Integers(integers),
            })) => Some(self.except_cardinality(integers.iter())),
            _ => None,
        }
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.iter_values()
                .map(|value| (value, self.get_count_for_value(value).unwrap_or(0)))
                .filter(move |(_value, count)| *count >= threshold)
                .map(move |(value, count)| PayloadBlockCondition {
                    condition: FieldCondition::new_match(key.clone(), (*value).into()),
                    cardinality: count,
                }),
        )
    }
}

impl ValueIndexer for MapIndex<str> {
    type ValueType = String;

    fn add_many(&mut self, id: PointOffsetType, values: Vec<String>) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<IntPayloadType> {
    type ValueType = IntPayloadType;

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
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<UuidIntType> {
    type ValueType = UuidIntType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<Self::ValueType> {
        Some(Uuid::parse_str(value.as_str()?).ok()?.as_u128())
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;

    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;

    const FIELD_NAME: &str = "test";

    #[derive(Clone, Copy)]
    enum IndexType {
        Mutable,
        Immutable,
        Mmap,
    }

    fn save_map_index<N>(
        data: &[Vec<N::Owned>],
        path: &Path,
        index_type: IndexType,
        into_value: impl Fn(&N::Owned) -> Value,
    ) where
        N: MapIndexKey + ?Sized,
        MapIndex<N>: PayloadFieldIndex + ValueIndexer,
        <MapIndex<N> as ValueIndexer>::ValueType: Into<N::Owned>,
    {
        match index_type {
            IndexType::Mutable | IndexType::Immutable => {
                let mut builder =
                    MapIndex::<N>::builder(open_db_with_existing_cf(path).unwrap(), FIELD_NAME);
                builder.init().unwrap();
                for (idx, values) in data.iter().enumerate() {
                    let values: Vec<Value> = values.iter().map(&into_value).collect();
                    let values: Vec<_> = values.iter().collect();
                    builder.add_point(idx as PointOffsetType, &values).unwrap();
                }
                builder.finalize().unwrap();
            }
            IndexType::Mmap => {
                let mut builder = MapIndex::<N>::mmap_builder(path);
                builder.init().unwrap();
                for (idx, values) in data.iter().enumerate() {
                    let values: Vec<Value> = values.iter().map(&into_value).collect();
                    let values: Vec<_> = values.iter().collect();
                    builder.add_point(idx as PointOffsetType, &values).unwrap();
                }
                builder.finalize().unwrap();
            }
        }
    }

    fn load_map_index<N: MapIndexKey + ?Sized>(
        data: &[Vec<N::Owned>],
        path: &Path,
        index_type: IndexType,
    ) -> MapIndex<N> {
        let mut index = match index_type {
            IndexType::Mutable => {
                MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME, true)
            }
            IndexType::Immutable => {
                MapIndex::<N>::new(open_db_with_existing_cf(path).unwrap(), FIELD_NAME, false)
            }
            IndexType::Mmap => MapIndex::<N>::new_mmap(path).unwrap(),
        };
        index.load_from_db().unwrap();
        for (idx, values) in data.iter().enumerate() {
            let index_values: HashSet<N::Owned> = index
                .get_values(idx as PointOffsetType)
                .unwrap()
                .map(|v| N::to_owned(N::from_referenced(&v)))
                .collect();
            let index_values: HashSet<&N> = index_values.iter().map(|v| v.borrow()).collect();
            let check_values: HashSet<&N> = values.iter().map(|v| v.borrow()).collect();
            assert_eq!(index_values, check_values);
        }

        index
    }

    #[test]
    fn test_index_non_ascending_insertion() {
        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        let mut builder = MapIndex::<IntPayloadType>::mmap_builder(temp_dir.path());
        builder.init().unwrap();

        let data = [vec![1, 2, 3, 4, 5, 6], vec![25], vec![10, 11]];

        for (idx, values) in data.iter().enumerate().rev() {
            let values: Vec<Value> = values.iter().map(|i| (*i).into()).collect();
            let values: Vec<_> = values.iter().collect();
            builder.add_point(idx as PointOffsetType, &values).unwrap();
        }

        let index = builder.finalize().unwrap();
        for (idx, values) in data.iter().enumerate().rev() {
            let res: Vec<_> = index
                .get_values(idx as u32)
                .unwrap()
                .map(|i| i as i32)
                .collect();
            assert_eq!(res, *values);
        }
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_int_disk_map_index(#[case] index_type: IndexType) {
        let data = vec![
            vec![1, 2, 3, 4, 5, 6],
            vec![1, 2, 3, 4, 5, 6],
            vec![13, 14, 15, 16, 17, 18],
            vec![19, 20, 21, 22, 23, 24],
            vec![25],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type, |v| (*v).into());
        let index = load_map_index::<IntPayloadType>(&data, temp_dir.path(), index_type);

        // Ensure cardinality is non zero
        assert!(!index
            .except_cardinality(vec![].into_iter())
            .equals_min_exp_max(&CardinalityEstimation::exact(0)));
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_string_disk_map_index(#[case] index_type: IndexType) {
        let data = vec![
            vec![
                SmolStr::from("AABB"),
                SmolStr::from("UUFF"),
                SmolStr::from("IIBB"),
            ],
            vec![
                SmolStr::from("PPMM"),
                SmolStr::from("QQXX"),
                SmolStr::from("YYBB"),
            ],
            vec![
                SmolStr::from("FFMM"),
                SmolStr::from("IICC"),
                SmolStr::from("IIBB"),
            ],
            vec![
                SmolStr::from("AABB"),
                SmolStr::from("UUFF"),
                SmolStr::from("IIBB"),
            ],
            vec![SmolStr::from("PPGG")],
        ];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
        let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

        // Ensure cardinality is non zero
        assert!(!index
            .except_cardinality(vec![].into_iter())
            .equals_min_exp_max(&CardinalityEstimation::exact(0)));
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_empty_index(#[case] index_type: IndexType) {
        let data: Vec<Vec<SmolStr>> = vec![];

        let temp_dir = Builder::new().prefix("store_dir").tempdir().unwrap();
        save_map_index::<str>(&data, temp_dir.path(), index_type, |v| v.to_string().into());
        let index = load_map_index::<str>(&data, temp_dir.path(), index_type);

        // Ensure cardinality is zero
        assert!(index
            .except_cardinality(vec![].into_iter())
            .equals_min_exp_max(&CardinalityEstimation::exact(0)));
    }
}
