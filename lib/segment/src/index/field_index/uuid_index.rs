use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use delegate::delegate;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use super::numeric_index::NumericIndexInner;
use super::{
    CardinalityEstimation, FieldIndexBuilderTrait, PayloadBlockCondition, PayloadFieldIndex,
    ValueIndexer,
};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::index::field_index::histogram::Numericable;
use crate::index::field_index::PrimaryCondition;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, Match, MatchValue, PayloadKeyType, UuidPayloadKeyType, UuidPayloadType,
    ValueVariants,
};

pub struct UuidPayloadIndex {
    index: NumericIndexInner<UuidPayloadKeyType>,
}

impl UuidPayloadIndex {
    pub fn new(db: Arc<RwLock<DB>>, field: &str, is_appendable: bool) -> Self {
        let index = NumericIndexInner::new(db, field, is_appendable);
        Self { index }
    }

    pub fn builder(db: Arc<RwLock<DB>>, field: &str) -> UuidPayloadIndexBuilder {
        let index = NumericIndexInner::new(db, field, true);
        UuidPayloadIndexBuilder(Self { index })
    }

    delegate! {
        to self.index{
            pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&UuidPayloadKeyType) -> bool) -> bool;
            pub fn clear(self) -> OperationResult<()>;
            pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry;
            pub fn load(&mut self) -> OperationResult<bool>;
            pub fn values_count(&self, idx: PointOffsetType) -> usize;
            pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = UuidPayloadKeyType> + '_>>;
            pub fn values_is_empty(&self, idx: PointOffsetType) -> bool;
        }
    }
}

impl PayloadFieldIndex for UuidPayloadIndex {
    delegate! {
        to self.index{
            fn count_indexed_points(&self) -> usize;

            fn load(&mut self) -> OperationResult<bool>;

            fn clear(self) -> OperationResult<()>;

            fn flusher(&self) -> Flusher;

            fn payload_blocks(
                &self,
                threshold: usize,
                key: PayloadKeyType,
            ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_>;
        }
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::Keyword(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();

            if let Ok(uuid) = UuidPayloadType::from_str(keyword) {
                let key = UuidPayloadKeyType::from_i128(uuid.to_u128_le() as UuidPayloadKeyType);
                return match &self.index {
                    NumericIndexInner::Mutable(mutable) => {
                        Ok(Box::new(mutable.values_by_key(&key)))
                    }
                    NumericIndexInner::Immutable(immutable) => {
                        Ok(Box::new(immutable.values_by_key(&key)))
                    }
                };
            }
        }

        Err(OperationError::service_error(
            "Range not yet supported for Uuid Index",
        ))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        if let Some(Match::Value(MatchValue {
            value: ValueVariants::Keyword(keyword),
        })) = &condition.r#match
        {
            let keyword = keyword.as_str();
            if let Ok(uuid) = UuidPayloadType::from_str(keyword) {
                let key = UuidPayloadKeyType::from_i128(uuid.to_u128_le() as UuidPayloadKeyType);

                let contains = match &self.index {
                    NumericIndexInner::Mutable(mutable) => mutable.contains_key(&key),
                    NumericIndexInner::Immutable(immutable) => immutable.contains_key(&key),
                };

                if contains {
                    let estimated_count = match &self.index {
                        NumericIndexInner::Mutable(index) => index.estimate_points(&key),
                        NumericIndexInner::Immutable(index) => index.estimate_points(&key),
                    };

                    return Ok(CardinalityEstimation::exact(estimated_count)
                        .with_primary_clause(PrimaryCondition::Condition(condition.clone())));
                }
            }
        }

        Err(OperationError::service_error(
            "Range not yet supported for Uuid Index",
        ))
    }
}

pub struct UuidPayloadIndexBuilder(pub(crate) UuidPayloadIndex);

impl FieldIndexBuilderTrait for UuidPayloadIndexBuilder {
    type FieldIndexType = UuidPayloadIndex;

    fn init(&mut self) -> OperationResult<()> {
        self.0.index.get_db_wrapper().recreate_column_family()
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

impl ValueIndexer for UuidPayloadIndex {
    type ValueType = UuidPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
    ) -> OperationResult<()> {
        match &mut self.index {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(
                id,
                values
                    .into_iter()
                    .map(|i| i.to_u128_le() as UuidPayloadKeyType),
            ),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
        }
    }

    fn get_value(&self, value: &Value) -> Option<Self::ValueType> {
        let value = value.as_str()?;
        UuidPayloadType::from_str(value).ok()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.index.remove_point(id)
    }
}
