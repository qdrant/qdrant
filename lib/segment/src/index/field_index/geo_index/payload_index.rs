use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::GeoMapIndex;
use super::read_ops::{self, GeoMapIndexRead};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

impl ValueIndexer for GeoMapIndex {
    type ValueType = GeoPoint;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<GeoPoint>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.add_many_geo_points(id, &values, hw_counter),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable geo index",
            )),
            GeoMapIndex::Storage(_) => Err(OperationError::service_error(
                "Can't add values to mmap geo index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<GeoPoint> {
        match value {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return GeoPoint::new(lon, lat).ok();
                }
                None
            }
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_)
            | Value::Array(_) => None,
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.remove_point(id),
            GeoMapIndex::Immutable(index) => index.remove_point(id),
            GeoMapIndex::Storage(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }
}

impl GeoMapIndex {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            GeoMapIndexRead::get_values(self, point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl PayloadFieldIndex for GeoMapIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.wipe(),
            GeoMapIndex::Immutable(index) => index.wipe(),
            GeoMapIndex::Storage(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            GeoMapIndex::Mutable(index) => index.flusher(),
            GeoMapIndex::Immutable(index) => index.flusher(),
            GeoMapIndex::Storage(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        GeoMapIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        GeoMapIndexRead::immutable_files(self)
    }
}

impl PayloadFieldIndexRead for GeoMapIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        read_ops::filter(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        read_ops::estimate_cardinality(self, condition, hw_counter)
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        read_ops::for_each_payload_block(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
        read_ops::condition_checker(self, condition, hw_acc)
    }
}
