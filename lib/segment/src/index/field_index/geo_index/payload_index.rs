use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::GeoIndex;
use super::read_ops::{self, GeoIndexRead};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionChecker;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

impl ValueIndexer for GeoIndex {
    type ValueType = GeoPoint;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<GeoPoint>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            GeoIndex::Mutable(index) => index.add_many_geo_points(id, values, hw_counter),
            GeoIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable geo index",
            )),
            GeoIndex::OnDisk(_) => Err(OperationError::service_error(
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
            GeoIndex::Mutable(index) => index.remove_point(id),
            GeoIndex::Immutable(index) => index.remove_point(id),
            GeoIndex::OnDisk(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }
}

impl GeoIndex {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            GeoIndexRead::get_values(self, point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl PayloadFieldIndex for GeoIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            GeoIndex::Mutable(index) => index.wipe(),
            GeoIndex::Immutable(index) => index.wipe(),
            GeoIndex::OnDisk(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            GeoIndex::Mutable(index) => index.flusher(),
            GeoIndex::Immutable(index) => index.flusher(),
            GeoIndex::OnDisk(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        GeoIndexRead::files(self)
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        GeoIndexRead::immutable_files(self)
    }
}

impl PayloadFieldIndexRead for GeoIndex {
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
    ) -> OperationResult<Option<Box<dyn ConditionChecker + 'a>>> {
        Ok(read_ops::condition_checker(self, condition, hw_acc))
    }
}
