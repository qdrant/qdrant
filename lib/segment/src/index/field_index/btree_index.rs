use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use rocksdb::DB;
use serde_json::Value;

use crate::common::rocksdb_operations::db_write_options;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::histogram::{Histogram, HistogramValue, Point};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, PrimaryCondition, ValueIndexer,
};
use crate::index::key_encoding::{
    decode_f64_ascending, decode_i64_ascending, encode_f64_key_ascending, encode_i64_key_ascending,
    F64_KEY_LEN, I64_KEY_LEN,
};
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PointOffsetType, Range,
};

const HISTOGRAM_BUCKET_SIZE : usize = 64;

trait KeyEncoder: HistogramValue {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;

    fn decode_key(v: &[u8]) -> Self;
}

impl KeyEncoder for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }

    fn decode_key(v: &[u8]) -> IntPayloadType {
        decode_i64_ascending(&v[0..I64_KEY_LEN])
    }
}

impl KeyEncoder for FloatPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_f64_key_ascending(*self, id)
    }

    fn decode_key(v: &[u8]) -> FloatPayloadType {
        decode_f64_ascending(&v[0..F64_KEY_LEN])
    }
}

struct NumericIndex<T: KeyEncoder> {
    map: BTreeMap<Vec<u8>, PointOffsetType>,
    db: Arc<AtomicRefCell<DB>>,
    store_cf_name: String,
    histogram: Histogram<T>,
}

impl<T: KeyEncoder> NumericIndex<T> {
    #[allow(dead_code)] // TODO(gvelo): remove when this index is integrated in `StructPayloadIndex`
    pub fn new(db: Arc<AtomicRefCell<DB>>, store_cf_name: String) -> Self {
        Self {
            map: BTreeMap::new(),
            db,
            store_cf_name,
            histogram: Histogram::new(HISTOGRAM_BUCKET_SIZE),
        }
    }

    pub fn add_value(&mut self, id: PointOffsetType, value: T) {
        let key = value.encode_key(id);
        let db_ref = self.db.borrow();
        //TODO(gvelo): currently all the methods on the ValueIndexer trait are infallible so we
        // cannot propagate error from here. We need to remove the unwrap() and handle the
        // cf_handle() result when ValueIndexer is refactored to return OperationResult<>
        let cf_handle = db_ref.cf_handle(&self.store_cf_name).unwrap();
        db_ref
            .put_cf_opt(cf_handle, &key, id.to_be_bytes(), &db_write_options())
            .unwrap();
        Self::add_to_map(&mut self.map, &mut self.histogram, key, id);
    }

    fn add_many_to_list(&mut self, idx: PointOffsetType, values: impl IntoIterator<Item = T>) {
        for value in values {
            self.add_value(idx, value);
        }
    }

    #[allow(dead_code)] // TODO(gvelo): remove when this index is integrated in `StructPayloadIndex`
    pub fn load(&mut self) -> OperationResult<()> {
        let db_ref = self.db.borrow();
        let cf_handle = db_ref.cf_handle(&self.store_cf_name).ok_or_else(|| {
            OperationError::service_error(&format!(
                "Index flush error: column family {} not found",
                self.store_cf_name
            ))
        })?;
        let mut iter = db_ref.raw_iterator_cf(&cf_handle);
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key().unwrap().to_owned();
            let value = PointOffsetType::from_be_bytes(
                iter.value()
                    .unwrap()
                    .try_into()
                    .expect("key with incorrect length"),
            );
            Self::add_to_map(&mut self.map, &mut self.histogram, key, value);
            iter.next();
        }
        Ok(())
    }

    #[allow(dead_code)] // TODO(gvelo): remove when this index is integrated in `StructPayloadIndex`
    pub fn flush(&self) -> OperationResult<()> {
        let db_ref = self.db.borrow();
        let cf_handle = db_ref.cf_handle(&self.store_cf_name).ok_or_else(|| {
            OperationError::service_error(&format!(
                "Index flush error: column family {} not found",
                self.store_cf_name
            ))
        })?;
        Ok(db_ref.flush_cf(cf_handle)?)
    }

    fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let expected_min: usize = 0;
        let expected_max: usize = self.map.len();

        let lbound = if let Some(lte) = range.lte {
            Included(T::from_f64(lte))
        } else if let Some(lt) = range.lt {
            Excluded(T::from_f64(lt))
        } else {
            Unbounded
        };

        let gbound = if let Some(gte) = range.gte {
            Included(T::from_f64(gte))
        } else if let Some(gt) = range.gt {
            Excluded(T::from_f64(gt))
        } else {
            Unbounded
        };

        let estimation = self.histogram.estimate(gbound, lbound);

        CardinalityEstimation {
            primary_clauses: vec![],
            min: expected_min,
            exp: min(expected_max, max(estimation, expected_min)),
            max: expected_max,
        }
    }

    fn add_to_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram<T>,
        key: Vec<u8>,
        id: PointOffsetType,
    ) {
        let to_histogram_point = |key, id: PointOffsetType| Point::<T> {
            val: T::decode_key(key),
            idx: id as usize,
        };
        map.insert(key.clone(), id);
        histogram.insert(
            to_histogram_point(&key, id),
            |x| {
                let key = x.val.encode_key(x.idx as PointOffsetType);
                map.range((Unbounded, Excluded(key)))
                    .next_back()
                    .map(|(key, id)| to_histogram_point(key, *id));
                None
            },
            |x| {
                let key = x.val.encode_key(x.idx as PointOffsetType);
                map.range((Excluded(key), Unbounded))
                    .next()
                    .map(|(key, id)| to_histogram_point(key, *id));
                None
            },
        );
    }
}

impl<T: KeyEncoder + From<f64>> PayloadFieldIndex for NumericIndex<T> {
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let cond_range = condition.range.as_ref()?;

        let start_bound = match cond_range {
            Range { gt: Some(gt), .. } => {
                let v: T = gt.to_owned().into();
                Excluded(v.encode_key(u32::MAX))
            }
            Range { gte: Some(gte), .. } => {
                let v: T = gte.to_owned().into();
                Included(v.encode_key(u32::MIN))
            }
            _ => Unbounded,
        };

        let end_bound = match cond_range {
            Range { lt: Some(lt), .. } => {
                let v: T = lt.to_owned().into();
                Excluded(v.encode_key(u32::MIN))
            }
            Range { lte: Some(lte), .. } => {
                let v: T = lte.to_owned().into();
                Included(v.encode_key(u32::MAX))
            }
            _ => Unbounded,
        };

        Some(Box::new(
            self.map.range((start_bound, end_bound)).map(|(_, v)| *v),
        ))
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        condition.range.as_ref().map(|range| {
            let mut cardinality = self.range_cardinality(range);
            cardinality
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            cardinality
        })
    }

    fn payload_blocks(
        &self,
        _threshold: usize,
        _key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        todo!()
    }

    fn count_indexed_points(&self) -> usize {
        self.map.len()
    }
}

impl PayloadFieldIndexBuilder for NumericIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value);
    }

    fn build(&mut self) -> FieldIndex {
        //TODO(gvelo): handle this properly
        panic!("cannot build");
    }
}

impl PayloadFieldIndexBuilder for NumericIndex<FloatPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value);
    }

    fn build(&mut self) -> FieldIndex {
        //TODO(gvelo): handle this properly
        panic!("cannot build");
    }
}

impl ValueIndexer<IntPayloadType> for NumericIndex<IntPayloadType> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<IntPayloadType>) {
        self.add_many_to_list(id, values)
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }
}

impl ValueIndexer<FloatPayloadType> for NumericIndex<FloatPayloadType> {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<FloatPayloadType>) {
        self.add_many_to_list(id, values)
    }

    fn get_value(&self, value: &Value) -> Option<FloatPayloadType> {
        if let Value::Number(num) = value {
            return num.as_f64();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::common::rocksdb_operations::db_options;
    use itertools::Itertools;
    use tempdir::TempDir;

    use super::*;

    const CF_NAME: &str = "test_cf";

    #[test]
    fn test_numeric_index_load_from_disk() {
        let tmp_dir = TempDir::new("test_numeric_index_load").unwrap();
        let file_path = tmp_dir.path().join("index");
        let mut db = DB::open_default(file_path).unwrap();
        db.create_cf(CF_NAME, &db_options()).unwrap();
        let db_ref = Arc::new(AtomicRefCell::new(db));
        let mut index: NumericIndex<_> = NumericIndex::new(db_ref.clone(), CF_NAME.to_string());

        let values = vec![
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![2.0],
            vec![2.5],
            vec![2.6],
            vec![3.0],
        ];

        values
            .into_iter()
            .enumerate()
            .for_each(|(idx, values)| index.add_many_to_list(idx as PointOffsetType + 1, values));

        index.flush().unwrap();

        let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, CF_NAME.to_string());
        new_index.load().unwrap();

        test_cond(
            &new_index,
            Range {
                gt: None,
                gte: None,
                lt: None,
                lte: Some(2.6),
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        );
    }

    #[test]
    fn test_numeric_index() {
        let tmp_dir = TempDir::new("test_numeric_index").unwrap();
        let file_path = tmp_dir.path().join("index");
        let mut db = DB::open_default(file_path).unwrap();
        db.create_cf(CF_NAME, &db_options()).unwrap();
        let db_ref = Arc::new(AtomicRefCell::new(db));

        let values = vec![
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![2.0],
            vec![2.5],
            vec![2.6],
            vec![3.0],
        ];

        let mut index: NumericIndex<_> = NumericIndex::new(db_ref, CF_NAME.to_string());

        values
            .into_iter()
            .enumerate()
            .for_each(|(idx, values)| index.add_many_to_list(idx as PointOffsetType + 1, values));

        test_cond(
            &index,
            Range {
                gt: Some(1.0),
                gte: None,
                lt: None,
                lte: None,
            },
            vec![6, 7, 8, 9],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: Some(1.0),
                lt: None,
                lte: None,
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: None,
                lt: Some(2.6),
                lte: None,
            },
            vec![1, 2, 3, 4, 5, 6, 7],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: None,
                lt: None,
                lte: Some(2.6),
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: Some(2.0),
                lt: None,
                lte: Some(2.6),
            },
            vec![6, 7, 8],
        );
    }

    fn test_cond<T: KeyEncoder + From<f64>>(index: &NumericIndex<T>, rng: Range, result: Vec<u32>) {
        let condition = FieldCondition {
            key: "".to_string(),
            r#match: None,
            range: Some(rng),
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
        };

        let offsets = index.filter(&condition).unwrap().collect_vec();

        assert_eq!(offsets, result);
    }
}
