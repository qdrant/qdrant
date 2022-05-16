use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Bound::{Excluded, Included, Unbounded};

use serde_json::Value;

use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, ValueIndexer,
};
use crate::index::key_encoding::{encode_f64_key_ascending, encode_i64_key_ascending};
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PointOffsetType, Range,
};

trait KeyEncoder {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;
}

impl KeyEncoder for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }
}

impl KeyEncoder for FloatPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_f64_key_ascending(*self, id)
    }
}

#[derive(Default)]
struct NumericIndex<T: KeyEncoder> {
    map: BTreeMap<Vec<u8>, u32>,
    phantom: PhantomData<T>,
}

impl<T: KeyEncoder> NumericIndex<T> {
    pub fn add_value(&mut self, id: PointOffsetType, value: T) {
        let key = value.encode_key(id);
        self.map.insert(key, id);
    }

    fn add_many_to_list(&mut self, idx: PointOffsetType, values: impl IntoIterator<Item = T>) {
        for value in values {
            self.add_value(idx, value);
        }
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

    fn estimate_cardinality(&self, _condition: &FieldCondition) -> Option<CardinalityEstimation> {
        //TODO(gvelo): add histogram.
        todo!()
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
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_numeric_index() {
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

        let mut index: NumericIndex<_> = Default::default();

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
