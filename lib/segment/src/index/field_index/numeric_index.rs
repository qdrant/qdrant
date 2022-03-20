use std::cmp::Ordering::{Greater, Less};
use std::cmp::{max, min};
use std::mem;

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, PrimaryCondition, ValueIndexer,
};
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PointOffsetType, Range,
};
use itertools::Itertools;
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Element<N> {
    pub id: PointOffsetType,
    pub value: N,
}

/// Sorting-based type of index
#[derive(Serialize, Deserialize, Default)]
pub struct PersistedNumericIndex<N: ToPrimitive + Clone> {
    /// Number of unique element ids.
    /// Each point can have several values
    points_count: usize,
    max_values_per_point: usize,
    elements: Vec<Element<N>>,
}

impl<N: ToPrimitive + Clone> PersistedNumericIndex<N> {
    fn search_range(&self, range: &Range) -> (usize, usize) {
        let mut lower_index = 0;
        let mut upper_index = self.elements.len();

        if let Some(thr) = range.gt {
            let index = self
                .elements
                .binary_search_by(|x| {
                    if x.value.to_f64().unwrap() <= thr {
                        Less
                    } else {
                        Greater
                    }
                })
                .err()
                .unwrap();
            lower_index = max(lower_index, index);
        }

        if let Some(thr) = range.gte {
            let index = self
                .elements
                .binary_search_by(|x| {
                    if x.value.to_f64().unwrap() < thr {
                        Less
                    } else {
                        Greater
                    }
                })
                .err()
                .unwrap();
            lower_index = max(lower_index, index);
        }

        if let Some(thr) = range.lt {
            let index = self
                .elements
                .binary_search_by(|x| {
                    if x.value.to_f64().unwrap() < thr {
                        Less
                    } else {
                        Greater
                    }
                })
                .err()
                .unwrap();
            upper_index = min(upper_index, index);
        };

        if let Some(thr) = range.lte {
            let index = self
                .elements
                .binary_search_by(|x| {
                    if x.value.to_f64().unwrap() <= thr {
                        Less
                    } else {
                        Greater
                    }
                })
                .err()
                .unwrap();
            upper_index = min(upper_index, index);
        }
        if lower_index > upper_index {
            (0, 0)
        } else {
            (lower_index, upper_index)
        }
    }

    pub fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let (lower_index, upper_index) = self.search_range(range);

        // ToDo: Check if there is a more precise implementation for multiple values

        let values_count: i64 = upper_index as i64 - lower_index as i64;
        let total_values = self.elements.len() as i64;

        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        let expected_min = max(
            values_count as usize / self.max_values_per_point,
            max(
                min(1, values_count),
                values_count - (total_values - self.points_count as i64),
            ) as usize,
        );
        let expected_max = min(self.points_count as i64, values_count) as usize;

        // estimate_multi_value_selection_cardinality might overflow at some corner cases
        // so it is better to limit its value with min and max
        let estimation = estimate_multi_value_selection_cardinality(
            self.points_count,
            total_values as usize,
            values_count as usize,
        )
        .round() as usize;

        CardinalityEstimation {
            primary_clauses: vec![],
            min: expected_min,
            exp: min(expected_max, max(estimation, expected_min)),
            max: expected_max,
        }
    }

    fn add_many_to_list(&mut self, id: PointOffsetType, values: impl IntoIterator<Item = N>) {
        let mut total_values = 0;
        for value in values {
            self.elements.push(Element { id, value });
            total_values += 1;
        }
        self.points_count += 1;
        self.max_values_per_point = self.max_values_per_point.max(total_values);
    }

    fn condition_iter(&self, range: &Range) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let (lower_index, upper_index) = self.search_range(range);
        Box::new(
            (&self.elements[lower_index..upper_index])
                .iter()
                .map(|element| element.id)
                .unique(),
        )
    }
}

impl<N: ToPrimitive + Clone> PayloadFieldIndex for PersistedNumericIndex<N> {
    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        condition
            .range
            .as_ref()
            .map(|range| self.condition_iter(range))
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
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // Creates half-overlapped ranges of points.
        let num_elements = self.elements.len();
        let value_per_point = num_elements as f64 / self.points_count as f64;
        let effective_threshold = (threshold as f64 * value_per_point) as usize;

        let iter = (0..num_elements)
            .step_by(effective_threshold / 2)
            .filter_map(move |init_offset| {
                let upper_index = min(num_elements - 1, init_offset + effective_threshold);

                let upper_value = self.elements[upper_index].value.to_f64();
                let lower_value = self.elements[init_offset].value.to_f64();

                if upper_value == lower_value {
                    return None; // Range blocks makes no sense within a single value
                }
                Some(Range {
                    lt: None,
                    gt: None,
                    gte: lower_value,
                    lte: upper_value,
                })
            })
            .dedup()
            .map(move |range| {
                let cardinality = self.range_cardinality(&range);

                PayloadBlockCondition {
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: None,
                        range: Some(range),
                        geo_bounding_box: None,
                        geo_radius: None,
                    },
                    cardinality: cardinality.exp,
                }
            });

        Box::new(iter)
    }
}

impl ValueIndexer<FloatPayloadType> for PersistedNumericIndex<FloatPayloadType> {
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

impl PayloadFieldIndexBuilder for PersistedNumericIndex<FloatPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::take(&mut self.elements);
        elements.sort_by_key(|el| OrderedFloat(el.value));
        FieldIndex::FloatIndex(PersistedNumericIndex {
            points_count: self.points_count,
            max_values_per_point: self.max_values_per_point,
            elements,
        })
    }
}

impl ValueIndexer<IntPayloadType> for PersistedNumericIndex<IntPayloadType> {
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

impl PayloadFieldIndexBuilder for PersistedNumericIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::take(&mut self.elements);
        elements.sort_by_key(|el| el.value);
        FieldIndex::IntIndex(PersistedNumericIndex {
            points_count: self.points_count,
            max_values_per_point: self.max_values_per_point,
            elements,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};

    #[test]
    fn test_payload_blocks() {
        let threshold = 4;
        let index = PersistedNumericIndex {
            points_count: 9,
            max_values_per_point: 1,
            elements: vec![
                Element { id: 1, value: 1.0 },
                Element { id: 2, value: 1.0 },
                Element { id: 3, value: 1.0 },
                Element { id: 4, value: 1.0 },
                Element { id: 5, value: 1.0 },
                Element { id: 6, value: 2.0 },
                Element { id: 7, value: 2.0 },
                Element { id: 8, value: 2.0 },
                Element { id: 9, value: 2.0 },
            ],
        };

        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0]
                .condition
                .range
                .expect("range condition")
                .gte
                .expect("gte"),
            1.0
        );
        assert_eq!(
            blocks[0]
                .condition
                .range
                .expect("range condition")
                .lte
                .expect("lte"),
            2.0
        );
    }

    #[test]
    fn test_bsearch() {
        let index = PersistedNumericIndex {
            points_count: 9,
            max_values_per_point: 1,
            elements: vec![
                Element { id: 1, value: 1.0 },
                Element { id: 2, value: 3.0 },
                Element { id: 3, value: 6.0 },
                Element { id: 4, value: 9.0 },
                Element { id: 5, value: 9.0 },
                Element { id: 6, value: 12.0 },
                Element { id: 7, value: 13.0 },
                Element { id: 8, value: 30.0 },
                Element { id: 9, value: 33.0 },
            ],
        };

        let res = index.search_range(&Range {
            lt: None,
            gt: None,
            gte: None,
            lte: None,
        });
        assert_eq!(res, (0, index.elements.len()));

        let res = index.search_range(&Range {
            lt: Some(15.0),
            gt: None,
            gte: Some(6.0),
            lte: None,
        });
        let elements = &index.elements[res.0..res.1];
        assert_eq!(elements[0].id, 3);
        assert_eq!(elements[elements.len() - 1].id, 7);
    }

    fn random_index(num_points: usize, values_per_point: usize) -> PersistedNumericIndex<f64> {
        let mut rng = StdRng::seed_from_u64(42);
        let mut elements: Vec<Element<f64>> = vec![];

        for i in 0..num_points {
            for _ in 0..values_per_point {
                elements.push(Element {
                    id: i as PointOffsetType,
                    value: rng.gen_range(0.0..100.0),
                });
            }
        }

        elements.sort_by_key(|x| OrderedFloat(x.value));

        PersistedNumericIndex {
            points_count: num_points,
            max_values_per_point: values_per_point,
            elements,
        }
    }

    #[test]
    fn test_cardinality_exp_small() {
        let index = random_index(1000, 2);

        let query = Range {
            lt: Some(20.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        };

        let estimation = index.range_cardinality(&query);

        let result = index
            .filter(&FieldCondition {
                key: "".to_string(),
                r#match: None,
                range: Some(query),
                geo_bounding_box: None,
                geo_radius: None,
            })
            .unwrap()
            .collect_vec();

        assert!(estimation.min <= result.len());
        assert!(estimation.max >= result.len());

        eprintln!("estimation = {:#?}", estimation);
        eprintln!("result.len() = {:#?}", result.len());
    }

    #[test]
    fn test_cardinality_exp_large() {
        let index = random_index(1000, 2);

        let query = Range {
            lt: Some(60.0),
            gt: None,
            gte: Some(10.0),
            lte: None,
        };

        let estimation = index.range_cardinality(&query);

        let result = index
            .filter(&FieldCondition {
                key: "".to_string(),
                r#match: None,
                range: Some(query),
                geo_bounding_box: None,
                geo_radius: None,
            })
            .unwrap()
            .collect_vec();

        assert!(estimation.min <= result.len());
        assert!(estimation.max >= result.len());

        eprintln!("estimation = {:#?}", estimation);
        eprintln!("result.len() = {:#?}", result.len());
    }

    #[test]
    fn test_cardinality() {
        let index = PersistedNumericIndex {
            points_count: 9,
            max_values_per_point: 1,
            elements: vec![
                Element { id: 1, value: 1.0 },
                Element { id: 2, value: 3.0 },
                Element { id: 3, value: 6.0 },
                Element { id: 4, value: 9.0 },
                Element { id: 5, value: 9.0 },
                Element { id: 6, value: 12.0 },
                Element { id: 7, value: 13.0 },
                Element { id: 8, value: 30.0 },
                Element { id: 9, value: 33.0 },
            ],
        };

        let estimation = index.range_cardinality(&Range {
            lt: Some(15.0),
            gt: None,
            gte: Some(6.0),
            lte: None,
        });
        eprintln!("estimation = {:#?}", estimation);
        assert!(estimation.min <= estimation.exp);
        assert!(estimation.exp <= estimation.max);

        let estimation = index.range_cardinality(&Range {
            lt: Some(6.0),
            gt: None,
            gte: Some(16.0),
            lte: None,
        });
        eprintln!("estimation = {:#?}", estimation);
        assert_eq!(estimation.min, 0);
        assert_eq!(estimation.exp, 0);
    }

    #[test]
    fn test_serde() {
        let index = PersistedNumericIndex {
            points_count: 9,
            max_values_per_point: 1,
            elements: vec![Element { id: 1, value: 1 }, Element { id: 2, value: 3 }],
        };

        let json = serde_json::to_string_pretty(&index).unwrap();
        println!("{}", json)
    }
}
