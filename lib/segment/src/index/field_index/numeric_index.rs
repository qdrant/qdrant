use std::cmp::Ordering::{Greater, Less};
use std::cmp::{max, min};
use std::mem;

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::index::field_index::field_index::{
    FieldIndex, PayloadFieldIndex, PayloadFieldIndexBuilder,
};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PayloadType, PointOffsetType,
    Range,
};
use itertools::Itertools;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Element<N> {
    pub id: PointOffsetType,
    pub value: N,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedNumericIndex<N: ToPrimitive + Clone> {
    /// Number of unique element ids.
    /// Each point can have several values
    points_count: usize,
    elements: Vec<Element<N>>,
}

impl<N: ToPrimitive + Clone> PersistedNumericIndex<N> {
    pub fn new() -> Self {
        Self {
            points_count: 0,
            elements: vec![],
        }
    }

    fn search_range(&self, range: &Range) -> (usize, usize) {
        let mut lower_index = 0;
        let mut upper_index = self.elements.len();

        range.gt.map(|thr| {
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
        });

        range.gte.map(|thr| {
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
        });

        range.lt.map(|thr| {
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
        });

        range.lte.map(|thr| {
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
        });
        return if lower_index > upper_index {
            (0, 0)
        } else {
            (lower_index, upper_index)
        };
    }

    pub fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let (lower_index, upper_index) = self.search_range(range);

        // ToDo: Check if there is a more precise implementation for multiple values

        let values_count: i64 = upper_index as i64 - lower_index as i64;
        let total_values = self.elements.len() as i64;
        let value_per_point = total_values as f64 / self.points_count as f64;

        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        CardinalityEstimation {
            primary_clauses: vec![],
            min: max(
                min(1, values_count),
                values_count - (total_values - self.points_count as i64),
            ) as usize,
            exp: (values_count as f64 / value_per_point) as usize,
            max: min(self.points_count as i64, values_count) as usize,
        }
    }

    fn add_many(&mut self, id: PointOffsetType, values: &Vec<N>) {
        for value in values.iter().cloned() {
            self.elements.push(Element { id, value })
        }
        self.points_count += 1
    }

    fn condition_iter(&self, range: &Range) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let (lower_index, upper_index) = self.search_range(range);
        Box::new(
            (&self.elements[lower_index..upper_index])
                .iter()
                .map(|element| element.id),
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

impl PayloadFieldIndexBuilder for PersistedNumericIndex<FloatPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType) {
        match value {
            PayloadType::Float(number) => self.add_many(id, number),
            _ => panic!("Unexpected payload type: {:?}", value),
        }
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::replace(&mut self.elements, vec![]);
        elements.sort_by_key(|el| OrderedFloat(el.value));
        FieldIndex::FloatIndex(PersistedNumericIndex {
            points_count: self.points_count,
            elements,
        })
    }
}

impl PayloadFieldIndexBuilder for PersistedNumericIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType) {
        match value {
            PayloadType::Integer(number) => self.add_many(id, number),
            _ => panic!("Unexpected payload type: {:?}", value),
        }
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::replace(&mut self.elements, vec![]);
        elements.sort_by_key(|el| el.value);
        FieldIndex::IntIndex(PersistedNumericIndex {
            points_count: self.points_count,
            elements,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_blocks() {
        let threshold = 4;
        let index = PersistedNumericIndex {
            points_count: 9,
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

    #[test]
    fn test_cardinality() {
        let index = PersistedNumericIndex {
            points_count: 9,
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
            elements: vec![Element { id: 1, value: 1 }, Element { id: 2, value: 3 }],
        };

        let json = serde_json::to_string_pretty(&index).unwrap();
        println!("{}", json)
    }
}
