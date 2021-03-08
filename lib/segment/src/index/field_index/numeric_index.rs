use std::cmp::{max, min};
use std::cmp::Ordering::{Greater, Less};
use std::mem;

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::field_index::field_index::{FieldIndex, PayloadFieldIndex, PayloadFieldIndexBuilder};
use crate::types::{FloatPayloadType, IntPayloadType, PayloadType, PointOffsetType, Range, FieldCondition};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Element<N> {
    pub id: PointOffsetType,
    pub value: N,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedNumericIndex<N: ToPrimitive + Clone> {
    points_count: usize,
    elements: Vec<Element<N>>,
}


impl<N: ToPrimitive + Clone> PersistedNumericIndex<N> {

    pub fn new() -> Self {
        Self {
            points_count: 0,
            elements: vec![]
        }
    }

    fn search_range(&self, range: &Range) -> (usize, usize) {
        let mut lower_index = 0;
        let mut upper_index = self.elements.len();

        range.gt.map(|thr| {
            let index = self.elements.binary_search_by(|x| {
                if x.value.to_f64().unwrap() <= thr { Less } else { Greater }
            }).err().unwrap();
            lower_index = max(lower_index, index);
        });

        range.gte.map(|thr| {
            let index = self.elements.binary_search_by(|x| {
                if x.value.to_f64().unwrap() < thr { Less } else { Greater }
            }).err().unwrap();
            lower_index = max(lower_index, index);
        });

        range.lt.map(|thr| {
            let index = self.elements.binary_search_by(|x| {
                if x.value.to_f64().unwrap() < thr { Less } else { Greater }
            }).err().unwrap();
            upper_index = min(upper_index, index);
        });

        range.lte.map(|thr| {
            let index = self.elements.binary_search_by(|x| {
                if x.value.to_f64().unwrap() <= thr { Less } else { Greater }
            }).err().unwrap();
            upper_index = min(upper_index, index);
        });

        (lower_index, upper_index)
    }

    pub fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let (lower_index, upper_index) = self.search_range(range);

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
            min: max(1, values_count - (total_values - self.points_count as i64)) as usize,
            exp: (values_count as f64 / value_per_point) as usize,
            max: min(self.points_count as i64, values_count) as usize,
        }
    }

    fn add_many(&mut self, id: PointOffsetType, values: &Vec<N>) {
        for value in values.iter().cloned() {
            self.elements.push(Element { id, value })
        }
    }
}


impl<N: ToPrimitive + Clone> PayloadFieldIndex for PersistedNumericIndex<N> {
    fn filter(&self, condition: &FieldCondition) -> Option<Box<dyn Iterator<Item=PointOffsetType>>> {
        unimplemented!()
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        condition.range
            .as_ref()
            .map(|range| {
                let mut cardinality = self.range_cardinality(range);
                cardinality.primary_clauses.push(PrimaryCondition::Condition(condition.clone()));
                cardinality
            })
    }
}

impl PayloadFieldIndexBuilder for PersistedNumericIndex<FloatPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType) {
        match value {
            PayloadType::Float(number) => self.add_many(id, number),
            _ => panic!("Unexpected payload type: {:?}", value)
        }
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::replace(&mut self.elements, vec![]);
        elements.sort_by_key(|el| OrderedFloat(el.value));
        FieldIndex::FloatIndex(PersistedNumericIndex {
            points_count: elements.len(),
            elements
        })
    }
}

impl PayloadFieldIndexBuilder for PersistedNumericIndex<IntPayloadType> {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType) {
        match value {
            PayloadType::Integer(number) => self.add_many(id, number),
            _ => panic!("Unexpected payload type: {:?}", value)
        }
    }

    fn build(&mut self) -> FieldIndex {
        let mut elements = mem::replace(&mut self.elements, vec![]);
        elements.sort_by_key(|el| el.value);
        FieldIndex::IntIndex(PersistedNumericIndex {
            points_count: elements.len(),
            elements
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_serde() {
        let index = PersistedNumericIndex {
            points_count: 9,
            elements: vec![
                Element { id: 1, value: 1 },
                Element { id: 2, value: 3 }
            ],
        };

        let json = serde_json::to_string_pretty(&index).unwrap();
        println!("{}", json)
    }
}