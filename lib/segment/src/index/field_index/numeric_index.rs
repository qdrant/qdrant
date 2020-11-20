use crate::types::{FloatPayloadType, Range, IntPayloadType};
use crate::index::field_index::index_builder::{Element, IndexBuilder};
use ordered_float::OrderedFloat;
use crate::index::field_index::Estimation;
use std::cmp::{min, max};
use std::cmp::Ordering::{Greater, Less};
use serde::{Deserialize, Serialize};
use num_traits::ToPrimitive;

impl Into<PersistedNumericIndex<FloatPayloadType>> for IndexBuilder<FloatPayloadType> {
    fn into(mut self) -> PersistedNumericIndex<FloatPayloadType> {
        self.elements.sort_by_key(|el| OrderedFloat(el.value));
        PersistedNumericIndex {
            points_count: self.ids.len(),
            elements: self.elements,
        }
    }
}

impl Into<PersistedNumericIndex<IntPayloadType>> for IndexBuilder<IntPayloadType> {
    fn into(mut self) -> PersistedNumericIndex<IntPayloadType> {
        self.elements.sort_by_key(|el| el.value);
        PersistedNumericIndex {
            points_count: self.ids.len(),
            elements: self.elements,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct PersistedNumericIndex<N: ToPrimitive> {
    points_count: usize,
    elements: Vec<Element<N>>,
}


impl<N: ToPrimitive> PersistedNumericIndex<N> {
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

    pub fn range_cardinality(&self, range: &Range) -> Estimation {
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
        Estimation {
            min: max(1, values_count - (total_values - self.points_count as i64)) as usize,
            exp: (values_count as f64 / value_per_point) as usize,
            max: min(self.points_count as i64, values_count) as usize,
        }
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
            key: "".to_string(),
            lt: None,
            gt: None,
            gte: None,
            lte: None,
        });
        assert_eq!(res, (0, index.elements.len()));

        let res = index.search_range(&Range {
            key: "".to_string(),
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
            key: "".to_string(),
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