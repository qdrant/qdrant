//! `offers`: an array of objects, the target of `nested` filters and of the per-element payload
//! indexes (`nested: offers`) that answer them without reading payload.
//!
//! A nested filter matches a point when one element satisfies all of it, so two conditions on
//! different fields have to hold on the same element. Which path the engine takes depends on the
//! indexes left on `offers[].price` and `offers[].shop`: when every field the filter uses is
//! indexed it is the per-element path, otherwise the payload. A `values_count` condition is never
//! served per element, so a filter holding one always reads payload. The model is the same for
//! both.

use rand::seq::IndexedRandom;
use rand::{Rng, RngExt};
use segment::data_types::index::{
    IntegerIndexParams, IntegerIndexType, KeywordIndexParams, KeywordIndexType,
};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, Match, MinShould, Nested,
    NestedCondition, Payload, PayloadField, PayloadFieldSchema, PayloadSchemaParams, Range,
    ValuesCount,
};
use serde_json::{Map, Value};

use super::ModelFilter;

const OFFERS: &str = "offers";
const PRICE: &str = "price";
const SHOP: &str = "shop";
/// Prices are drawn from `0..PRICE_END`: few enough that two conditions often hold on the same
/// element.
const PRICE_END: i64 = 10;
const SHOPS: [&str; 3] = ["s1", "s2", "s3"];
/// Most `shop` values hold this many values, see [`ElementCondition::ShopValuesCount`].
const MAX_SHOP_VALUES: usize = 2;

/// The `offers` value of a payload: 0 to 3 elements, each missing `price` a fifth of the time,
/// and an empty array for a point without elements. See [`random_shop`] for `shop`.
pub(super) fn random_offers(rng: &mut impl Rng) -> Value {
    let len = rng.random_range(0..=3);
    let elements = (0..len)
        .map(|_| {
            let mut element = Map::new();
            if rng.random_bool(0.8) {
                element.insert(
                    PRICE.to_string(),
                    Value::from(rng.random_range(0..PRICE_END)),
                );
            }
            if let Some(shop) = random_shop(rng) {
                element.insert(SHOP.to_string(), shop);
            }
            Value::Object(element)
        })
        .collect();
    Value::Array(elements)
}

/// The `shop` of an element, `None` when missing: mostly one shop, otherwise every form that
/// `is_empty`, `is_null`, `values_count` and keyword matching tell apart. An array holds several
/// values for the keyword index of one element, and a number is a value the keyword index skips
/// but the null index counts.
fn random_shop(rng: &mut impl Rng) -> Option<Value> {
    let shop = |rng: &mut _| Value::from(*SHOPS.choose(rng).unwrap());
    let value = match rng.random_range(0..10) {
        0 | 1 => return None,
        2..=5 => shop(rng),
        6 => {
            let len = rng.random_range(1..=MAX_SHOP_VALUES);
            Value::Array((0..len).map(|_| shop(rng)).collect())
        }
        7 => Value::Null,
        8 => Value::Array(Vec::new()),
        9 => {
            if rng.random_bool(0.5) {
                Value::Array(vec![shop(rng), Value::Null])
            } else {
                Value::from(7)
            }
        }
        _ => unreachable!(),
    };
    Some(value)
}

/// The two per-element indexes of `offers`. Together they serve every [`NestedFilter`] without a
/// `values_count` condition.
pub(in crate::model_testing) fn nested_indexes() -> [(JsonPath, PayloadFieldSchema); 2] {
    let nested = || Some(OFFERS.parse().unwrap());
    let price = IntegerIndexParams {
        r#type: IntegerIndexType::Integer,
        lookup: Some(true),
        range: Some(true),
        nested: nested(),
        ..Default::default()
    };
    let shop = KeywordIndexParams {
        r#type: KeywordIndexType::Keyword,
        nested: nested(),
        ..Default::default()
    };
    [
        (element_path(PRICE), PayloadSchemaParams::Integer(price)),
        (element_path(SHOP), PayloadSchemaParams::Keyword(shop)),
    ]
    .map(|(field, params)| (field, PayloadFieldSchema::FieldParams(params)))
}

/// One of the two per-element indexes of `offers`.
pub(super) fn random_nested_index(rng: &mut impl Rng) -> (JsonPath, PayloadFieldSchema) {
    let [price, shop] = nested_indexes();
    if rng.random_bool(0.5) { price } else { shop }
}

/// The field path of one of the per-element indexes of `offers`.
pub(super) fn random_nested_index_field(rng: &mut impl Rng) -> JsonPath {
    element_path(if rng.random_bool(0.5) { PRICE } else { SHOP })
}

fn element_path(field: &str) -> JsonPath {
    format!("{OFFERS}[].{field}").parse().unwrap()
}

/// Whether an [`ElementFilter`] may hold a sub-filter, see [`ElementCondition::Filter`].
#[derive(Debug, Clone, Copy)]
enum SubFilters {
    Allowed,
    Forbidden,
}

/// A condition on one element of `offers`.
#[derive(Debug, Clone)]
pub(super) enum ElementCondition {
    PriceEq(i64),
    PriceRange {
        gte: i64,
        lte: i64,
    },
    ShopEq(&'static str),
    ShopIsEmpty,
    ShopIsNull,
    /// Never served per element: the whole nested filter reads payload.
    ShopValuesCount {
        gte: usize,
        lte: usize,
    },
    /// A `Condition::Filter` on the same element.
    Filter(ElementFilter),
}

impl ElementCondition {
    fn random(rng: &mut impl Rng, sub_filters: SubFilters) -> Self {
        let end = match sub_filters {
            SubFilters::Allowed => 13,
            SubFilters::Forbidden => 12,
        };
        match rng.random_range(0..end) {
            0 | 1 => ElementCondition::PriceEq(rng.random_range(0..PRICE_END)),
            2 | 3 => {
                let gte = rng.random_range(0..PRICE_END);
                let lte = rng.random_range(gte..PRICE_END);
                ElementCondition::PriceRange { gte, lte }
            }
            4..=6 => ElementCondition::ShopEq(SHOPS.choose(rng).unwrap()),
            7 | 8 => ElementCondition::ShopIsEmpty,
            9 | 10 => ElementCondition::ShopIsNull,
            11 => {
                let gte = rng.random_range(0..=MAX_SHOP_VALUES);
                let lte = rng.random_range(gte..=MAX_SHOP_VALUES);
                ElementCondition::ShopValuesCount { gte, lte }
            }
            12 => ElementCondition::Filter(ElementFilter::random(rng, SubFilters::Forbidden)),
            _ => unreachable!(),
        }
    }

    /// The engine condition, with keys relative to the element.
    fn condition(&self) -> Condition {
        let price = || PRICE.parse().unwrap();
        let shop = || SHOP.parse().unwrap();
        match self {
            ElementCondition::PriceEq(price_eq) => {
                Condition::Field(FieldCondition::new_match(price(), Match::from(*price_eq)))
            }
            ElementCondition::PriceRange { gte, lte } => {
                Condition::Field(FieldCondition::new_range(
                    price(),
                    Range {
                        lt: None,
                        gt: None,
                        gte: Some((*gte as f64).into()),
                        lte: Some((*lte as f64).into()),
                    },
                ))
            }
            ElementCondition::ShopEq(shop_eq) => Condition::Field(FieldCondition::new_match(
                shop(),
                Match::from(shop_eq.to_string()),
            )),
            ElementCondition::ShopIsEmpty => Condition::IsEmpty(IsEmptyCondition {
                is_empty: PayloadField { key: shop() },
            }),
            ElementCondition::ShopIsNull => Condition::IsNull(IsNullCondition {
                is_null: PayloadField { key: shop() },
            }),
            ElementCondition::ShopValuesCount { gte, lte } => {
                Condition::Field(FieldCondition::new_values_count(
                    shop(),
                    ValuesCount {
                        lt: None,
                        gt: None,
                        gte: Some(*gte),
                        lte: Some(*lte),
                    },
                ))
            }
            ElementCondition::Filter(filter) => Condition::Filter(filter.filter()),
        }
    }

    fn matches(&self, element: &Value) -> bool {
        let price = element.get(PRICE).and_then(Value::as_i64);
        let shop = element.get(SHOP);
        match self {
            ElementCondition::PriceEq(price_eq) => price == Some(*price_eq),
            ElementCondition::PriceRange { gte, lte } => {
                price.is_some_and(|price| (*gte..=*lte).contains(&price))
            }
            // A keyword matches a string, or any string of an array
            ElementCondition::ShopEq(shop_eq) => match shop {
                Some(Value::String(value)) => value == shop_eq,
                Some(Value::Array(values)) => values.iter().any(|value| value == shop_eq),
                None | Some(Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_)) => {
                    false
                }
            },
            // Missing, `null` and `[]` are empty, `[s, null]` is not
            ElementCondition::ShopIsEmpty => match shop {
                None | Some(Value::Null) => true,
                Some(Value::Array(values)) => values.is_empty(),
                Some(Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Object(_)) => {
                    false
                }
            },
            // `null` or an array holding one, missing is not null
            ElementCondition::ShopIsNull => match shop {
                Some(Value::Null) => true,
                Some(Value::Array(values)) => values.iter().any(Value::is_null),
                None
                | Some(Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Object(_)) => {
                    false
                }
            },
            // Missing and `null` count 0, an array its length, anything else 1
            ElementCondition::ShopValuesCount { gte, lte } => {
                let count = match shop {
                    None | Some(Value::Null) => 0,
                    Some(Value::Array(values)) => values.len(),
                    Some(
                        Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Object(_),
                    ) => 1,
                };
                (*gte..=*lte).contains(&count)
            }
            ElementCondition::Filter(filter) => filter.matches(element),
        }
    }
}

/// A filter on one element of `offers`. An empty list is left out of the engine filter.
#[derive(Debug, Clone)]
pub(super) struct ElementFilter {
    must: Vec<ElementCondition>,
    must_not: Vec<ElementCondition>,
    should: Vec<ElementCondition>,
    /// Conditions and how many of them have to match.
    min_should: Option<(Vec<ElementCondition>, usize)>,
}

impl ElementFilter {
    /// One or two `must` conditions, sometimes with a `must_not` one, one or two `should` ones, or
    /// a `min_should` over two or three.
    fn random(rng: &mut impl Rng, sub_filters: SubFilters) -> Self {
        let conditions = |rng: &mut _, len| {
            (0..len)
                .map(|_| ElementCondition::random(rng, sub_filters))
                .collect()
        };
        let must_len = rng.random_range(1..=2);
        let must = conditions(rng, must_len);
        let (must_not_len, should_len, min_should_len) = match rng.random_range(0..5) {
            0 => (1, 0, 0),
            1 => (0, rng.random_range(1..=2), 0),
            2 => (0, 0, rng.random_range(2..=3)),
            _ => (0, 0, 0),
        };
        let must_not = conditions(rng, must_not_len);
        let should = conditions(rng, should_len);
        let min_should = (min_should_len > 0).then(|| {
            let min_count = rng.random_range(1..=min_should_len);
            (conditions(rng, min_should_len), min_count)
        });
        ElementFilter {
            must,
            must_not,
            should,
            min_should,
        }
    }

    fn filter(&self) -> Filter {
        let ElementFilter {
            must,
            must_not,
            should,
            min_should,
        } = self;
        let conditions = |list: &[ElementCondition]| -> Vec<Condition> {
            list.iter().map(ElementCondition::condition).collect()
        };
        let non_empty = |list: &[ElementCondition]| (!list.is_empty()).then(|| conditions(list));
        Filter {
            should: non_empty(should),
            min_should: min_should.as_ref().map(|(list, min_count)| MinShould {
                conditions: conditions(list),
                min_count: *min_count,
            }),
            must: non_empty(must),
            must_not: non_empty(must_not),
        }
    }

    fn matches(&self, element: &Value) -> bool {
        let ElementFilter {
            must,
            must_not,
            should,
            min_should,
        } = self;
        must.iter().all(|condition| condition.matches(element))
            && !must_not.iter().any(|condition| condition.matches(element))
            && (should.is_empty() || should.iter().any(|condition| condition.matches(element)))
            && min_should.as_ref().is_none_or(|(list, min_count)| {
                list.iter()
                    .filter(|condition| condition.matches(element))
                    .count()
                    >= *min_count
            })
    }
}

/// A `nested` filter over `offers`, whose element filter may hold one level of sub-filters.
#[derive(Debug, Clone)]
pub(in crate::model_testing) struct NestedFilter(ElementFilter);

impl NestedFilter {
    pub(super) fn random(rng: &mut impl Rng) -> Self {
        NestedFilter(ElementFilter::random(rng, SubFilters::Allowed))
    }
}

impl ModelFilter for NestedFilter {
    fn filter(&self) -> Filter {
        let NestedFilter(element_filter) = self;
        Filter::new_must(Condition::Nested(NestedCondition::new(Nested {
            key: OFFERS.parse().unwrap(),
            filter: element_filter.filter(),
        })))
    }

    fn matches(&self, payload: &Payload) -> bool {
        let NestedFilter(element_filter) = self;
        let Some(Value::Array(elements)) = payload.0.get(OFFERS) else {
            return false;
        };
        elements
            .iter()
            .any(|element| element_filter.matches(element))
    }
}
