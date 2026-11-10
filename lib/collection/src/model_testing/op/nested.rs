//! `offers`: an array of objects, the target of `nested` filters and of the per-element payload
//! indexes (`nested: offers`) that answer them without reading payload.
//!
//! A nested filter matches a point when one element satisfies all of it, so two conditions on
//! different fields have to hold on the same element. Which path the engine takes depends on the
//! indexes left on `offers[].price` and `offers[].shop`: when every field the filter uses is
//! indexed it is the per-element path, otherwise the payload. The model is the same for both.

use rand::seq::IndexedRandom;
use rand::{Rng, RngExt};
use segment::data_types::index::{
    IntegerIndexParams, IntegerIndexType, KeywordIndexParams, KeywordIndexType,
};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, Match, Nested, NestedCondition, Payload,
    PayloadField, PayloadFieldSchema, PayloadSchemaParams, Range,
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

/// The `offers` value of a payload: 0 to 3 elements, each missing a field a fifth of the time so
/// `is_empty` has elements to match, and an empty array for a point without elements.
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
            if rng.random_bool(0.8) {
                element.insert(SHOP.to_string(), Value::from(*SHOPS.choose(rng).unwrap()));
            }
            Value::Object(element)
        })
        .collect();
    Value::Array(elements)
}

/// The two per-element indexes of `offers`, which together serve every [`NestedFilter`].
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

/// A condition on one element of `offers`.
#[derive(Debug, Clone)]
pub(super) enum ElementCondition {
    PriceEq(i64),
    PriceRange { gte: i64, lte: i64 },
    ShopEq(&'static str),
    ShopIsEmpty,
}

impl ElementCondition {
    fn random(rng: &mut impl Rng) -> Self {
        match rng.random_range(0..4) {
            0 => ElementCondition::PriceEq(rng.random_range(0..PRICE_END)),
            1 => {
                let gte = rng.random_range(0..PRICE_END);
                let lte = rng.random_range(gte..PRICE_END);
                ElementCondition::PriceRange { gte, lte }
            }
            2 => ElementCondition::ShopEq(SHOPS.choose(rng).unwrap()),
            3 => ElementCondition::ShopIsEmpty,
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
        }
    }

    fn matches(&self, element: &Value) -> bool {
        let price = element.get(PRICE).and_then(Value::as_i64);
        let shop = element.get(SHOP).and_then(Value::as_str);
        match self {
            ElementCondition::PriceEq(price_eq) => price == Some(*price_eq),
            ElementCondition::PriceRange { gte, lte } => {
                price.is_some_and(|price| (*gte..=*lte).contains(&price))
            }
            ElementCondition::ShopEq(shop_eq) => shop == Some(*shop_eq),
            // Generated elements hold a string shop or none at all
            ElementCondition::ShopIsEmpty => element.get(SHOP).is_none(),
        }
    }
}

/// A `nested` filter over `offers`. An empty list is left out of the engine filter.
#[derive(Debug, Clone)]
pub(in crate::model_testing) struct NestedFilter {
    must: Vec<ElementCondition>,
    must_not: Vec<ElementCondition>,
    should: Vec<ElementCondition>,
}

impl NestedFilter {
    /// One or two `must` conditions, a quarter of the time with a `must_not` one and a quarter
    /// with one or two `should` ones.
    pub(super) fn random(rng: &mut impl Rng) -> Self {
        let conditions =
            |rng: &mut _, len| (0..len).map(|_| ElementCondition::random(rng)).collect();
        let must_len = rng.random_range(1..=2);
        let must = conditions(rng, must_len);
        let (must_not_len, should_len) = match rng.random_range(0..4) {
            0 => (1, 0),
            1 => (0, rng.random_range(1..=2)),
            _ => (0, 0),
        };
        let must_not = conditions(rng, must_not_len);
        let should = conditions(rng, should_len);
        NestedFilter {
            must,
            must_not,
            should,
        }
    }

    fn element_matches(&self, element: &Value) -> bool {
        let NestedFilter {
            must,
            must_not,
            should,
        } = self;
        must.iter().all(|condition| condition.matches(element))
            && !must_not.iter().any(|condition| condition.matches(element))
            && (should.is_empty() || should.iter().any(|condition| condition.matches(element)))
    }
}

impl ModelFilter for NestedFilter {
    fn filter(&self) -> Filter {
        let NestedFilter {
            must,
            must_not,
            should,
        } = self;
        let conditions = |list: &[ElementCondition]| {
            (!list.is_empty()).then(|| list.iter().map(ElementCondition::condition).collect())
        };
        let element_filter = Filter {
            should: conditions(should),
            min_should: None,
            must: conditions(must),
            must_not: conditions(must_not),
        };
        Filter::new_must(Condition::Nested(NestedCondition::new(Nested {
            key: OFFERS.parse().unwrap(),
            filter: element_filter,
        })))
    }

    fn matches(&self, payload: &Payload) -> bool {
        let Some(Value::Array(elements)) = payload.0.get(OFFERS) else {
            return false;
        };
        elements.iter().any(|element| self.element_matches(element))
    }
}
