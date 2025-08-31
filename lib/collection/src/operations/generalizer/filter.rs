use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, GeoBoundingBox, GeoPolygon, GeoRadius,
    HasIdCondition, Match, MatchAny, MatchExcept, MatchPhrase, MatchText, MatchTextAny, MatchValue,
    MinShould, Nested, NestedCondition, RangeInterface, ValueVariants, ValuesCount,
};
use serde_json::{Value, json};

use crate::operations::generalizer::placeholders::{
    float_value_placeholder, size_value_placeholder, text_placeholder, values_array_placeholder,
};
use crate::operations::generalizer::{GeneralizationLevel, Generalizer};

impl Generalizer for Filter {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep filter as-is
                // Safely unwrap as Filter should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => {
                let Filter {
                    should,
                    min_should,
                    must,
                    must_not,
                } = self;

                let mut result = json!({});

                if let Some(should) = should {
                    let generalized_should: Vec<_> =
                        should.iter().map(|cond| cond.generalize(level)).collect();
                    result["should"] = Value::Array(generalized_should);
                }

                if let Some(min_should) = min_should {
                    let MinShould {
                        conditions,
                        min_count,
                    } = min_should;

                    let generalized_conditions: Vec<_> = conditions
                        .iter()
                        .map(|cond| cond.generalize(level))
                        .collect();

                    result["min_should"] = json!({
                        "conditions": generalized_conditions,
                        "min_count": min_count,
                    });
                }

                if let Some(must) = must {
                    let generalized_must: Vec<_> =
                        must.iter().map(|cond| cond.generalize(level)).collect();
                    result["must"] = Value::Array(generalized_must);
                }

                if let Some(must_not) = must_not {
                    let generalized_must_not: Vec<_> =
                        must_not.iter().map(|cond| cond.generalize(level)).collect();
                    result["must_not"] = Value::Array(generalized_must_not);
                }

                result
            }
        }
    }
}

impl Generalizer for Condition {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep condition as-is
                // Safely unwrap as Filter should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => match self {
                Condition::Field(field) => field.generalize(level),
                Condition::IsEmpty(is_empty) => serde_json::to_value(is_empty).unwrap_or_default(),
                Condition::IsNull(is_null) => serde_json::to_value(is_null).unwrap_or_default(),
                Condition::HasId(has_id) => {
                    let HasIdCondition { has_id } = has_id;
                    let values = values_array_placeholder(has_id.len(), true);
                    json!({
                        "has_id": values
                    })
                }
                Condition::HasVector(has_vector) => {
                    serde_json::to_value(has_vector).unwrap_or_default()
                }
                Condition::Nested(nested) => nested.generalize(level),
                Condition::Filter(filter) => filter.generalize(level),
                Condition::CustomIdChecker(_) => json!({"type": "custom_id_checker"}),
            },
        }
    }
}

impl Generalizer for NestedCondition {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let NestedCondition { nested } = self;
        json!({
            "nested": nested.generalize(level)
        })
    }
}

impl Generalizer for Nested {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let Nested { key, filter } = self;
        json!({
            "key": key,
            "filter": filter.generalize(level)
        })
    }
}

impl Generalizer for FieldCondition {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        let FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count,
            is_empty,
            is_null,
        } = self;

        let mut result = json!({ "key": key });

        if let Some(r#match) = r#match {
            result["match"] = r#match.generalize(level);
        }

        if let Some(range) = range {
            result["range"] = range.generalize(level);
        }

        if let Some(geo_bounding_box) = geo_bounding_box {
            result["geo_bounding_box"] = geo_bounding_box.generalize(level);
        }

        if let Some(geo_radius) = geo_radius {
            result["geo_radius"] = geo_radius.generalize(level);
        }

        if let Some(geo_polygon) = geo_polygon {
            result["geo_polygon"] = geo_polygon.generalize(level);
        }

        if let Some(values_count) = values_count {
            result["values_count"] = values_count.generalize(level);
        }

        if let Some(is_empty) = is_empty {
            result["is_empty"] = json!(is_empty);
        }

        if let Some(is_null) = is_null {
            result["is_null"] = json!(is_null);
        }

        result
    }
}

impl Generalizer for Match {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep match as-is
                // Safely unwrap as Match should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => match self {
                Match::Value(value) => {
                    let MatchValue { value } = value;
                    match value {
                        ValueVariants::String(_) => json!({
                            "value": "string"
                        }),
                        ValueVariants::Integer(_) => json!({
                            "value": "integer"
                        }),
                        ValueVariants::Bool(_) => json!({
                            "value": "bool"
                        }),
                    }
                }
                Match::Text(text) => {
                    let MatchText { text } = text;
                    json!({
                        "text": text_placeholder(text, true)
                    })
                }
                Match::TextAny(text) => {
                    let MatchTextAny { text_any } = text;
                    json!({
                        "text_any": text_placeholder(text_any, true)
                    })
                }
                Match::Phrase(phrase) => {
                    let MatchPhrase { phrase } = phrase;
                    json!({
                        "phrase": text_placeholder(phrase, true)
                    })
                }
                Match::Any(match_any) => {
                    let MatchAny { any } = match_any;
                    match any {
                        AnyVariants::Strings(strings) => json!({
                            "any_strings": values_array_placeholder(strings.len(), true)
                        }),
                        AnyVariants::Integers(ints) => json!({
                            "any_integers": values_array_placeholder(ints.len(), true)
                        }),
                    }
                }
                Match::Except(except) => {
                    let MatchExcept { except } = except;
                    match except {
                        AnyVariants::Strings(strings) => json!({
                            "except_strings": values_array_placeholder(strings.len(), true)
                        }),
                        AnyVariants::Integers(ints) => json!({
                            "except_integers": values_array_placeholder(ints.len(), true)
                        }),
                    }
                }
            },
        }
    }
}

impl Generalizer for RangeInterface {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep range as-is
                // Safely unwrap as Range should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => match self {
                RangeInterface::Float(_) => json!({
                    "range": "float"
                }),
                RangeInterface::DateTime(_) => json!({
                    "range": "datetime"
                }),
            },
        }
    }
}

impl Generalizer for GeoBoundingBox {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep geo bounding box as-is
                // Safely unwrap as GeoBoundingBox should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => {
                json!({
                    "type": "geo_bounding_box"
                })
            }
        }
    }
}

impl Generalizer for GeoRadius {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep geo radius as-is
                // Safely unwrap as GeoRadius should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => {
                let GeoRadius { center: _, radius } = self;
                json!({
                    "type": "geo_radius",
                    "radius": float_value_placeholder(*radius),
                })
            }
        }
    }
}

impl Generalizer for GeoPolygon {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep geo polygon as-is
                // Safely unwrap as GeoPolygon should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => {
                let GeoPolygon {
                    exterior: _,
                    interiors,
                } = self;
                let num_interiors = interiors.as_ref().map(|ints| ints.len()).unwrap_or(0);
                json!({
                    "type": "geo_polygon",
                    "num_interiors": values_array_placeholder(num_interiors, true),
                })
            }
        }
    }
}

impl Generalizer for ValuesCount {
    fn generalize(&self, level: GeneralizationLevel) -> Value {
        match level {
            GeneralizationLevel::OnlyVector => {
                // Keep values count as-is
                // Safely unwrap as ValuesCount should always be serializable
                serde_json::to_value(self).unwrap_or_default()
            }
            GeneralizationLevel::VectorAndValues => {
                let ValuesCount { lt, gt, gte, lte } = self;
                json!({
                    "type": "values_count",
                    "lt": lt.map(size_value_placeholder),
                    "gt": gt.map(size_value_placeholder),
                    "gte": gte.map(size_value_placeholder),
                    "lte": lte.map(size_value_placeholder),
                })
            }
        }
    }
}
