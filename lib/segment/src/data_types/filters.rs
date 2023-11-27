use crate::types::{Condition, Filter, PayloadKeyType};

impl Condition {
    fn _join_key(nested_prefix: &str, key: &PayloadKeyType) -> PayloadKeyType {
        if nested_prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", nested_prefix, key)
        }
    }

    fn _get_payload_fields(&self, fields: &mut Vec<PayloadKeyType>, nested_prefix: &str) {
        match self {
            Condition::Field(condition) => {
                fields.push(Self::_join_key(nested_prefix, &condition.key))
            }
            Condition::IsEmpty(condition) => {
                fields.push(Self::_join_key(nested_prefix, &condition.is_empty.key))
            }
            Condition::IsNull(condition) => {
                fields.push(Self::_join_key(nested_prefix, &condition.is_null.key))
            }
            Condition::HasId(_) => {}
            Condition::Nested(condition) => {
                condition.nested.filter._get_payload_fields(
                    fields,
                    &Self::_join_key(nested_prefix, &condition.nested.key),
                );
            }
            Condition::Filter(filter) => filter._get_payload_fields(fields, nested_prefix),
        }
    }
}

impl Filter {
    fn _get_payload_fields(&self, fields: &mut Vec<PayloadKeyType>, nested_prefix: &str) {
        if let Some(ref condition) = self.must {
            for cond in condition {
                cond._get_payload_fields(fields, nested_prefix);
            }
        }

        if let Some(ref condition) = self.must_not {
            for cond in condition {
                cond._get_payload_fields(fields, nested_prefix);
            }
        }

        if let Some(ref condition) = self.should {
            for cond in condition {
                cond._get_payload_fields(fields, nested_prefix);
            }
        }
    }

    /// Walk through filter and return all payload fields used in it
    pub fn get_payload_fields(&self) -> Vec<PayloadKeyType> {
        let mut fields = Vec::new();
        self._get_payload_fields(&mut fields, "");
        fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldCondition, Nested, NestedCondition};

    #[test]
    fn test_get_payload_fields() {
        let nester2_filter = Filter {
            must: Some(vec![
                Condition::Field(FieldCondition::new_match(
                    "field3".to_owned(),
                    "value3".to_owned().into(),
                )),
                Condition::Field(FieldCondition::new_match(
                    "field4".to_owned(),
                    "value4".to_owned().into(),
                )),
            ]),
            ..Default::default()
        };

        let nester_filter = Filter {
            should: Some(vec![
                Condition::Field(FieldCondition::new_match(
                    "field1".to_owned(),
                    "value1".to_owned().into(),
                )),
                Condition::Nested(NestedCondition {
                    nested: Nested {
                        key: "nested2".to_string(),
                        filter: nester2_filter,
                    },
                }),
            ]),
            ..Default::default()
        };

        let filter = Filter {
            must: Some(vec![
                Condition::Field(FieldCondition::new_match(
                    "hello".to_owned(),
                    "world".to_owned().into(),
                )),
                Condition::Nested(NestedCondition {
                    nested: Nested {
                        key: "nested1".to_string(),
                        filter: nester_filter,
                    },
                }),
            ]),
            ..Default::default()
        };

        let fields = filter.get_payload_fields();
        assert_eq!(fields.len(), 4);
        assert!(fields.contains(&"hello".to_owned()));
        assert!(fields.contains(&"nested1.field1".to_owned()));
        assert!(fields.contains(&"nested1.nested2.field3".to_owned()));
        assert!(fields.contains(&"nested1.nested2.field4".to_owned()));
    }
}
