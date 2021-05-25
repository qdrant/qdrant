
use crate::types::{PointOffsetType, PayloadKeyType, PayloadType, Filter, TheMap, PayloadSchemaType};
use crate::entry::entry_point::OperationResult;
use serde_json::value::Value;

/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {

    fn assign_all_with_value(&mut self, point_id: PointOffsetType, payload: TheMap<PayloadKeyType, serde_json::value::Value>) -> OperationResult<()> {

        fn _extract_payloads<'a, I>(_payload: I, prefix_key: Option<PayloadKeyType>) -> Vec<(PayloadKeyType, Option<PayloadType>)>
            where I: Iterator<Item=(&'a PayloadKeyType, &'a serde_json::value::Value)> {

            fn _handle_array(key: PayloadKeyType, x: &Vec<Value>) -> Vec<(PayloadKeyType, Option<PayloadType>)> {
                let result = {
                    match &x.first() {
                        Some(Value::Bool(_)) => {
                            let vec = x.iter().fold(vec![], |mut data, b| {
                                match &b.as_bool() {
                                    Some(ref y) => data.push(y.to_string()),
                                    None => ()
                                }
                                data
                            });
                            vec![(key, Some(PayloadType::Keyword(vec)))]
                        },
                        Some(Value::String(_)) => {
                            let vec = x.iter().fold(vec![], |mut data, b| {
                                match &b.as_str() {
                                    Some(ref y) => data.push(y.to_string()),
                                    None => ()
                                }
                                data
                            });
                            vec![(key, Some(PayloadType::Keyword(vec)))]
                        },
                        Some(Value::Number(ref y)) => {
                            if y.is_f64() {
                                let vec = x.iter().fold(vec![], |mut data, b| {
                                    match &b.as_f64() {
                                        Some(ref y) => data.push(y.clone()),
                                        None => ()
                                    }
                                    data
                                });
                                vec![(key, Some(PayloadType::Float(vec)))]
                            } else if y.is_i64() {
                                let vec = x.iter().fold(vec![], |mut data, b| {
                                    match &b.as_i64() {
                                        Some(ref y) => data.push(y.clone()),
                                        None => ()
                                    }
                                    data
                                });
                                vec![(key, Some(PayloadType::Integer(vec)))]
                            } else {
                                vec![]
                            }
                        },
                        _ => vec![],
                    }
                };
                result
            }

            fn _fn(prefix: &Option<PayloadKeyType>, k: &PayloadKeyType, v: &Value) -> Vec<(PayloadKeyType, Option<PayloadType>)> {
                let key = match &prefix {
                    None => k.to_string(),
                    Some(_k) => (_k.to_owned() + "__" + k).to_string(),
                };
                match v {
                    Value::Bool(ref x) => vec![(key, Some(PayloadType::Keyword(vec![x.to_string()])))],
                    Value::String(ref x) => vec![(key, Some(PayloadType::Keyword(vec![x.to_string()])))],
                    Value::Number(ref x) => {
                        if x.is_f64() {
                            vec![(key, Some(PayloadType::Float(vec![x.as_f64().unwrap()])))]
                        } else if x.is_i64() {
                            vec![(key, Some(PayloadType::Integer(vec![x.as_i64().unwrap()])))]
                        } else {
                            vec![]
                        }
                    },
                    Value::Array(ref x) => _handle_array(key, x),
                    Value::Object(ref x) => _extract_payloads(x.iter(), Some(key)),
                    _ => vec![]
                }

            }
            _payload.flat_map(|(k, value)| _fn(&prefix_key, k, value)).collect()
        }
        self.drop(point_id)?;
        let inner_payloads = _extract_payloads(payload.iter(), None);
        for (key, value) in inner_payloads.iter() {
            match value {
                Some(ref v) => self.assign(point_id, key, v.to_owned())?,
                None => (),
            }
        }
        Ok(())
    }

    /// Assign same payload to each given point
    fn assign_all(&mut self, point_id: PointOffsetType, payload: TheMap<PayloadKeyType, PayloadType>) -> OperationResult<()> {
        self.drop(point_id)?;
        for (key, value) in payload {
            self.assign(point_id, &key, value)?;
        }

        Ok(())
    }

    /// Assign payload to a concrete point with a concrete payload value
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType) -> OperationResult<()>;

    /// Get payload for point
    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType>;

    /// Delete payload by key
    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType) -> OperationResult<Option<PayloadType>>;

    /// Drop all payload of the point
    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<TheMap<PayloadKeyType, PayloadType>>>;

    /// Completely drop payload. Pufff!
    fn wipe(&mut self) -> OperationResult<()>;

    /// Force persistence of current storage state.
    fn flush(&self) -> OperationResult<()>;

    /// Get payload schema, automatically generated from payload
    fn schema(&self) -> TheMap<PayloadKeyType, PayloadSchemaType>;

    /// Iterate all point ids with payload
    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_>;
}


pub trait ConditionChecker {
    /// Check if point satisfies filter condition
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}
