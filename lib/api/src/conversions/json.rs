use std::collections::HashMap;

use segment::json_path::JsonPath;
use tonic::Status;

use crate::grpc::qdrant::value::Kind;
use crate::grpc::qdrant::{ListValue, Struct, Value};

pub fn payload_to_proto(payload: segment::types::Payload) -> HashMap<String, Value> {
    payload
        .into_iter()
        .map(|(k, v)| (k, json_to_proto(v)))
        .collect()
}

pub fn dict_to_proto(dict: HashMap<String, serde_json::Value>) -> HashMap<String, Value> {
    dict.into_iter()
        .map(|(k, v)| (k, json_to_proto(v)))
        .collect()
}

pub fn json_to_proto(json_value: serde_json::Value) -> Value {
    match json_value {
        serde_json::Value::Null => Value {
            kind: Some(Kind::NullValue(0)),
        },
        serde_json::Value::Bool(v) => Value {
            kind: Some(Kind::BoolValue(v)),
        },
        serde_json::Value::Number(n) => Value {
            kind: if let Some(int) = n.as_i64() {
                Some(Kind::IntegerValue(int))
            } else {
                Some(Kind::DoubleValue(n.as_f64().unwrap()))
            },
        },
        serde_json::Value::String(s) => Value {
            kind: Some(Kind::StringValue(s)),
        },
        serde_json::Value::Array(v) => {
            let list = v.into_iter().map(json_to_proto).collect();
            Value {
                kind: Some(Kind::ListValue(ListValue { values: list })),
            }
        }
        serde_json::Value::Object(m) => {
            let map = m.into_iter().map(|(k, v)| (k, json_to_proto(v))).collect();
            Value {
                kind: Some(Kind::StructValue(Struct { fields: map })),
            }
        }
    }
}

pub fn json_path_from_proto(a: &str) -> Result<JsonPath, Status> {
    JsonPath::try_from(a)
        .map_err(|_| Status::invalid_argument(format!("Invalid json path: \'{a}\'")))
}

pub fn proto_to_payloads(proto: HashMap<String, Value>) -> Result<segment::types::Payload, Status> {
    let mut map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for (k, v) in proto {
        map.insert(k, proto_to_json(v)?);
    }
    Ok(map.into())
}

pub fn proto_dict_to_json(
    proto: HashMap<String, Value>,
) -> Result<HashMap<String, serde_json::Value>, Status> {
    let mut map = HashMap::new();
    for (k, v) in proto {
        map.insert(k, proto_to_json(v)?);
    }
    Ok(map)
}

pub fn proto_to_json(proto: Value) -> Result<serde_json::Value, Status> {
    match proto.kind {
        None => Ok(serde_json::Value::default()),
        Some(kind) => match kind {
            Kind::NullValue(_) => Ok(serde_json::Value::Null),
            Kind::DoubleValue(n) => {
                let Some(v) = serde_json::Number::from_f64(n) else {
                    return Err(Status::invalid_argument("cannot convert to json number"));
                };
                Ok(serde_json::Value::Number(v))
            }
            Kind::IntegerValue(i) => Ok(serde_json::Value::Number(i.into())),
            Kind::StringValue(s) => Ok(serde_json::Value::String(s)),
            Kind::BoolValue(b) => Ok(serde_json::Value::Bool(b)),
            Kind::StructValue(s) => {
                let mut map = serde_json::Map::new();
                for (k, v) in s.fields {
                    map.insert(k, proto_to_json(v)?);
                }
                Ok(serde_json::Value::Object(map))
            }
            Kind::ListValue(l) => {
                let mut list = Vec::new();
                for v in l.values {
                    list.push(proto_to_json(v)?);
                }
                Ok(serde_json::Value::Array(list))
            }
        },
    }
}
