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
    let kind = match json_value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(v) => Kind::BoolValue(v),
        serde_json::Value::Number(n) => {
            if let Some(int) = n.as_i64() {
                Kind::IntegerValue(int)
            } else {
                Kind::DoubleValue(n.as_f64().unwrap())
            }
        }
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(v) => Kind::ListValue(ListValue {
            values: v.into_iter().map(json_to_proto).collect(),
        }),
        serde_json::Value::Object(m) => Kind::StructValue(Struct {
            fields: m.into_iter().map(|(k, v)| (k, json_to_proto(v))).collect(),
        }),
    };
    Value { kind: Some(kind) }
}

pub fn json_path_from_proto(a: &str) -> Result<JsonPath, Status> {
    JsonPath::try_from(a)
        .map_err(|_| Status::invalid_argument(format!("Invalid json path: \'{a}\'")))
}

pub fn proto_to_payloads(proto: HashMap<String, Value>) -> Result<segment::types::Payload, Status> {
    proto
        .into_iter()
        .map(|(k, v)| proto_to_json(v).map(|json| (k, json)))
        .collect::<Result<serde_json::Map<_, _>, _>>()
        .map(segment::types::Payload)
}

pub fn proto_dict_to_json(
    proto: HashMap<String, Value>,
) -> Result<HashMap<String, serde_json::Value>, Status> {
    proto
        .into_iter()
        .map(|(k, v)| proto_to_json(v).map(|json| (k, json)))
        .collect::<Result<_, _>>()
}

pub fn proto_to_json(proto: Value) -> Result<serde_json::Value, Status> {
    let Some(kind) = proto.kind else {
        return Ok(serde_json::Value::default());
    };

    let json_value = match kind {
        Kind::NullValue(_) => serde_json::Value::Null,
        Kind::DoubleValue(n) => {
            let Some(v) = serde_json::Number::from_f64(n) else {
                return Err(Status::invalid_argument("cannot convert to json number"));
            };
            serde_json::Value::Number(v)
        }
        Kind::IntegerValue(i) => serde_json::Value::Number(i.into()),
        Kind::StringValue(s) => serde_json::Value::String(s),
        Kind::BoolValue(b) => serde_json::Value::Bool(b),
        Kind::StructValue(s) => serde_json::Value::Object(
            s.fields
                .into_iter()
                .map(|(k, v)| proto_to_json(v).map(|json| (k, json)))
                .collect::<Result<_, _>>()?,
        ),
        Kind::ListValue(l) => serde_json::Value::Array(
            l.values
                .into_iter()
                .map(proto_to_json)
                .collect::<Result<_, _>>()?,
        ),
    };

    Ok(json_value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::grpc::qdrant::value::Kind;
    use crate::grpc::qdrant::{Struct, Value};

    fn gen_proto_json_dicts() -> (HashMap<String, serde_json::Value>, HashMap<String, Value>) {
        let raw_json = r#"
        {
            "f64": 1.0,
            "i64": 1,
            "string": "s",
            "bool": true,
            "struct": {"i64": 1},
            "list": [1,2],
            "null": null
        }"#;

        let values = vec![
            ("null", Kind::NullValue(0)),
            ("f64", Kind::DoubleValue(1.0)),
            ("i64", Kind::IntegerValue(1)),
            ("string", Kind::StringValue("s".to_string())),
            ("bool", Kind::BoolValue(true)),
            (
                "struct",
                Kind::StructValue(Struct {
                    fields: HashMap::from([(
                        "i64".to_string(),
                        Value {
                            kind: Some(Kind::IntegerValue(1)),
                        },
                    )]),
                }),
            ),
            (
                "list",
                Kind::ListValue(ListValue {
                    values: vec![
                        Value {
                            kind: Some(Kind::IntegerValue(1)),
                        },
                        Value {
                            kind: Some(Kind::IntegerValue(2)),
                        },
                    ],
                }),
            ),
        ];

        let json_map: HashMap<String, serde_json::Value> = serde_json::from_str(raw_json).unwrap();

        let proto_map: HashMap<String, Value> = values
            .into_iter()
            .map(|(k, v)| (k.to_string(), Value { kind: Some(v) }))
            .collect();

        (json_map, proto_map)
    }

    #[test]
    fn test_dict_to_prot() {
        let (json_map, proto_map) = gen_proto_json_dicts();
        assert_eq!(dict_to_proto(json_map), proto_map);
    }

    #[test]
    fn test_proto_dict_to_json() {
        let (mut json_map, mut proto_map) = gen_proto_json_dicts();
        proto_map.insert("unknown".to_string(), Value { kind: None });
        json_map.insert("unknown".to_string(), serde_json::Value::default());

        let got_json_map = proto_dict_to_json(proto_map);
        assert!(got_json_map.is_ok());
        assert_eq!(got_json_map.unwrap(), json_map);
    }

    #[test]
    fn test_proto_payload() {
        let (json_map, proto_map) = gen_proto_json_dicts();

        let payload: serde_json::Map<String, serde_json::Value> = json_map.into_iter().collect();
        let payload = segment::types::Payload(payload);

        // proto to payload
        let got_json_payload = proto_to_payloads(proto_map.clone());
        assert!(got_json_payload.is_ok());
        assert_eq!(got_json_payload.unwrap(), payload);

        // payload to proto
        let proto = payload_to_proto(payload);
        assert_eq!(proto, proto_map);
    }

    #[test]
    fn test_proto_to_json_invalid_struct_value() {
        let proto = Value {
            kind: Some(Kind::StructValue(Struct {
                fields: HashMap::from([
                    (
                        "int".to_string(),
                        Value {
                            kind: Some(Kind::IntegerValue(1)),
                        },
                    ),
                    (
                        "nan".to_string(),
                        Value {
                            kind: Some(Kind::DoubleValue(f64::NAN)),
                        },
                    ),
                ]),
            })),
        };

        let result = proto_to_json(proto);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_proto_to_json_invalid_list_value() {
        let proto = Value {
            kind: Some(Kind::ListValue(ListValue {
                values: vec![
                    Value {
                        kind: Some(Kind::IntegerValue(1)),
                    },
                    Value {
                        kind: Some(Kind::DoubleValue(f64::NAN)),
                    },
                ],
            })),
        };
        let result = proto_to_json(proto);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
}
