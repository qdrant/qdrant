use http::{HeaderMap, Method, Uri};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub enum Solution {
    /// A solution that can be applied immediately
    Immediate(ImmediateSolution),

    /// Two or more solutions to choose from
    ImmediateChoice(Vec<ImmediateSolution>),

    /// A solution that requires manual intervention
    Refactor(String),

    /// Failed to generate solution
    None,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct ImmediateSolution {
    pub message: String,
    pub action: Action,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct Action {
    #[serde(with = "http_serde::method")]
    #[schemars(with = "http_schemars::Method")]
    pub method: Method,

    #[serde(with = "http_serde::uri")]
    #[schemars(with = "http_schemars::Uri")]
    pub uri: Uri,

    #[serde(with = "http_serde::header_map")]
    #[schemars(with = "http_schemars::HeaderMap")]
    pub headers: HeaderMap,

    pub body: Option<serde_json::Map<String, Value>>,
}

mod http_schemars {
    use std::collections::HashMap;

    use schemars::JsonSchema;

    pub struct Method;

    impl JsonSchema for Method {
        fn schema_name() -> String {
            "Method".to_string()
        }

        fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
            let mut schema = gen.subschema_for::<String>().into_object();
            schema.metadata().description = Some("HTTP method".to_string());
            schema.into()
        }
    }

    pub struct Uri;

    impl JsonSchema for Uri {
        fn schema_name() -> String {
            "Uri".to_string()
        }

        fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
            let mut schema = gen.subschema_for::<String>().into_object();
            schema.metadata().description = Some("HTTP URI".to_string());
            schema.into()
        }
    }

    pub struct HeaderMap;

    impl JsonSchema for HeaderMap {
        fn schema_name() -> String {
            "HeaderMap".to_string()
        }

        fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
            let mut schema = gen.subschema_for::<HashMap<String, String>>().into_object();
            schema.metadata().description = Some("HTTP headers".to_string());
            schema.into()
        }
    }
}
