use pyo3::inspect::PyStaticExpr;
use pyo3::type_hint_identifier;

/// A type alias in the generated `.pyi` stub.
pub struct Alias {
    pub name: &'static str,
    pub definition: PyStaticExpr,
}

impl Alias {
    pub const fn hint(&self) -> PyStaticExpr {
        PyStaticExpr::Name { id: self.name }
    }
}

pub const ANY: PyStaticExpr = type_hint_identifier!("typing", "Any");

pub const ALIASES: &[&Alias] = &[
    &crate::config::quantization::QUANTIZATION_CONFIG,
    &crate::config::vector_data::INDEXES,
    &crate::query::SCORING_QUERY,
    &crate::query::START_FROM,
    &crate::types::filter::condition::CONDITION,
    &crate::types::filter::geo::GEO_LINE_STRING,
    &crate::types::filter::r#match::MATCH,
    &crate::types::json_path::JSON_PATH,
    &crate::types::payload::PAYLOAD,
    &crate::types::payload_schema::PAYLOAD_FIELD_SCHEMA,
    &crate::types::payload_schema::PAYLOAD_SCHEMA_PARAMS,
    &crate::types::payload_schema::text_index::STEMMING_ALGORITHM,
    &crate::types::payload_schema::text_index::STOPWORDS,
    &crate::types::point_id::POINT_ID,
    &crate::types::query::with_payload::WITH_PAYLOAD,
    &crate::types::query::with_vector::WITH_VECTOR,
    &crate::types::vector::NAMED_VECTOR,
    &crate::types::vector::VECTOR,
];
