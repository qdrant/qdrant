//! Reconciles a [`LoadProfile`] with the collection config.

use std::collections::HashMap;

use segment::data_types::load_profile::LoadProfile;
use segment::types::{Memory, PayloadFieldSchema, PayloadKeyType, VectorName};

use super::shard::EdgeConfig;

impl EdgeConfig {
    /// `profile` without the components that load cold anyway: those the config explicitly
    /// places cold and undefined vectors, plus unindexed payload fields when the payload
    /// storage is cold (they keep a warm one warm). Unset placements count as warm. Meant for
    /// comparing profiles; open with the full one.
    pub fn disregard_cold(
        &self,
        mut profile: LoadProfile,
        payload_schema: &HashMap<PayloadKeyType, PayloadFieldSchema>,
    ) -> LoadProfile {
        let payload_storage_cold = self.payload_memory_placement().is_cold();
        profile.retain_warm(
            |vector_name| self.vector_may_load_warm(vector_name),
            |field| {
                !payload_storage_cold
                    || payload_schema
                        .get(field)
                        .is_some_and(|schema| !schema.is_cold())
            },
        );
        profile
    }

    fn vector_may_load_warm(&self, vector_name: &VectorName) -> bool {
        if let Some(params) = self.vectors.get(vector_name) {
            let config = self.dense_vector_optimizer_config(params);
            let storage_cold = config.memory_placement() == Some(Memory::Cold);
            let index_cold = config.hnsw_config.memory_placement().is_cold();
            // Unset, quantized vectors follow the storage.
            let quantized_cold = config
                .quantization_config
                .as_ref()
                .is_none_or(|quantization| {
                    quantization
                        .memory_placement()
                        .map_or(storage_cold, Memory::is_cold)
                });
            return !(storage_cold && index_cold && quantized_cold);
        }
        if let Some(params) = self.sparse_vectors.get(vector_name) {
            return params.memory_placement() != Some(Memory::Cold);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, HnswConfig, Match, PayloadSchemaType,
        QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig, ScalarType,
        ValueVariants,
    };
    use serde_json::json;

    use super::*;
    use crate::config::vectors::{EdgeSparseVectorParams, EdgeVectorParams};

    fn dense(
        memory: Option<Memory>,
        hnsw: Option<Memory>,
        quantization: Option<Option<Memory>>,
    ) -> EdgeVectorParams {
        let mut params = EdgeVectorParams::builder(4, Distance::Dot).build();
        params.memory = memory;
        params.hnsw_config = hnsw.map(|memory| HnswConfig {
            memory: Some(memory),
            ..Default::default()
        });
        #[allow(deprecated)]
        let scalar = |memory| ScalarQuantizationConfig {
            r#type: ScalarType::Int8,
            quantile: None,
            always_ram: None,
            memory,
        };
        params.quantization_config = quantization.map(|memory| {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar(memory),
            })
        });
        params
    }

    fn filter_on(key: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            key.parse().unwrap(),
            Match::from(ValueVariants::String(String::from("x"))),
        )))
    }

    /// Whether a shard opened for a retrieve serves `requested` without reopening.
    #[test]
    fn covers_requests_adding_only_components_that_load_cold() {
        use Memory::{Cached, Cold, Pinned};

        let sparse = |memory| EdgeSparseVectorParams {
            memory,
            ..Default::default()
        };
        let mut config = EdgeConfig::builder()
            .vector("unset", dense(None, None, None))
            .vector("cold", dense(Some(Cold), None, None))
            .vector(
                "cold_own_hnsw_cached",
                dense(Some(Cold), Some(Cached), None),
            )
            .vector("cold_quantized_unset", dense(Some(Cold), None, Some(None)))
            .vector(
                "cold_quantized_pinned",
                dense(Some(Cold), None, Some(Some(Pinned))),
            )
            .vector(
                "cached_own_hnsw_cold",
                dense(Some(Cached), Some(Cold), None),
            )
            .sparse_vector("sparse_cold", sparse(Some(Cold)))
            .sparse_vector("sparse_unset", sparse(None))
            .payload_memory(Cold)
            .build();
        config.hnsw_config = Some(HnswConfig {
            memory: Some(Cold),
            ..Default::default()
        });
        let payload_schema = HashMap::from([
            (
                "indexed".parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
            ),
            (
                "cold_index".parse().unwrap(),
                serde_json::from_value(json!({"type": "keyword", "memory": "cold"})).unwrap(),
            ),
        ]);
        let covers = |config: &EdgeConfig, requested: LoadProfile| {
            let active = LoadProfile::for_retrieve();
            let mut merged = active.clone();
            merged.merge(requested);
            config.disregard_cold(merged, &payload_schema)
                == config.disregard_cold(active, &payload_schema)
        };

        for (vector, covered) in [
            ("unset", false),
            ("cold", true),
            ("cold_own_hnsw_cached", false),
            ("cold_quantized_unset", true),
            ("cold_quantized_pinned", false),
            ("cached_own_hnsw_cold", false),
            ("sparse_cold", true),
            ("sparse_unset", false),
            ("missing", true),
        ] {
            let requested = LoadProfile::for_search(vector, None);
            assert_eq!(covers(&config, requested), covered, "vector {vector}");
        }
        for (field, covered) in [
            ("indexed", false),
            ("cold_index", true),
            ("unindexed", true),
        ] {
            let requested = LoadProfile::for_search("cold", Some(&filter_on(field)));
            assert_eq!(covers(&config, requested), covered, "field {field}");
        }

        // A warm payload storage loads warm for any field.
        config.payload_memory = Some(Cached);
        let requested = LoadProfile::for_scroll(Some(&filter_on("unindexed")), None);
        assert!(!covers(&config, requested));
    }
}
