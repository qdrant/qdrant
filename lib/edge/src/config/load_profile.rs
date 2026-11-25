//! Reconciles a [`LoadProfile`] with the collection config.

use std::collections::HashMap;

use segment::data_types::load_profile::LoadProfile;
use segment::types::{Memory, PayloadFieldSchema, PayloadKeyType, VectorName};

use super::shard::EdgeConfig;

impl EdgeConfig {
    /// `profile` without the components that load cold anyway: those the config explicitly
    /// places cold, unindexed payload fields and undefined vectors. Unset placements count
    /// as warm. Meant for comparing profiles; open with the full one.
    pub fn disregard_cold(
        &self,
        mut profile: LoadProfile,
        payload_schema: &HashMap<PayloadKeyType, PayloadFieldSchema>,
    ) -> LoadProfile {
        profile.retain_warm(
            |vector_name| self.vector_may_load_warm(vector_name),
            |field| {
                payload_schema
                    .get(field)
                    .is_some_and(|schema| !schema.is_cold())
            },
            !self.payload_memory_placement().is_cold(),
        );
        profile
    }

    fn vector_may_load_warm(&self, vector_name: &VectorName) -> bool {
        if let Some(params) = self.vectors.get(vector_name) {
            let config = self.dense_vector_optimizer_config(params);
            let storage_cold = config.memory_placement() == Some(Memory::Cold);
            let index_cold = config.hnsw_config.memory_placement().is_cold();
            let quantized_cold = config
                .quantization_config
                .as_ref()
                .is_none_or(|quantization| {
                    quantization
                        .resolved_memory_placement(storage_cold)
                        .is_cold()
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
    use common::universal_io::Populate;
    use segment::json_path::JsonPath;
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, HnswConfig, Match, PayloadSchemaType,
        QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig, ScalarType,
        ValueVariants,
    };
    use serde_json::json;

    use super::*;
    use crate::config::vectors::EdgeVectorParams;

    fn dense(memory: Option<Memory>) -> EdgeVectorParams {
        let mut params = EdgeVectorParams::builder(4, Distance::Dot).build();
        params.memory = memory;
        params
    }

    fn hnsw(memory: Memory) -> HnswConfig {
        HnswConfig {
            memory: Some(memory),
            ..Default::default()
        }
    }

    fn scalar(memory: Option<Memory>) -> QuantizationConfig {
        #[allow(deprecated)]
        let scalar = ScalarQuantizationConfig {
            r#type: ScalarType::Int8,
            quantile: None,
            always_ram: None,
            memory,
        };
        QuantizationConfig::Scalar(ScalarQuantization { scalar })
    }

    fn path(raw: &str) -> JsonPath {
        raw.parse().unwrap()
    }

    fn filter_on(key: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            path(key),
            Match::from(ValueVariants::String(String::from("x"))),
        )))
    }

    fn schema(raw: serde_json::Value) -> PayloadFieldSchema {
        serde_json::from_value(raw).unwrap()
    }

    fn warm_vectors<'a>(config: &EdgeConfig, names: &[&'a str]) -> Vec<&'a str> {
        let profile = names
            .iter()
            .map(|name| LoadProfile::for_search(name, None))
            .reduce(|mut merged, profile| {
                merged.merge(profile);
                merged
            })
            .unwrap();
        let reduced = config.disregard_cold(profile, &HashMap::new());
        names
            .iter()
            .copied()
            .filter(|name| reduced.vector_storage_placement(name).is_none())
            .collect()
    }

    #[test]
    fn a_dense_vector_is_cold_only_when_every_component_is() {
        let mut config = EdgeConfig::builder()
            .vector("unset", dense(None))
            .vector("cold", dense(Some(Memory::Cold)))
            .build();
        assert_eq!(warm_vectors(&config, &["unset", "cold"]), ["unset", "cold"]);

        config.hnsw_config = Some(hnsw(Memory::Cold));
        assert_eq!(warm_vectors(&config, &["unset", "cold"]), ["unset"]);

        config.vectors.get_mut("cold").unwrap().hnsw_config = Some(hnsw(Memory::Cached));
        assert_eq!(warm_vectors(&config, &["cold"]), ["cold"]);
        config.vectors.get_mut("cold").unwrap().hnsw_config = None;

        config.quantization_config = Some(scalar(None));
        assert!(warm_vectors(&config, &["cold"]).is_empty());
        config.quantization_config = Some(scalar(Some(Memory::Pinned)));
        assert_eq!(warm_vectors(&config, &["cold"]), ["cold"]);
        config.vectors.get_mut("cold").unwrap().quantization_config =
            Some(scalar(Some(Memory::Cold)));
        assert!(warm_vectors(&config, &["cold"]).is_empty());
    }

    #[test]
    fn payload_fields_without_a_warm_index_are_dropped() {
        let payload_schema = HashMap::from([
            (
                path("indexed"),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
            ),
            (
                path("cold"),
                schema(json!({"type": "keyword", "memory": "cold"})),
            ),
            (
                path("cached"),
                schema(json!({"type": "keyword", "memory": "cached"})),
            ),
        ]);
        let mut profile = LoadProfile::for_retrieve();
        for field in ["indexed", "cold", "cached", "unindexed"] {
            profile.merge(LoadProfile::for_scroll(Some(&filter_on(field)), None));
        }
        let config = EdgeConfig::builder().payload_memory(Memory::Cold).build();
        let reduced = config.disregard_cold(profile.clone(), &payload_schema);
        for (field, warm) in [
            ("indexed", true),
            ("cached", true),
            ("cold", false),
            ("unindexed", false),
        ] {
            assert_eq!(
                reduced.payload_index_placement(&path(field)).is_none(),
                warm,
                "{field}"
            );
        }
        assert_eq!(reduced.payload_storage_placement(), Some(Populate::No));
        let config = EdgeConfig::builder().payload_memory(Memory::Cached).build();
        let only_unindexed = LoadProfile::for_scroll(Some(&filter_on("unindexed")), None);
        let reduced = config.disregard_cold(only_unindexed, &payload_schema);
        assert_eq!(reduced.payload_storage_placement(), None);
        assert_eq!(
            reduced.payload_index_placement(&path("unindexed")),
            Some(Populate::No)
        );
    }

    #[test]
    fn requests_adding_only_cold_components_reduce_to_the_active_profile() {
        let mut config = EdgeConfig::builder()
            .vector("warm", dense(None))
            .vector("cold", dense(Some(Memory::Cold)))
            .payload_memory(Memory::Cold)
            .build();
        config.hnsw_config = Some(hnsw(Memory::Cold));
        let payload_schema = HashMap::from([(
            path("city"),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
        )]);
        let covers = |active: &LoadProfile, requested: LoadProfile| {
            let mut merged = active.clone();
            merged.merge(requested);
            config.disregard_cold(merged, &payload_schema)
                == config.disregard_cold(active.clone(), &payload_schema)
        };

        let active = LoadProfile::for_search("warm", None);
        assert!(covers(&active, LoadProfile::for_search("cold", None)));
        assert!(covers(
            &active,
            LoadProfile::for_search("cold", Some(&filter_on("unindexed")))
        ));
        assert!(!covers(
            &active,
            LoadProfile::for_search("cold", Some(&filter_on("city")))
        ));
        assert!(!covers(
            &LoadProfile::for_retrieve(),
            LoadProfile::for_search("warm", None)
        ));
    }
}
