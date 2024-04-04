#![allow(deprecated)]

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{
    Distance, HnswConfig, Indexes, PayloadStorageType, QuantizationConfig, SegmentConfig,
    SegmentState, SeqNumberType, VectorDataConfig, VectorStorageType,
};

#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[deprecated = "use SegmentConfig instead"]
pub struct SegmentConfigV5 {
    pub vector_data: HashMap<String, VectorDataConfigV5>,
    /// Type of index used for search
    pub index: Indexes,
    /// Type of vector storage
    pub storage_type: StorageTypeV5,
    /// Defines payload storage type
    #[serde(default)]
    pub payload_storage_type: PayloadStorageType,
    /// Quantization parameters. If none - quantization is disabled.
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
}

impl From<SegmentConfigV5> for SegmentConfig {
    fn from(old_segment: SegmentConfigV5) -> Self {
        let vector_data = old_segment
            .vector_data
            .into_iter()
            .map(|(vector_name, old_data)| {
                let new_data = VectorDataConfig {
                    size: old_data.size,
                    distance: old_data.distance,
                    // Use HNSW index if vector specific one is set, or fall back to segment index
                    index: match old_data.hnsw_config {
                        Some(hnsw_config) => Indexes::Hnsw(hnsw_config),
                        None => old_segment.index.clone(),
                    },
                    // Remove vector specific quantization config if no segment one is set
                    // This is required because in some cases this was incorrectly set on the vector
                    // level
                    quantization_config: old_segment
                        .quantization_config
                        .as_ref()
                        .and(old_data.quantization_config),
                    // Mmap if explicitly on disk, otherwise convert old storage type
                    storage_type: (old_data.on_disk == Some(true))
                        .then_some(VectorStorageType::Mmap)
                        .unwrap_or_else(|| old_segment.storage_type.into()),
                    multi_vec_config: None,
                };

                (vector_name, new_data)
            })
            .collect();

        SegmentConfig {
            vector_data,
            sparse_vector_data: Default::default(),
            payload_storage_type: old_segment.payload_storage_type,
        }
    }
}

/// Type of vector storage
#[derive(Default, Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "options")]
#[deprecated]
pub enum StorageTypeV5 {
    // Store vectors in memory and use persistence storage only if vectors are changed
    #[default]
    InMemory,
    // Use memmap to store vectors, a little slower than `InMemory`, but requires little RAM
    Mmap,
}

impl From<StorageTypeV5> for VectorStorageType {
    fn from(old: StorageTypeV5) -> Self {
        match old {
            StorageTypeV5::InMemory => Self::Memory,
            StorageTypeV5::Mmap => Self::Mmap,
        }
    }
}

/// Config of single vector data storage
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[deprecated = "use VectorDataConfig instead"]
pub struct VectorDataConfigV5 {
    /// Size of a vectors used
    pub size: usize,
    /// Type of distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Vector specific HNSW config that overrides collection config
    #[serde(default)]
    pub hnsw_config: Option<HnswConfig>,
    /// Vector specific quantization config that overrides collection config
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
    /// If true - vectors will not be stored in memory.
    /// Instead, it will store vectors on mmap-files.
    /// If enabled, search performance will defined by disk speed
    /// and fraction of vectors that fit in RAM.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
#[deprecated = "use SegmentState instead"]
pub struct SegmentStateV5 {
    pub version: Option<SeqNumberType>,
    pub config: SegmentConfigV5,
}

impl From<SegmentStateV5> for SegmentState {
    fn from(old: SegmentStateV5) -> Self {
        Self {
            version: old.version,
            config: old.config.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ScalarQuantization, ScalarQuantizationConfig};

    #[test]
    fn convert_from_v5_to_newest() {
        let old_segment = SegmentConfigV5 {
            vector_data: vec![
                (
                    "vec1".to_string(),
                    VectorDataConfigV5 {
                        size: 10,
                        distance: Distance::Dot,
                        hnsw_config: Some(HnswConfig {
                            m: 20,
                            ef_construct: 100,
                            full_scan_threshold: 10000,
                            max_indexing_threads: 0,
                            on_disk: None,
                            payload_m: Some(10),
                        }),
                        quantization_config: None,
                        on_disk: None,
                    },
                ),
                (
                    "vec2".to_string(),
                    VectorDataConfigV5 {
                        size: 10,
                        distance: Distance::Dot,
                        hnsw_config: None,
                        quantization_config: Some(QuantizationConfig::Scalar(ScalarQuantization {
                            scalar: ScalarQuantizationConfig {
                                r#type: Default::default(),
                                quantile: Some(0.99),
                                always_ram: Some(true),
                            },
                        })),
                        on_disk: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            index: Indexes::Hnsw(HnswConfig {
                m: 25,
                ef_construct: 120,
                full_scan_threshold: 10000,
                max_indexing_threads: 0,
                on_disk: None,
                payload_m: None,
            }),
            storage_type: StorageTypeV5::InMemory,
            payload_storage_type: PayloadStorageType::default(),
            quantization_config: None,
        };

        let new_segment: SegmentConfig = old_segment.into();

        eprintln!("new = {:#?}", new_segment);

        match &new_segment.vector_data.get("vec1").unwrap().index {
            Indexes::Plain { .. } => panic!("expected HNSW index"),
            Indexes::Hnsw(hnsw) => {
                assert_eq!(hnsw.m, 20);
            }
        }

        match &new_segment.vector_data.get("vec2").unwrap().index {
            Indexes::Plain { .. } => panic!("expected HNSW index"),
            Indexes::Hnsw(hnsw) => {
                assert_eq!(hnsw.m, 25);
            }
        }

        if new_segment
            .vector_data
            .get("vec1")
            .unwrap()
            .quantization_config
            .is_some()
        {
            panic!("expected no quantization");
        }
    }

    #[test]
    fn convert_from_v5_to_newest_2() {
        let old_segment = SegmentConfigV5 {
            vector_data: vec![
                (
                    "vec1".to_string(),
                    VectorDataConfigV5 {
                        size: 10,
                        distance: Distance::Dot,
                        hnsw_config: None,
                        quantization_config: None,
                        on_disk: None,
                    },
                ),
                (
                    "vec2".to_string(),
                    VectorDataConfigV5 {
                        size: 10,
                        distance: Distance::Dot,
                        hnsw_config: None,
                        quantization_config: Some(QuantizationConfig::Scalar(ScalarQuantization {
                            scalar: ScalarQuantizationConfig {
                                r#type: Default::default(),
                                quantile: Some(0.99),
                                always_ram: Some(true),
                            },
                        })),
                        on_disk: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            index: Indexes::Hnsw(HnswConfig {
                m: 25,
                ef_construct: 120,
                full_scan_threshold: 10000,
                max_indexing_threads: 0,
                on_disk: None,
                payload_m: None,
            }),
            storage_type: StorageTypeV5::InMemory,
            payload_storage_type: PayloadStorageType::default(),
            quantization_config: Some(QuantizationConfig::Scalar(ScalarQuantization {
                scalar: ScalarQuantizationConfig {
                    r#type: Default::default(),
                    quantile: Some(0.95),
                    always_ram: Some(true),
                },
            })),
        };

        let new_segment: SegmentConfig = old_segment.into();

        eprintln!("new = {:#?}", new_segment);

        if new_segment
            .vector_data
            .get("vec1")
            .unwrap()
            .quantization_config
            .is_some()
        {
            panic!("expected no quantization");
        }

        match &new_segment
            .vector_data
            .get("vec2")
            .unwrap()
            .quantization_config
        {
            Some(q) => match q {
                QuantizationConfig::Scalar(scalar) => {
                    assert_eq!(scalar.scalar.quantile, Some(0.99));
                }
                QuantizationConfig::Product(_) => {
                    panic!("expected scalar quantization")
                }
                QuantizationConfig::Binary(_) => {
                    panic!("expected scalar quantization")
                }
            },
            _ => {
                panic!("expected quantization")
            }
        }
    }
}
