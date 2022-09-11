use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::named_vectors::NamedVectors;
use crate::common::utils::transpose_map_into_named_vector;

/// Type of vector element.
pub type VectorElementType = f32;

pub const DEFAULT_VECTOR_NAME: &str = "";

/// Type for vector
pub type VectorType = Vec<VectorElementType>;

pub fn default_vector(vec: Vec<VectorElementType>) -> NamedVectors<'static> {
    NamedVectors::from([(DEFAULT_VECTOR_NAME.to_owned(), vec)])
}

pub fn only_default_vector(vec: &[VectorElementType]) -> NamedVectors {
    NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vec)
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum VectorStruct {
    Single(VectorType),
    Multi(HashMap<String, Vec<VectorElementType>>),
}

impl From<VectorType> for VectorStruct {
    fn from(v: VectorType) -> Self {
        VectorStruct::Single(v)
    }
}

impl From<&[VectorElementType]> for VectorStruct {
    fn from(v: &[VectorElementType]) -> Self {
        VectorStruct::Single(v.to_vec())
    }
}

impl<'a> From<NamedVectors<'a>> for VectorStruct {
    fn from(v: NamedVectors) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            VectorStruct::Single(v.into_default_vector().unwrap())
        } else {
            VectorStruct::Multi(v.into_owned_map())
        }
    }
}

impl VectorStruct {
    pub fn get(&self, name: &str) -> Option<&VectorType> {
        match self {
            VectorStruct::Single(v) => {
                if name == DEFAULT_VECTOR_NAME {
                    Some(v)
                } else {
                    None
                }
            }
            VectorStruct::Multi(v) => v.get(name),
        }
    }

    pub fn into_all_vectors(self) -> NamedVectors<'static> {
        match self {
            VectorStruct::Single(v) => default_vector(v),
            VectorStruct::Multi(v) => NamedVectors::from_map(v),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct NamedVector {
    pub name: String,
    pub vector: VectorType,
}

// {
//   "vector": [1.0, 2.0, 3.0]
// }
//
// or
//
// {
//   "vector": {
//     "vector": [1.0, 2.0, 3.0],
//     "name": "image-embeddings"
//   }
// }
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum NamedVectorStruct {
    Default(VectorType),
    Named(NamedVector),
}

impl From<VectorType> for NamedVectorStruct {
    fn from(v: VectorType) -> Self {
        NamedVectorStruct::Default(v)
    }
}

impl From<NamedVectorStruct> for NamedVector {
    fn from(v: NamedVectorStruct) -> Self {
        match v {
            NamedVectorStruct::Default(v) => NamedVector {
                name: DEFAULT_VECTOR_NAME.to_owned(),
                vector: v,
            },
            NamedVectorStruct::Named(v) => v,
        }
    }
}

impl From<NamedVector> for NamedVectorStruct {
    fn from(v: NamedVector) -> Self {
        NamedVectorStruct::Named(v)
    }
}

impl NamedVectorStruct {
    pub fn get_name(&self) -> &str {
        match self {
            NamedVectorStruct::Default(_) => DEFAULT_VECTOR_NAME,
            NamedVectorStruct::Named(v) => &v.name,
        }
    }

    pub fn get_vector(&self) -> &VectorType {
        match self {
            NamedVectorStruct::Default(v) => v,
            NamedVectorStruct::Named(v) => &v.vector,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum BatchVectorStruct {
    Single(Vec<VectorType>),
    Multi(HashMap<String, Vec<VectorType>>),
}

impl From<Vec<VectorType>> for BatchVectorStruct {
    fn from(v: Vec<VectorType>) -> Self {
        BatchVectorStruct::Single(v)
    }
}

impl From<HashMap<String, Vec<VectorType>>> for BatchVectorStruct {
    fn from(v: HashMap<String, Vec<VectorType>>) -> Self {
        if v.len() == 1 && v.contains_key(DEFAULT_VECTOR_NAME) {
            BatchVectorStruct::Single(v.into_iter().next().unwrap().1)
        } else {
            BatchVectorStruct::Multi(v)
        }
    }
}

impl BatchVectorStruct {
    pub fn single(&mut self) -> &mut Vec<VectorType> {
        match self {
            BatchVectorStruct::Single(v) => v,
            BatchVectorStruct::Multi(v) => v.get_mut(DEFAULT_VECTOR_NAME).unwrap(),
        }
    }

    pub fn multi(&mut self) -> &mut HashMap<String, Vec<VectorType>> {
        match self {
            BatchVectorStruct::Single(_) => panic!("BatchVectorStruct is not Single"),
            BatchVectorStruct::Multi(v) => v,
        }
    }

    pub fn into_all_vectors(self, num_records: usize) -> Vec<NamedVectors<'static>> {
        match self {
            BatchVectorStruct::Single(vectors) => vectors.into_iter().map(default_vector).collect(),
            BatchVectorStruct::Multi(named_vectors) => {
                if named_vectors.is_empty() {
                    vec![NamedVectors::default(); num_records]
                } else {
                    transpose_map_into_named_vector(named_vectors)
                }
            }
        }
    }
}
