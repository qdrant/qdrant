use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use tempfile::{Builder, TempDir};

use crate::config::StorageOptions;
use crate::{Blob, BlobStore};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Payload(pub Map<String, serde_json::Value>);

impl Default for Payload {
    fn default() -> Self {
        Payload(serde_json::Map::new())
    }
}

impl Blob for Payload {
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    fn from_bytes(data: &[u8]) -> Self {
        serde_json::from_slice(data).unwrap()
    }
}

/// Create an empty storage with the default configuration
pub fn empty_storage() -> (TempDir, BlobStore<Payload>) {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let storage = BlobStore::new(dir.path().to_path_buf(), Default::default()).unwrap();
    (dir, storage)
}

/// Create an empty storage with a specific page size
pub fn empty_storage_sized(page_size: usize) -> (TempDir, BlobStore<Payload>) {
    let dir = Builder::new().prefix("test-storage").tempdir().unwrap();
    let options = StorageOptions {
        page_size_bytes: Some(page_size),
        ..Default::default()
    };
    let storage = BlobStore::new(dir.path().to_path_buf(), options).unwrap();
    (dir, storage)
}

pub fn random_word(rng: &mut impl Rng) -> String {
    let len = rng.gen_range(1..10);
    let mut word = String::with_capacity(len);
    for _ in 0..len {
        word.push(rng.gen_range(b'a'..=b'z') as char);
    }
    word
}

pub fn random_payload(rng: &mut impl Rng, size_factor: usize) -> Payload {
    let mut payload = Payload::default();

    let word = random_word(rng);

    let sentence = (0..rng.gen_range(1..20 * size_factor))
        .map(|_| random_word(rng))
        .collect::<Vec<_>>()
        .join(" ");

    let distr = Uniform::new(0, 100000);
    let indices = (0..rng.gen_range(1..100 * size_factor))
        .map(|_| distr.sample(rng))
        .collect::<Vec<_>>();

    payload.0 = serde_json::json!(
        {
            "word": word, // string
            "sentence": sentence, // string
            "number": rng.gen_range(0..1000), // number
            "indices": indices, // array of numbers
            "bool": rng.gen_bool(0.5), // boolean
            "null": serde_json::Value::Null, // null
            "object": {
                "bool": rng.gen_bool(0.5),
            }, // object
        }
    )
    .as_object()
    .unwrap()
    .clone();

    payload
}

pub fn minimal_payload() -> Payload {
    Payload(serde_json::json!({"a": 1}).as_object().unwrap().clone())
}

pub const HM_FIELDS: [&str; 23] = [
    "article_id",
    "product_code",
    "prod_name",
    "product_type_no",
    "product_type_name",
    "product_group_name",
    "graphical_appearance_no",
    "graphical_appearance_name",
    "colour_group_code",
    "colour_group_name",
    "perceived_colour_value_id",
    "perceived_colour_value_name",
    "perceived_colour_master_id",
    "perceived_colour_master_name",
    "department_no",
    "department_name",
    "index_code,index_name",
    "index_group_no",
    "index_group_name",
    "section_no,section_name",
    "garment_group_no",
    "garment_group_name",
    "detail_desc",
];

#[cfg(test)]
mod tests {
    use crate::fixtures::Payload;
    use crate::Blob;

    #[test]
    fn test_serde_symmetry() {
        let mut payload = Payload::default();
        payload.0.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        let bytes = payload.to_bytes();

        let deserialized = Payload::from_bytes(&bytes);
        assert_eq!(payload, deserialized);
    }
}
