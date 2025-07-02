use std::borrow::Cow;
use std::sync::Arc;

use rust_stemmers::Algorithm;

use crate::data_types::index::{SnowballParameters, StemmingAlgorithm};

/// Abstraction to handle different stemming libraries and algorithms with a clean API.
#[derive(Clone)]
pub enum Stemmer {
    // TODO(rocksdb): Remove `Clone` and this Arc once rocksdb has been removed!
    Snowball(Arc<rust_stemmers::Stemmer>),
}

impl std::fmt::Debug for Stemmer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snowball(_) => f.debug_tuple("Snowball").finish(),
        }
    }
}

impl Stemmer {
    pub fn from_algorithm(config: &StemmingAlgorithm) -> Self {
        match config {
            StemmingAlgorithm::Snowball(SnowballParameters {
                r#type: _,
                language,
            }) => Self::Snowball(Arc::new(rust_stemmers::Stemmer::create(Algorithm::from(
                *language,
            )))),
        }
    }

    pub fn stem<'a>(&self, input: Cow<'a, str>) -> Cow<'a, str> {
        match self {
            Stemmer::Snowball(algorithm) => algorithm.stem_cow(input),
        }
    }
}
