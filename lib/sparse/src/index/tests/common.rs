use std::borrow::Cow;
use std::sync::OnceLock;

use common::types::PointOffsetType;
use rand::SeedableRng;
use tempfile::TempDir;

use crate::common::scores_memory_pool::{PooledScoresHandle, ScoresMemoryPool};
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimOffset, Weight};
use crate::index::inverted_index::InvertedIndex;
use crate::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use crate::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

static TEST_SCORES_POOL: OnceLock<ScoresMemoryPool> = OnceLock::new();

pub fn get_pooled_scores() -> PooledScoresHandle<'static> {
    TEST_SCORES_POOL
        .get_or_init(ScoresMemoryPool::default)
        .get()
}

pub struct TestIndex<I: InvertedIndex> {
    pub index: I,
    _temp_dir: TempDir,
}

impl<I: InvertedIndex> TestIndex<I> {
    fn from_ram(ram_index: InvertedIndexRam) -> Self {
        let temp_dir = tempfile::Builder::new()
            .prefix("test_index_dir")
            .tempdir()
            .unwrap();
        TestIndex {
            index: I::from_ram_index(Cow::Owned(ram_index), &temp_dir).unwrap(),
            _temp_dir: temp_dir,
        }
    }
}

pub fn random_sparse_vector<R: rand::Rng>(
    rnd_gen: &mut R,
    density: usize,
    vocab1: usize,
    vocab2: usize,
) -> RemappedSparseVector {
    let mut indices = vec![];
    let mut values = vec![];

    let value_range = -0.0..2.0;

    for _ in 0..density {
        loop {
            let index = if rnd_gen.random_bool(0.5) {
                rnd_gen.random_range(0..vocab1)
            } else {
                rnd_gen.random_range(vocab1..(vocab1 + vocab2))
            };

            if indices.contains(&(index as DimOffset)) {
                continue;
            }

            let value = rnd_gen.random_range(value_range.clone());
            indices.push(index as DimOffset);
            values.push(value);
            break;
        }
    }

    RemappedSparseVector { indices, values }
}

pub fn generate_sparse_index<W, R>(
    rnd: &mut R,
    count: usize,
    density: usize,
    vocab1: usize,
    vocab2: usize,
) -> TestIndex<InvertedIndexCompressedMmap<W>>
where
    W: Weight + 'static,
    R: rand::Rng,
{
    let mut builder = InvertedIndexBuilder::new();

    for i in 0..count {
        let vector = random_sparse_vector(rnd, density, vocab1, vocab2);
        builder.add(i as PointOffsetType, vector);
    }

    TestIndex::from_ram(builder.build())
}

pub fn build_index<W>(
    count: usize,
    density: usize,
    vocab1: usize,
    vocab2: usize,
) -> TestIndex<InvertedIndexCompressedMmap<W>>
where
    W: Weight + 'static,
{
    let seed = 42;
    let mut rnd_gen = rand::rngs::StdRng::seed_from_u64(seed);
    generate_sparse_index::<W, _>(&mut rnd_gen, count, density, vocab1, vocab2)
}

pub fn match_all(_p: PointOffsetType) -> bool {
    true
}
