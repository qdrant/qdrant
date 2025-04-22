use std::{error, result};

use common::counter::hardware_counter::HardwareCounterCell;
use rand::seq::IteratorRandom;

use crate::data_types::vectors::VectorElementType;
use crate::id_tracker::IdTracker;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub type Result<T, E = Error> = result::Result<T, E>;
pub type Error = Box<dyn error::Error>;

pub fn sampler(rng: impl rand::Rng) -> impl Iterator<Item = f32> {
    rng.sample_iter(rand::distr::StandardUniform)
}

pub fn insert_distributed_vectors(
    dim: usize,
    storage: &mut VectorStorageEnum,
    vectors: usize,
    sampler: &mut impl Iterator<Item = VectorElementType>,
) -> Result<()> {
    let start = storage.total_vector_count() as u32;
    let end = start + vectors as u32;

    let mut vector = vec![0.; dim];

    let hw_counter = HardwareCounterCell::new();

    for offset in start..end {
        for (item, value) in vector.iter_mut().zip(&mut *sampler) {
            *item = value;
        }

        storage.insert_vector(offset, vector.as_slice().into(), &hw_counter)?;
    }

    Ok(())
}

pub fn delete_random_vectors(
    rng: &mut impl rand::Rng,
    storage: &mut VectorStorageEnum,
    id_tracker: &mut impl IdTracker,
    vectors: usize,
) -> Result<()> {
    let offsets = (0..storage.total_vector_count() as _).choose_multiple(rng, vectors);

    for offset in offsets {
        storage.delete_vector(offset)?;
        id_tracker.drop(crate::types::ExtendedPointId::NumId(offset.into()))?;
    }

    Ok(())
}
