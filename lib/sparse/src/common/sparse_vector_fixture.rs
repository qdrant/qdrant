use rand::Rng;

use crate::common::sparse_vector::SparseVector;

/// Generates a non empty sparse vector
pub fn random_sparse_vector<R: Rng + ?Sized>(rnd_gen: &mut R, max_size: usize) -> SparseVector {
    let size = rnd_gen.gen_range(1..max_size);
    let mut tuples: Vec<(i32, f64)> = vec![];

    for i in 1..=size {
        let no_skip = rnd_gen.gen_bool(0.5);
        if no_skip {
            tuples.push((i as i32, rnd_gen.gen_range(0.0..100.0)));
        }
    }

    // make sure we have at least one vector
    if tuples.is_empty() {
        tuples.push((
            rnd_gen.gen_range(1..max_size) as i32,
            rnd_gen.gen_range(0.0..100.0),
        ));
    }

    SparseVector::from(tuples)
}

/// Generates a sparse vector with all dimensions filled
pub fn random_full_sparse_vector<R: Rng + ?Sized>(
    rnd_gen: &mut R,
    max_size: usize,
) -> SparseVector {
    let mut tuples: Vec<(i32, f64)> = vec![];

    for i in 1..=max_size {
        tuples.push((i as i32, rnd_gen.gen_range(0.0..100.0)));
    }

    SparseVector::from(tuples)
}
