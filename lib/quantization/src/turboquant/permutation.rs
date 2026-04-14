use rand::RngExt;
use rand::prelude::StdRng;

/// A random permutation of indices `[0, dim)` and its precomputed inverse.
///
/// Used by [`HadamardRotation`](super::rotation::HadamardRotation) to shuffle
/// dimensions between Walsh-Hadamard transforms, breaking alignment between
/// the chunk boundaries and the original coordinate axes.
pub struct Permutation {
    /// Maps original index → permuted index.
    forward: Vec<usize>,

    /// Maps permuted index → original index (inverse of `forward`).
    backward: Vec<usize>,
}

impl Permutation {
    /// Create a new random permutation of `dim` elements using Fisher-Yates shuffle.
    pub fn new(rng: &mut StdRng, dim: usize) -> Self {
        let (forward, backward) = Self::generate_permutation(rng, dim);
        Self { forward, backward }
    }

    /// Reorder `buf` according to the forward or backward permutation.
    ///
    /// `tmp` is a scratch buffer that must be the same length as `buf`.
    pub fn apply(&self, buf: &mut [f64], tmp: &mut [f64], forward: bool) {
        let permutation = if forward {
            &self.forward
        } else {
            &self.backward
        };

        Self::apply_permutation(buf, tmp, permutation);
    }

    /// Generate a random permutation and its inverse using Fisher-Yates shuffle.
    fn generate_permutation(rng: &mut StdRng, n: usize) -> (Vec<usize>, Vec<usize>) {
        let mut perm: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            let j = rng.random_range(0..=i);
            perm.swap(i, j);
        }
        let mut inv = vec![0usize; n];
        for (i, &p) in perm.iter().enumerate() {
            inv[p] = i;
        }
        (perm, inv)
    }

    /// Apply a permutation out-of-place: element at index `i` moves to `perm[i]`.
    fn apply_permutation(buf: &mut [f64], tmp: &mut [f64], perm: &[usize]) {
        debug_assert_eq!(tmp.len(), buf.len());
        for (i, &p) in perm.iter().enumerate() {
            tmp[p] = buf[i];
        }
        buf.copy_from_slice(tmp);
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn forward_backward_are_valid_permutations() {
        for dim in [1, 2, 5, 64, 300] {
            let mut rng = StdRng::seed_from_u64(42);
            let perm = Permutation::new(&mut rng, dim);

            // Both should contain each index exactly once.
            for (label, mapping) in [("forward", &perm.forward), ("backward", &perm.backward)] {
                let mut sorted = mapping.clone();
                sorted.sort();
                assert_eq!(
                    sorted,
                    (0..dim).collect::<Vec<_>>(),
                    "{label} is not a valid permutation for dim={dim}"
                );
            }
        }
    }

    #[test]
    fn forward_and_backward_are_inverses() {
        let mut rng = StdRng::seed_from_u64(42);
        let perm = Permutation::new(&mut rng, 128);

        for i in 0..128 {
            assert_eq!(perm.backward[perm.forward[i]], i);
            assert_eq!(perm.forward[perm.backward[i]], i);
        }
    }

    #[test]
    fn apply_roundtrip() {
        let dim = 64;
        let mut rng = StdRng::seed_from_u64(42);
        let perm = Permutation::new(&mut rng, dim);

        let original: Vec<f64> = (0..dim).map(|i| i as f64).collect();
        let mut buf = original.clone();
        let mut tmp = vec![0.0; dim];

        // Forward then backward should recover the original.
        perm.apply(&mut buf, &mut tmp, true);
        assert_ne!(buf, original, "forward permutation should shuffle");
        perm.apply(&mut buf, &mut tmp, false);
        assert_eq!(buf, original, "roundtrip should recover original");
    }

    #[test]
    fn deterministic_with_same_seed() {
        let dim = 100;
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        let p1 = Permutation::new(&mut rng1, dim);
        let p2 = Permutation::new(&mut rng2, dim);

        assert_eq!(p1.forward, p2.forward);
        assert_eq!(p1.backward, p2.backward);
    }

    #[test]
    fn dim_one_is_identity() {
        let mut rng = StdRng::seed_from_u64(42);
        let perm = Permutation::new(&mut rng, 1);

        assert_eq!(perm.forward, vec![0]);
        assert_eq!(perm.backward, vec![0]);
    }
}
