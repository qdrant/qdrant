/// A reversible Linear Congruential Generator (LCG).
///
/// Uses Knuth's MMIX constants. The forward recurrence is:
///   state_{n+1} = A * state_n + C  (mod 2^64)
///
/// Because A is odd (coprime with 2^64), the modular multiplicative
/// inverse A_INV exists, allowing backward stepping:
///   state_n = A_INV * (state_{n+1} - C)  (mod 2^64)
struct ReversibleLcg {
    state: u64,
}

impl ReversibleLcg {
    /// Knuth's MMIX multiplier.
    const A: u64 = 6_364_136_223_846_793_005;
    /// Knuth's MMIX increment.
    const C: u64 = 1_442_695_040_888_963_407;
    /// Modular multiplicative inverse of A mod 2^64: A * A_INV ≡ 1 (mod 2^64).
    const A_INV: u64 = 13_877_824_140_714_322_085;

    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Iterator for ReversibleLcg {
    type Item = u64;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.state = self.state.wrapping_mul(Self::A).wrapping_add(Self::C);
        Some(self.state)
    }
}

impl DoubleEndedIterator for ReversibleLcg {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        let val = self.state;
        self.state = self.state.wrapping_sub(Self::C).wrapping_mul(Self::A_INV);
        Some(val)
    }
}

/// A random permutation that operates in-place without storing index maps.
///
/// Uses a Fisher-Yates shuffle driven by a [`ReversibleLcg`]. Forward
/// permutation replays the shuffle; backward permutation reverses it by
/// running the LCG in reverse.
///
/// Memory: O(1) per permutation (three scalars) instead of O(n) for index maps.
pub struct Permutation {
    seed: u64,
    count: usize,
    /// LCG state after all forward-pass random draws, used as starting
    /// point for the reverse pass.
    end_state: u64,
}

impl Permutation {
    /// Create a new permutation for `count` elements seeded by `seed`.
    pub fn new(seed: u64, count: usize) -> Self {
        let mut rng = ReversibleLcg::new(seed);
        for _ in 1..count {
            rng.next();
        }
        Self {
            seed,
            count,
            end_state: rng.state,
        }
    }

    /// Apply the forward permutation in-place (Fisher-Yates replay).
    pub fn permute(&self, arr: &mut [f64]) {
        debug_assert_eq!(arr.len(), self.count);
        let rng = ReversibleLcg::new(self.seed);
        for (i, rand) in ((1..self.count).rev()).zip(rng) {
            let j = Self::bounded_rand(rand, i as u64 + 1) as usize;
            arr.swap(i, j);
        }
    }

    /// Apply the inverse permutation in-place (reversed Fisher-Yates).
    pub fn unpermute(&self, arr: &mut [f64]) {
        debug_assert_eq!(arr.len(), self.count);
        let rng = ReversibleLcg::new(self.end_state);
        for (i, rand) in (1..self.count).zip(rng.rev()) {
            let j = Self::bounded_rand(rand, i as u64 + 1) as usize;
            arr.swap(i, j);
        }
    }

    /// Map a 64-bit LCG output to `[0, bound)` using the upper 32 bits.
    ///
    /// The low bits of an LCG have short periods (bit k has period 2^k),
    /// so bare `% bound` produces degenerate permutations. Using the
    /// upper half avoids this.
    fn bounded_rand(val: u64, bound: u64) -> u64 {
        (val >> 32) % bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lcg_reversibility() {
        let mut rng = ReversibleLcg::new(12345);
        let values: Vec<u64> = (0..100).map(|_| rng.next().unwrap()).collect();

        // Walking backward should recover every value in reverse order.
        for v in values.iter().rev() {
            assert_eq!(rng.next_back().unwrap(), *v);
        }
    }

    #[test]
    fn permute_unpermute_roundtrip() {
        for &count in &[2, 5, 64, 128, 300, 1000, 1024, 2048] {
            let original: Vec<f64> = (0..count).map(|i| i as f64).collect();
            let perm = Permutation::new(42, count);

            let mut arr = original.clone();
            perm.permute(&mut arr);

            // For large arrays the probability of identity is negligible.
            if count >= 5 {
                assert_ne!(arr, original, "count={count}: permute should shuffle");
            }

            perm.unpermute(&mut arr);
            assert_eq!(
                arr, original,
                "count={count}: roundtrip should recover original"
            );
        }
    }

    #[test]
    fn different_seeds_produce_different_permutations() {
        for &count in &[63, 64, 65] {
            let original: Vec<f64> = (0..count).map(|i| i as f64).collect();

            let p1 = Permutation::new(1, count);
            let p2 = Permutation::new(2, count);

            let mut a = original.clone();
            let mut b = original.clone();
            p1.permute(&mut a);
            p2.permute(&mut b);

            assert_ne!(
                a, b,
                "count={count}: different seeds should yield different permutations"
            );
        }
    }

    #[test]
    fn edge_case_count_zero() {
        let perm = Permutation::new(0, 0);
        let mut arr = vec![];
        perm.permute(&mut arr);
        assert!(arr.is_empty());
        perm.unpermute(&mut arr);
        assert!(arr.is_empty());
    }

    #[test]
    fn edge_case_count_one() {
        let perm = Permutation::new(0, 1);
        let mut arr = vec![42.0];
        perm.permute(&mut arr);
        assert_eq!(arr, vec![42.0]);
        perm.unpermute(&mut arr);
        assert_eq!(arr, vec![42.0]);
    }

    #[test]
    fn edge_case_count_two() {
        let original = vec![1.0, 2.0];
        let perm = Permutation::new(99, 2);
        let mut arr = original.clone();
        perm.permute(&mut arr);
        perm.unpermute(&mut arr);
        assert_eq!(arr, original);
    }

    #[test]
    fn permute_is_a_valid_permutation() {
        for &count in &[99, 100, 101] {
            let original: Vec<f64> = (0..count).map(|i| i as f64).collect();
            let perm = Permutation::new(42, count);

            let mut arr = original.clone();
            perm.permute(&mut arr);

            // Every element should appear exactly once.
            let mut sorted = arr.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            assert_eq!(sorted, original, "count={count}");
        }
    }

    /// Regression test: with bare `% bound` on raw LCG state, the lowest bit
    /// strictly alternates (A and C are both odd), making the i=1 Fisher-Yates
    /// step deterministic on seed parity. This halved the reachable permutation
    /// space — e.g. only 12/24 permutations for count=4.
    ///
    /// The fix uses the upper 32 bits (`>> 32`) which have much longer periods.
    #[test]
    fn all_small_permutations_reachable() {
        use std::collections::HashSet;

        let count = 4;
        let mut seen = HashSet::new();
        for seed in 0..10_000u64 {
            let original: Vec<f64> = (0..count).map(|i| i as f64).collect();
            let perm = Permutation::new(seed, count);
            let mut arr = original;
            perm.permute(&mut arr);
            seen.insert(arr.iter().map(|&v| v as u32).collect::<Vec<_>>());
        }
        assert_eq!(
            seen.len(),
            24,
            "expected all 4!=24 permutations reachable, got {}",
            seen.len()
        );
    }

    #[test]
    fn deterministic_with_same_seed() {
        for &count in &[99, 100, 101] {
            let original: Vec<f64> = (0..count).map(|i| i as f64).collect();

            let p1 = Permutation::new(42, count);
            let p2 = Permutation::new(42, count);

            let mut a = original.clone();
            let mut b = original.clone();
            p1.permute(&mut a);
            p2.permute(&mut b);

            assert_eq!(
                a, b,
                "count={count}: same seed should produce identical permutations"
            );
        }
    }
}
