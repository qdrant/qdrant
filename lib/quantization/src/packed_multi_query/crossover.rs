//! When the packed kernels beat scoring pair by pair.
//!
//! A packed block costs about as much for one query token as for a full
//! block, while scoring pair by pair costs in proportion to the tokens, so
//! small multi-vectors are faster pair by pair. Where the two meet depends on
//! the kernel, the dimension and the CPU: each [`Crossover`] is the larger of
//! the crossovers measured on AMD Milan (AVX2) and AMD Genoa (AVX-512),
//! non-decreasing in the dimension. Other CPUs keep scoring pair by pair: the
//! packed kernels have no measured SIMD variant for them.

/// `(up to dim, fewest vectors)` rows by increasing dim; the last row also
/// covers any larger dim. `None`: never faster.
pub type Crossover = &'static [(usize, Option<usize>)];

/// Whether a packed multi-vector of `vectors_count` vectors of `dim` dims is
/// faster to score than its vectors one by one.
pub fn pays(crossover: Crossover, dim: usize, vectors_count: usize) -> bool {
    if !measured_cpu() {
        return false;
    }
    crossover
        .iter()
        .find(|&&(up_to, _)| dim <= up_to)
        .or(crossover.last())
        .and_then(|&(_, fewest)| fewest)
        .is_some_and(|fewest| vectors_count >= fewest)
}

fn measured_cpu() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_by_dim() {
        let crossover: Crossover = &[(128, Some(4)), (768, Some(16)), (1024, None)];
        let pays = |dim, vectors| pays(crossover, dim, vectors);
        if !measured_cpu() {
            assert!(!pays(64, 1000));
            return;
        }
        assert!(!pays(64, 3));
        assert!(pays(64, 4));
        assert!(pays(128, 4));
        assert!(!pays(129, 15));
        assert!(pays(129, 16));
        assert!(pays(768, 16));
        assert!(!pays(769, 1000));
        assert!(!pays(4096, 1000));
    }
}
