use crate::collection_manager::probabilistic_search_sampling::find_search_sampling_over_point_distribution;

/// Returns suggested search sampling size for a given number of points and required limit.
pub(super) fn sampling_limit(
    limit: usize,
    ef_limit: Option<usize>,
    segment_points: usize,
    total_points: usize,
) -> usize {
    // shortcut empty segment
    if segment_points == 0 {
        return 0;
    }
    let segment_probability = segment_points as f64 / total_points as f64;
    let poisson_sampling =
        find_search_sampling_over_point_distribution(limit as f64, segment_probability);

    // if no ef_limit was found, it is a plain index => sampling optimization is not needed.
    let effective = ef_limit.map_or(limit, |ef_limit| {
        effective_limit(limit, ef_limit, poisson_sampling)
    });
    log::trace!(
        "sampling: {effective}, poisson: {poisson_sampling} segment_probability: {segment_probability}, segment_points: {segment_points}, total_points: {total_points}",
    );
    effective
}

/// Determines the effective ef limit value for the given parameters.
fn effective_limit(limit: usize, ef_limit: usize, poisson_sampling: usize) -> usize {
    // Prefer the highest of poisson_sampling/ef_limit, but never be higher than limit
    poisson_sampling.max(ef_limit).min(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sampling_limit() {
        assert_eq!(sampling_limit(1000, None, 464530, 35103551), 1000);
    }

    #[test]
    fn test_sampling_limit_ef() {
        assert_eq!(sampling_limit(1000, Some(100), 464530, 35103551), 100);
    }

    #[test]
    fn test_sampling_limit_high() {
        assert_eq!(sampling_limit(1000000, None, 464530, 35103551), 1000000);
    }

    /// Tests whether calculating the effective ef limit value is correct.
    ///
    /// Because there was confusion about what the effective value should be for some input
    /// combinations, we decided to write this tests to ensure correctness.
    ///
    /// See: <https://github.com/qdrant/qdrant/pull/1694>
    #[test]
    fn test_effective_limit() {
        // Test cases to assert: (limit, ef_limit, poisson_sampling, effective)
        let tests = [
            (1000, 128, 150, 150),
            (1000, 128, 110, 128),
            (130, 128, 150, 130),
            (130, 128, 110, 128),
            (50, 128, 150, 50),
            (50, 128, 110, 50),
            (500, 1000, 300, 500),
            (500, 400, 300, 400),
            (1000, 0, 150, 150),
            (1000, 0, 110, 110),
        ];
        tests.into_iter().for_each(|(limit, ef_limit, poisson_sampling, effective)| assert_eq!(
            effective_limit(limit, ef_limit, poisson_sampling),
            effective,
            "effective limit for [limit: {limit}, ef_limit: {ef_limit}, poisson_sampling: {poisson_sampling}] must be {effective}",
        ));
    }
}
