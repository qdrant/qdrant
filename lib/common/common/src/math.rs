use crate::types::ScoreType;

/// Acts as a substitute for sigmoid function, but faster because it doesn't do exponent.
///
/// Range of output is (-1, 1)
#[inline]
pub fn fast_sigmoid(x: ScoreType) -> ScoreType {
    // from https://stackoverflow.com/questions/10732027/fast-sigmoid-algorithm
    x / (1.0 + x.abs())
}

/// Acts as a substitute for sigmoid function, but faster because it doesn't do exponent.
///
/// Scales the output to fit within (0, 1)
#[inline]
pub fn scaled_fast_sigmoid(x: ScoreType) -> ScoreType {
    0.5 * (fast_sigmoid(x) + 1.0)
}

/// Sample size for an unlimited population, confidence level 0.99 and margin of error 0.01.
pub const SAMPLE_SIZE_CL99_ME01: usize = 16641;

/// Calculates the ideal sample size for a given population size and proportion
/// at confidence level 0.99 and margin of error 0.01.
///
/// If population is `None`, considers an unlimited population.
pub fn ideal_sample_size(population_size: Option<usize>) -> usize {
    // z-score for confidence level 0.99
    const Z_SCORE: f32 = 2.58;

    // Margin of error for confidence level 0.99
    const MARGIN_OF_ERROR: f32 = 0.01;

    // We don't know the population proportion, so we use 0.5 as a conservative estimate
    const POPULATION_PROPORTION: f32 = 0.5;

    let unlimited_sample_size =
        ((Z_SCORE * Z_SCORE * POPULATION_PROPORTION * (1.0 - POPULATION_PROPORTION))
            / (MARGIN_OF_ERROR * MARGIN_OF_ERROR))
            .ceil();

    let Some(population_size) = population_size else {
        return unlimited_sample_size as usize;
    };

    let sample_size = (unlimited_sample_size * population_size as f32)
        / (unlimited_sample_size + population_size as f32 - 1.0);

    sample_size.ceil() as usize
}

#[cfg(test)]
mod tests {
    use crate::math::{ideal_sample_size, SAMPLE_SIZE_CL99_ME01};

    /// Tests equivalence against https://www.calculator.net/sample-size-calculator.html
    #[test]
    fn test_ideal_sample_size() {
        assert_eq!(ideal_sample_size(Some(1000)), 944);
        assert_eq!(ideal_sample_size(Some(10000)), 6247);
        assert_eq!(ideal_sample_size(Some(100000)), 14267);
        assert_eq!(ideal_sample_size(Some(1000000)), 16369);
        assert_eq!(ideal_sample_size(Some(10000000)), 16614);
        assert_eq!(ideal_sample_size(None), SAMPLE_SIZE_CL99_ME01);
    }
}
