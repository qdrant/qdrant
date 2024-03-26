use crate::types::ScoreType;

/// Acts as a substitute for sigmoid function, but faster because it doesn't do exponent.
///
/// Range of output is (-1, 1)
#[inline]
pub fn fast_sigmoid(x: ScoreType) -> ScoreType {
    // from https://stackoverflow.com/questions/10732027/fast-sigmoid-algorithm
    x / ScoreType::from(1.0 + x.abs())
}

/// Acts as a substitute for sigmoid function, but faster because it doesn't do exponent.
///
/// Scales the output to fit within (0, 1)
#[inline]
pub fn scaled_fast_sigmoid(x: ScoreType) -> ScoreType {
    ScoreType::from(0.5) * (fast_sigmoid(x) + ScoreType::from(1.0))
}
