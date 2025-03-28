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

pub fn is_close(a: f64, b: f64) -> bool {
    const ABS_TOL: f64 = 1e-6;
    const REL_TOL: f64 = 1e-6;
    is_close_tol(a, b, ABS_TOL, REL_TOL)
}

pub fn is_close_tol(a: f64, b: f64, abs_tol: f64, rel_tol: f64) -> bool {
    let tol = a.abs().max(b.abs()) * rel_tol + abs_tol;
    (a - b).abs() <= tol
}
