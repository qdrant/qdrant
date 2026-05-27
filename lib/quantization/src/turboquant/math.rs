/// Standard normal CDF Φ(x) = (1 + erf(x/√2)) / 2 via the Abramowitz & Stegun
/// 7.1.26 rational approximation (max error ≈ 1.5e-7).
pub(crate) fn std_normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf_approx(x / std::f64::consts::SQRT_2))
}

fn erf_approx(x: f64) -> f64 {
    let a = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let result = 1.0 - poly * (-a * a).exp();
    if x >= 0.0 { result } else { -result }
}
