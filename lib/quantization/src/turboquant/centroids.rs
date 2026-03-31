//! Precomputed Lloyd-Max optimal centroids for a standard normal distribution.
//!
//! These are MSE-optimal scalar quantizer centroids for N(0,1), which is the
//! high-dimensional limit of the Beta distribution after random rotation.

/// 1-bit (2 centroids): ±sqrt(2/π)
const CENTROIDS_1BIT: &[f32] = &[-0.797_884_6, 0.797_884_6];

/// 2-bit (4 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_2BIT: &[f32] = &[-1.510, -0.4528, 0.4528, 1.510];

/// 3-bit (8 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_3BIT: &[f32] = &[
    -2.152, -1.344, -0.7560, -0.2451, 0.2451, 0.7560, 1.344, 2.152,
];

/// 4-bit (16 centroids): Lloyd-Max for N(0,1)
const CENTROIDS_4BIT: &[f32] = &[
    -2.733, -2.069, -1.618, -1.256, -0.9424, -0.6568, -0.3881, -0.1284, 0.1284, 0.3881, 0.6568,
    0.9424, 1.256, 1.618, 2.069, 2.733,
];

/// Return the centroid slice for a given bit-width (1–4).
///
/// # Panics
/// Panics if `bits` is not in 1..=4.
pub fn get_centroids(bits: u8) -> &'static [f32] {
    match bits {
        1 => CENTROIDS_1BIT,
        2 => CENTROIDS_2BIT,
        3 => CENTROIDS_3BIT,
        4 => CENTROIDS_4BIT,
        _ => panic!("unsupported bit-width: {bits}"),
    }
}
