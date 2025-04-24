use std::cmp::Ordering;

/// Returns the ordering between two slices of `f32` values.
///
/// The order is arbitrary but consistent.
pub fn total_cmp_f32_slices(a: &[f32], b: &[f32]) -> Ordering {
    if a.len() != b.len() {
        return a.len().cmp(&b.len());
    }

    for (a, b) in a.iter().zip(b.iter()) {
        match Ord::cmp(&a.to_bits(), &b.to_bits()) {
            Ordering::Equal => (),
            cmp => return cmp,
        }
    }

    Ordering::Equal
}
