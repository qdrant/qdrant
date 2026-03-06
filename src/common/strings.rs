/// Constant-time equality for String types
#[inline]
pub fn ct_eq(lhs: impl AsRef<str>, rhs: impl AsRef<str>) -> bool {
    constant_time_eq::constant_time_eq(lhs.as_ref().as_bytes(), rhs.as_ref().as_bytes())
}
