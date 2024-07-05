mod parse;
mod v2;

use v2::JsonPathItem;
pub use v2::JsonPathV2 as JsonPath;

/// Create a new `JsonPath` from a string.
///
/// # Panics
///
/// Panics if the string is not a valid path. Thus, this function should only be used in tests.
#[cfg(feature = "testing")]
pub fn path(p: &str) -> JsonPath {
    p.parse().unwrap()
}
