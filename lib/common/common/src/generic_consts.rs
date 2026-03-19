//! Named consts to use as generic parameters.
//!
//! ```ignore
//! // Not obvious:
//! fn read<const IS_SEQUENTIAL: bool>(&self);
//! foo.read::<true>();
//!
//! // Better:
//! fn read<P: AccessPattern>(&self);
//! foo.read::<Sequential>();
//! ```
//!
//!
//! Ideally, we should use regular enums instead of traits and unit structs from
//! this module:
//! ```ignore
//! enum AccessPattern { Random, Sequential }
//! fn read<const P: AccessPattern>(&self);
//! foo.read::<AccessPattern::Sequential>();
//! ```
//! But until <https://github.com/rust-lang/rust/issues/95174> is resolved, we
//! have to use this workaround.

/// A hint describing the pattern in which the caller retrieves items by IDs.
/// Affects the performance. The implementation might enable read-ahead aka
/// pre-fetch for sequential access.
/// [`Random`] is like `MADV_RANDOM`. [`Sequential`] is like `MADV_SEQUENTIAL`.
pub trait AccessPattern: Copy {
    const IS_SEQUENTIAL: bool;
}

#[derive(Copy, Clone)]
pub struct Random;

#[derive(Copy, Clone)]
pub struct Sequential;

impl AccessPattern for Random {
    const IS_SEQUENTIAL: bool = false;
}

impl AccessPattern for Sequential {
    const IS_SEQUENTIAL: bool = true;
}
