//! Traits that should belong to the [`num-traits`] crate, but are missing.
//!
//! [`num-traits`]: https://crates.io/crates/num-traits

use std::num::{NonZero, Saturating};

pub trait ConstBits {
    /// The size of this integer type in bits.
    const BITS: u32;
}

macro_rules! impl_const_bits {
    ($($t:ty),* $(,)?) => {
        $(
            impl ConstBits for $t {
                const BITS: u32 = Self::BITS;
            }
            impl ConstBits for NonZero<$t> {
                const BITS: u32 = Self::BITS;
            }
            impl ConstBits for Saturating<$t> {
                const BITS: u32 = Self::BITS;
            }
        )*
    };
}

impl_const_bits!(i8, i16, i32, i64, i128, isize);
impl_const_bits!(u8, u16, u32, u64, u128, usize);
