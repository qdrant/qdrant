use std::sync::atomic::AtomicUsize;

pub mod prelude {
    pub use bitvec::prelude::*;

    pub use super::{BitSlice, BitVec};
}

pub type BitVec = bitvec::prelude::BitVec<AtomicUsize>;
pub type BitSlice = bitvec::prelude::BitSlice<AtomicUsize>;
