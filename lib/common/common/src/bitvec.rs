use bitvec::slice::BitSlice as RawBitSlice;
pub use bitvec::store::BitStore;
pub use bitvec::vec::BitVec as RawBitVec;

// BitVec type we use across the codebase. Has consistent bit layout, suitable for persisting.
pub type BitSliceElement = u64;
pub type BitSlice = RawBitSlice<BitSliceElement, bitvec::order::Lsb0>;
pub type BitVec = RawBitVec<BitSliceElement, bitvec::order::Lsb0>;

// BitVec type we use for in-memory bit vectors. Can be more efficient than BitVec.
pub type MemoryBitVec = bitvec::vec::BitVec;
