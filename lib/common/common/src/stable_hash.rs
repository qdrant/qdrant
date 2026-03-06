use std::hash::{Hash, Hasher};

use bytemuck::TransparentWrapper;

/// A hashable type, like [`Hash`], but with a stable/portable implementation.
///
/// According to the [`Hash`] docs, its implementations for most standard
/// library types should not considered stable across platforms or compiler
/// versions. Neither we can rely on implementations for types from third-party
/// crates.
///
/// This trait is intended for hashes that should be stable across different
/// Qdrant versions.
pub trait StableHash {
    /// Feed this value into the hasher.
    ///
    /// Similar to [`Hash::hash()`], but accepts [`Hasher::write()`] as a
    /// closure. This difference prevents implementations of this trait from:
    /// 1. Reusing [`Hash`] implementations which might be not portable.
    /// 2. Using other [`Hasher`] methods which are non-portable. See
    ///    <https://docs.rs/siphasher/1.0.1/siphasher/index.html#note>.
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W);
}

impl StableHash for i32 {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        // WARN: endianess-dependent; keep for backward compatibility
        write(&self.to_ne_bytes());
    }
}

impl StableHash for u32 {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        // WARN: endianess-dependent; keep for backward compatibility
        write(&self.to_ne_bytes());
    }
}

impl StableHash for u64 {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        // WARN: endianess-dependent; keep for backward compatibility
        write(&self.to_ne_bytes());
    }
}

impl StableHash for usize {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        (*self as u64).stable_hash(write);
    }
}

impl<A: StableHash, B: StableHash> StableHash for (A, B) {
    fn stable_hash<W: FnMut(&[u8])>(&self, write: &mut W) {
        let (a, b) = self;
        a.stable_hash(write);
        b.stable_hash(write);
    }
}

/// Compatibility wrapper that allows to use [`StableHash`] implementation in
/// contexts where [`Hash`] is expected.
///
/// This wrapper should be used in accompaniment with a stable [`Hasher`]
/// implementation such as from the `siphasher` crate. Hashes produced by
/// [`std::hash::DefaultHasher`] should not be relied upon over releases.
#[derive(Copy, Clone, Eq, PartialEq, Debug, TransparentWrapper)]
#[repr(transparent)]
pub struct StableHashed<T: StableHash>(pub T);

impl<T: StableHash> Hash for StableHashed<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.stable_hash(&mut |bytes| state.write(bytes));
    }
}
