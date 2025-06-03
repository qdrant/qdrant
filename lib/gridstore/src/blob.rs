use zerocopy::{FromBytes, Immutable, IntoBytes};

pub trait Blob {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Self;
}

/// Blob with a fixed in-memory and serialized size.
///
/// # Safety
///
/// Must ONLY be implemented for types that have the exact same size in memory and when serialized.
pub trait BlobFixedSize: Blob + Sized {
    #[inline]
    fn size() -> usize {
        size_of::<Self>()
    }
}

/// All `zerocopy` types are guaranteed to have the same size in memory and when serialized
impl<T> BlobFixedSize for T where T: Blob + FromBytes + IntoBytes + Immutable + Copy {}

macro_rules! impl_blob_le_bytes {
    ($type:ty) => {
        impl Blob for $type {
            fn to_bytes(&self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
            fn from_bytes(data: &[u8]) -> Self {
                match data.try_into() {
                    Ok(data) => <$type>::from_le_bytes(data),
                    Err(_) => panic!("slice with incorrect length for {}", stringify!($type)),
                }
            }
        }
    };
}

impl_blob_le_bytes!(i64);
impl_blob_le_bytes!(f64);
impl_blob_le_bytes!(u128);

impl<T> Blob for Vec<T>
where
    T: BlobFixedSize + Copy,
{
    fn to_bytes(&self) -> Vec<u8> {
        self.iter()
            .flat_map(|item| {
                let bytes = item.to_bytes();
                debug_assert_eq!(T::size(), bytes.len(), "unexpected size for T");
                bytes
            })
            .collect()
    }

    fn from_bytes(data: &[u8]) -> Self {
        assert!(
            data.len().is_multiple_of(T::size()),
            "unexpected number of bytes for Vec<T>",
        );
        data.chunks(T::size()).map(|v| T::from_bytes(v)).collect()
    }
}
