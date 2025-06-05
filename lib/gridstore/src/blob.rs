use zerocopy::{FromBytes, Immutable, IntoBytes};

pub trait Blob {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Self;
}

macro_rules! impl_blob_vec_zerocopy {
    ($type:ty) => {
        impl Blob for Vec<$type>
        where
            $type: FromBytes + IntoBytes + Immutable,
        {
            fn to_bytes(&self) -> Vec<u8> {
                self.iter()
                    .flat_map(|item| item.as_bytes())
                    .copied()
                    .collect()
            }

            fn from_bytes(bytes: &[u8]) -> Self {
                assert!(
                    bytes.len().is_multiple_of(size_of::<$type>()),
                    "unexpected number of bytes for Vec<{}>",
                    stringify!($type),
                );
                bytes
                    .chunks(size_of::<$type>())
                    .map(|v| <$type>::read_from_bytes(v).expect("invalid chunk size for type T"))
                    .collect()
            }
        }
    };
}

impl_blob_vec_zerocopy!(i64);
impl_blob_vec_zerocopy!(u128);
impl_blob_vec_zerocopy!(f64);
