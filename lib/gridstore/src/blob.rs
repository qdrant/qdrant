use zerocopy::{FromBytes, Immutable, IntoBytes};

pub trait Blob {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &[u8]) -> Self;
}

impl Blob for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

impl Blob for Vec<ecow::EcoString> {
    fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(self).expect("Failed to serialize Vec<ecow::EcoString>")
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).expect("Failed to deserialize Vec<ecow::EcoString>")
    }
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
