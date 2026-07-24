use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::error::GridstoreError;

pub trait Blob: Sized {
    fn to_bytes(&self) -> Vec<u8>;

    /// Deserialize `Self` from a previously-serialized blob.
    ///
    /// On-disk blobs can be torn or corrupted by a hard crash mid-flush, so this must not
    /// panic on malformed input — callers rely on the error to trigger index rebuild instead
    /// of crash-looping.
    fn from_bytes(bytes: &[u8]) -> Result<Self, GridstoreError>;
}

impl Blob for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, GridstoreError> {
        Ok(bytes.to_vec())
    }
}

impl Blob for Vec<ecow::EcoString> {
    fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(self).expect("Failed to serialize Vec<ecow::EcoString>")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, GridstoreError> {
        serde_cbor::from_slice(bytes).map_err(|err| {
            GridstoreError::service_error(format!(
                "Failed to deserialize Vec<ecow::EcoString>: {err}"
            ))
        })
    }
}

impl Blob for Vec<(f64, f64)> {
    fn to_bytes(&self) -> Vec<u8> {
        self.iter()
            .flat_map(|(a, b)| a.to_le_bytes().into_iter().chain(b.to_le_bytes()))
            .collect()
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, GridstoreError> {
        if !bytes.len().is_multiple_of(size_of::<f64>() * 2) {
            return Err(GridstoreError::service_error(format!(
                "unexpected number of bytes for Vec<(f64, f64)>: {}",
                bytes.len(),
            )));
        }
        bytes
            .chunks(size_of::<f64>() * 2)
            .map(|v| {
                let (a, b) = v.split_at(size_of::<f64>());
                let a = f64::read_from_bytes(a).map_err(|err| {
                    GridstoreError::service_error(format!(
                        "invalid number of bytes for type f64: {err}"
                    ))
                })?;
                let b = f64::read_from_bytes(b).map_err(|err| {
                    GridstoreError::service_error(format!(
                        "invalid number of bytes for type f64: {err}"
                    ))
                })?;
                Ok((a, b))
            })
            .collect()
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

            fn from_bytes(bytes: &[u8]) -> Result<Self, GridstoreError> {
                if !bytes.len().is_multiple_of(size_of::<$type>()) {
                    return Err(GridstoreError::service_error(format!(
                        "unexpected number of bytes for Vec<{}>: {}",
                        stringify!($type),
                        bytes.len(),
                    )));
                }
                bytes
                    .chunks(size_of::<$type>())
                    .map(|v| {
                        <$type>::read_from_bytes(v).map_err(|err| {
                            GridstoreError::service_error(format!(
                                "invalid chunk for type {}: {err}",
                                stringify!($type),
                            ))
                        })
                    })
                    .collect()
            }
        }
    };
}

impl_blob_vec_zerocopy!(i64);
impl_blob_vec_zerocopy!(u128);
impl_blob_vec_zerocopy!(f64);

#[cfg(test)]
mod tests {
    use super::*;

    /// Reproduces the crash from a torn/corrupted Gridstore blob after a hard crash
    /// mid-flush (see qdrant#9857): deserializing a malformed or truncated blob must
    /// return an error so the caller can trigger an index rebuild, not panic and
    /// crash-loop the whole node.
    #[test]
    fn from_bytes_reports_error_on_malformed_ecostring_blob() {
        // A bare CBOR-encoded integer, not the expected sequence: mirrors the
        // "invalid type: integer `0`, expected a sequence" panic from the issue.
        let malformed = [0x00u8];
        assert!(Vec::<ecow::EcoString>::from_bytes(&malformed).is_err());
    }

    #[test]
    fn from_bytes_reports_error_on_truncated_ecostring_blob() {
        let value = vec![
            ecow::EcoString::from("hello"),
            ecow::EcoString::from("world"),
        ];
        let bytes = value.to_bytes();
        // Truncate mid-value: mirrors the "EofWhileParsingValue" panic from the issue.
        let truncated = &bytes[..bytes.len() - 1];
        assert!(Vec::<ecow::EcoString>::from_bytes(truncated).is_err());
    }

    #[test]
    fn from_bytes_roundtrips_ecostring_blob() {
        let value = vec![
            ecow::EcoString::from("hello"),
            ecow::EcoString::from("world"),
        ];
        let bytes = value.to_bytes();
        assert_eq!(Vec::<ecow::EcoString>::from_bytes(&bytes).unwrap(), value);
    }

    #[test]
    fn from_bytes_reports_error_on_wrong_length_numeric_blob() {
        assert!(Vec::<i64>::from_bytes(&[0u8; 3]).is_err());
        assert!(Vec::<f64>::from_bytes(&[0u8; 3]).is_err());
        assert!(Vec::<(f64, f64)>::from_bytes(&[0u8; 3]).is_err());
    }

    #[test]
    fn from_bytes_roundtrips_numeric_blobs() {
        let ints = vec![1i64, -2, 3];
        assert_eq!(Vec::<i64>::from_bytes(&ints.to_bytes()).unwrap(), ints);

        let floats = vec![1.5f64, -2.5, 3.5];
        assert_eq!(Vec::<f64>::from_bytes(&floats.to_bytes()).unwrap(), floats);

        let pairs = vec![(1.0f64, 2.0f64), (3.0, 4.0)];
        assert_eq!(
            Vec::<(f64, f64)>::from_bytes(&pairs.to_bytes()).unwrap(),
            pairs
        );
    }
}
