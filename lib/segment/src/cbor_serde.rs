//! This module encapsulate all the CBOR serde logic.

use std::io::{Read, Write};
use std::vec::Vec;

use serde::{de, ser};

#[derive(Debug)]
pub struct Error {
    pub description: String,
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Error {
            description: format!("CBOR serde error: {:?}", err),
        }
    }
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Error {
            description: format!("CBOR serde error: {:?}", err),
        }
    }
}

/// Serializes as CBOR into a [Writer](std::io::Write)
#[inline]
pub fn into_writer<T: ?Sized + ser::Serialize, W: Write>(
    value: &T,
    writer: W,
) -> Result<(), Error> {
    ciborium::ser::into_writer(value, writer)?;
    Ok(())
}

/// Deserializes as CBOR from a [Reader](std::io::Read)
#[inline]
pub fn from_reader<'de, T: de::Deserialize<'de>, R: Read>(reader: R) -> Result<T, Error> {
    let result = ciborium::de::from_reader(reader)?;
    Ok(result)
}

/// Serializes as CBOR into a [Vec](std::vec::Vec)
pub fn into_vec<T: ?Sized + ser::Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let mut vec = Vec::new();
    ciborium::ser::into_writer(value, &mut vec)?;
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cbor_serde_into_vec_rountrip() {
        let expected_values = vec![5, 604, 34];
        let bytes = into_vec(&expected_values).unwrap();
        let actual_values: Vec<i32> = from_reader(bytes.as_slice()).unwrap();
        assert_eq!(actual_values.len(), 3);

        for (expected, actual) in expected_values.iter().zip(actual_values.iter()) {
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_cbor_serde_into_writer_roundtrip() {
        let expected_values = vec![5, 604, 34];
        let mut bytes: Vec<u8> = Vec::new();
        into_writer(&expected_values, &mut bytes).unwrap();
        let actual_values: Vec<i32> = from_reader(bytes.as_slice()).unwrap();
        assert_eq!(actual_values.len(), 3);

        for (expected, actual) in expected_values.iter().zip(actual_values.iter()) {
            assert_eq!(expected, actual);
        }
    }
}
