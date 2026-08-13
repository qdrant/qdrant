//! Serde helpers for raw byte blobs.
//!
//! By default serde serializes `Vec<u8>` as a sequence of integers, which CBOR (used for
//! the WAL) writes as one item per byte. The helpers here force blobs through
//! `serialize_bytes` so they become a single byte string instead.

use std::fmt;

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// Upper bound for the capacity we pre-allocate from an untrusted `size_hint` when deserializing a single raw blob.
/// This is *not* a hard size limit.
const MAX_RAW_BLOB_PREALLOC: usize = 128 * 1024 * 1024;

/// Reference wrapper that serializes a byte slice as a byte string.
pub struct BytesRef<'a>(pub &'a [u8]);

impl serde::Serialize for BytesRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// Owned wrapper that deserializes a byte string into a `Vec<u8>`.
pub struct ByteVec(pub Vec<u8>);

impl<'de> serde::Deserialize<'de> for ByteVec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByteVecVisitor;

        impl<'de> Visitor<'de> for ByteVecVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a byte string")
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(value.to_vec())
            }

            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(value)
            }

            /// Formats that lack a native byte-string type (e.g. JSON) fall
            /// back to a sequence of integers; accept those too.
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let capacity = seq.size_hint().unwrap_or(0).min(MAX_RAW_BLOB_PREALLOC);
                let mut bytes = Vec::with_capacity(capacity);
                while let Some(byte) = seq.next_element()? {
                    bytes.push(byte);
                }
                Ok(bytes)
            }
        }

        deserializer
            .deserialize_byte_buf(ByteVecVisitor)
            .map(ByteVec)
    }
}

/// `#[serde(with = "...")]` entry point for a plain `Vec<u8>` field.
pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    BytesRef(bytes).serialize(serializer)
}

/// `#[serde(with = "...")]` entry point for a plain `Vec<u8>` field.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    ByteVec::deserialize(deserializer).map(|bytes| bytes.0)
}
