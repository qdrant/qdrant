//! Writing and reading a compact vocabulary file, see the [module docs](super).

use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;

use common::universal_io::UniversalWriteFs;
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes as _, IntoBytes as _};

use super::super::varint::{read_varint, write_varint};
use super::CompactVocab;
use super::compression::Compression;
use super::header::{FORMAT_VERSION, Header, MAGIC};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::full_text_index::inverted_index::TokenId;

/// Tokens per front-coded block. A lookup decodes up to this many tokens after
/// the binary search, and the block index costs 4 bytes of RAM per block.
pub(super) const KEYS_PER_BLOCK: u32 = 16;

/// Atomically replace the file at `path` with `vocab`.
///
/// Errors when the ids of `vocab` are not the ranks of its tokens in byte
/// order, since the file stores no ids.
pub(in super::super) fn save(
    fs: &impl UniversalWriteFs,
    path: &Path,
    vocab: &HashMap<String, TokenId>,
) -> OperationResult<()> {
    let mut entries: Vec<(&str, TokenId)> = vocab.iter().map(|(k, &v)| (k.as_str(), v)).collect();
    entries.sort_unstable();
    if let Some((rank, (token, id))) = (0..).zip(&entries).find(|&(rank, &(_, id))| id != rank) {
        return Err(OperationError::service_error(format!(
            "compact vocabulary needs ids in token order: {token:?} has id {id}, rank {rank}",
        )));
    }
    let bytes = encode(entries.iter().map(|&(token, _)| token), entries.len())?;
    fs.atomic_save(path, &bytes)?;
    Ok(())
}

/// Encode `count` tokens, which must be strictly ascending.
pub(super) fn encode<'a>(
    tokens: impl Iterator<Item = &'a str>,
    count: usize,
) -> OperationResult<Vec<u8>> {
    let body = encode_body(tokens, KEYS_PER_BLOCK);
    // The block index of the loaded vocabulary holds `u32` offsets.
    if u32::try_from(body.len()).is_err() {
        return Err(OperationError::service_error(format!(
            "compact vocabulary of {} bytes exceeds the 4 GiB limit",
            body.len(),
        )));
    }

    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        compression: U32::new(Compression::Zstd as u32),
        count: U64::new(count as u64),
        body_len: U64::new(body.len() as u64),
        keys_per_block: U32::new(KEYS_PER_BLOCK),
        reserved: [0; 28],
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    encoder.write_all(&body)?;
    Ok(encoder.finish()?)
}

/// The front-coded body of strictly ascending `tokens`, see the [module docs](super).
fn encode_body<'a>(tokens: impl Iterator<Item = &'a str>, keys_per_block: u32) -> Vec<u8> {
    let mut body = Vec::new();
    let mut previous: &[u8] = &[];
    for (index, token) in tokens.enumerate() {
        let token = token.as_bytes();
        debug_assert!(
            index == 0 || previous < token,
            "tokens must be strictly ascending"
        );
        let shared = if index % keys_per_block as usize == 0 {
            0
        } else {
            previous
                .iter()
                .zip(token)
                .take_while(|(a, b)| a == b)
                .count()
        };
        let suffix = &token[shared..];
        write_varint(&mut body, shared as u32);
        write_varint(&mut body, suffix.len() as u32);
        body.extend_from_slice(suffix);
        previous = token;
    }
    body
}

/// Decode a file into the vocabulary it holds.
///
/// `Err` carries why the bytes are not a vocabulary this index can use: another
/// format, another version, or a truncated, padded or corrupted file. Unlike
/// the document length sidecar, the vocabulary cannot be done without, so the
/// caller fails the open.
pub(in super::super) fn read(bytes: &[u8]) -> Result<CompactVocab, String> {
    let (header, payload) = Header::ref_from_prefix(bytes)
        .map_err(|_| format!("{} bytes is shorter than the header", bytes.len()))?;
    if header.magic != MAGIC {
        return Err("not a compact vocabulary file".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }
    let Some(compression) = Compression::from_id(header.compression.get()) else {
        return Err(format!("unknown compression {}", header.compression.get()));
    };
    let keys_per_block = header.keys_per_block.get() as usize;
    if keys_per_block == 0 {
        return Err("zero tokens per block".to_string());
    }
    let count = usize::try_from(header.count.get()).map_err(|err| err.to_string())?;
    let body_len = u32::try_from(header.body_len.get())
        .map_err(|_| format!("body of {} bytes exceeds 4 GiB", header.body_len.get()))?
        as usize;

    let body = match compression {
        Compression::Zstd => {
            // Decompressed into exactly the room the header announces, so a
            // frame holding more fails before it can allocate for it.
            let body = zstd::bulk::decompress(payload, body_len).map_err(|err| err.to_string())?;
            if body.len() != body_len {
                return Err(format!(
                    "{} bytes of tokens where the header announces {body_len}",
                    body.len(),
                ));
            }
            body
        }
    };
    CompactVocab::from_body(body, count, keys_per_block)
}

/// Decode the next front-coded token from `rest` into `key`, which holds the
/// previous token of the block. Returns the shared prefix length, `None` if
/// the bytes are malformed.
pub(super) fn next_key(rest: &mut &[u8], key: &mut Vec<u8>) -> Option<usize> {
    let (shared, suffix) = next_entry(rest)?;
    if shared > key.len() {
        return None;
    }
    key.truncate(shared);
    key.extend_from_slice(suffix);
    Some(shared)
}

/// Split the next `(shared_prefix_len, suffix)` entry off `rest`.
pub(super) fn next_entry<'a>(rest: &mut &'a [u8]) -> Option<(usize, &'a [u8])> {
    let shared = read_varint(rest)? as usize;
    let suffix_len = read_varint(rest)? as usize;
    let (suffix, tail) = rest.split_at_checked(suffix_len)?;
    *rest = tail;
    Some((shared, suffix))
}
