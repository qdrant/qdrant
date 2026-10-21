//! Writing and reading a posting directory file, see the [module docs](super).

use std::io::Write as _;
use std::path::Path;

use common::universal_io::UniversalWriteFs;
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes as _, IntoBytes as _};

use super::super::types::{PostingListHeader, PostingsHeader, ZerocopyPostingValue};
use super::super::varint::write_varint;
use super::PostingDirectory;
use super::compression::Compression;
use super::entry::{header_at, next_entry};
use super::header::{FLAG_TERM_FREQUENCIES, FORMAT_VERSION, Header, MAGIC};
use crate::common::operation_error::{OperationError, OperationResult};

/// Entries per block of the in-RAM index: a lookup decodes up to this many.
const ENTRIES_PER_BLOCK: u32 = 16;

/// Offset in `postings.dat` of the first posting list of an index of `count`
/// lists: right after the headers.
pub(super) fn data_start(count: usize) -> u64 {
    (size_of::<PostingsHeader>() + count * size_of::<PostingListHeader>()) as u64
}

/// Atomically replace the directory at `path` with `headers`, the headers of
/// a `postings.dat` written with values `V`, and `frequency_sizes`, the size
/// of each term's data in `term_frequencies.dat`, when the index scores.
///
/// Errors when the headers do not lie back to back from the first data offset,
/// or carry an alignment other than the one implied: the directory stores
/// neither, and would place the lists wrongly.
pub(in super::super) fn save<V: ZerocopyPostingValue>(
    fs: &impl UniversalWriteFs,
    path: &Path,
    headers: &[PostingListHeader],
    frequency_sizes: Option<&[u64]>,
) -> OperationResult<()> {
    let bytes = encode::<V>(headers, frequency_sizes)?;
    fs.atomic_save(path, &bytes)?;
    Ok(())
}

pub(super) fn encode<V: ZerocopyPostingValue>(
    headers: &[PostingListHeader],
    frequency_sizes: Option<&[u64]>,
) -> OperationResult<Vec<u8>> {
    if let Some(sizes) = frequency_sizes
        && sizes.len() != headers.len()
    {
        return Err(OperationError::service_error(format!(
            "{} term frequency lists for {} posting lists",
            sizes.len(),
            headers.len(),
        )));
    }
    let start = data_start(headers.len());
    let mut offset = start;
    let mut frequencies_len = 0u64;
    let mut body = Vec::new();
    for (token_id, header) in headers.iter().enumerate() {
        let implied = header_at(
            offset,
            header.chunks_count,
            header.remainder_count,
            header.ids_data_bytes_count,
            header.var_size_data_bytes_count,
        );
        if implied.offset != header.offset
            || implied.alignment_bytes_count != header.alignment_bytes_count
        {
            return Err(OperationError::service_error(format!(
                "posting list {token_id} does not follow the layout the posting directory \
                 implies: offset {} where {offset} was expected",
                header.offset,
            )));
        }
        write_varint(&mut body, header.chunks_count);
        write_varint(&mut body, u32::from(header.remainder_count));
        write_varint(&mut body, header.ids_data_bytes_count);
        write_varint(&mut body, header.var_size_data_bytes_count);
        if let Some(sizes) = frequency_sizes {
            let size = u32::try_from(sizes[token_id]).map_err(|_| {
                OperationError::service_error(format!(
                    "term frequencies of token {token_id} exceed 4 GiB",
                ))
            })?;
            write_varint(&mut body, size);
            frequencies_len += u64::from(size);
        }
        offset += header.posting_size::<V>() as u64;
    }
    // The in-RAM block index holds `u32` offsets into the entries.
    if u32::try_from(body.len()).is_err() {
        return Err(OperationError::service_error(format!(
            "posting directory of {} bytes exceeds the 4 GiB limit",
            body.len(),
        )));
    }

    let header = Header {
        magic: MAGIC,
        version: U32::new(FORMAT_VERSION),
        compression: U32::new(Compression::Zstd as u32),
        count: U64::new(headers.len() as u64),
        body_len: U64::new(body.len() as u64),
        data_start: U64::new(start),
        data_end: U64::new(offset),
        entries_per_block: U32::new(ENTRIES_PER_BLOCK),
        flags: U32::new(if frequency_sizes.is_some() {
            FLAG_TERM_FREQUENCIES
        } else {
            0
        }),
        frequencies_len: U64::new(frequencies_len),
    };
    let mut encoder =
        zstd::Encoder::new(header.as_bytes().to_vec(), zstd::DEFAULT_COMPRESSION_LEVEL)?;
    encoder.include_checksum(true)?;
    encoder.write_all(&body)?;
    Ok(encoder.finish()?)
}

/// Decode a directory and check it against the `postings.dat` it describes:
/// one written with values `V`, holding `count` posting lists and `file_len`
/// bytes. The frequency spans are checked against `term_frequencies.dat` by
/// whoever reads that file, see [`PostingDirectory::frequencies_len`].
///
/// `Err` carries why the bytes are not a directory of that file. The caller
/// falls back to the headers in `postings.dat`: reading posting lists at the
/// offsets of another file would answer with the wrong points.
pub(in super::super) fn read<V: ZerocopyPostingValue>(
    bytes: &[u8],
    count: usize,
    file_len: u64,
) -> Result<PostingDirectory, String> {
    let (header, payload) = Header::ref_from_prefix(bytes)
        .map_err(|_| format!("{} bytes is shorter than the header", bytes.len()))?;
    if header.magic != MAGIC {
        return Err("not a posting directory file".to_string());
    }
    if header.version.get() != FORMAT_VERSION {
        return Err(format!("unsupported version {}", header.version.get()));
    }
    let Some(compression) = Compression::from_id(header.compression.get()) else {
        return Err(format!("unknown compression {}", header.compression.get()));
    };
    if header.count.get() != count as u64 {
        return Err(format!(
            "it lists {} posting lists while the postings file holds {count}",
            header.count.get(),
        ));
    }
    let start = data_start(count);
    if header.data_start.get() != start || header.data_end.get() != file_len {
        return Err(format!(
            "it places the posting lists at {}..{} while the postings file holds them at \
             {start}..{file_len}",
            header.data_start.get(),
            header.data_end.get(),
        ));
    }
    let entries_per_block = header.entries_per_block.get() as usize;
    if entries_per_block == 0 {
        return Err("zero entries per block".to_string());
    }
    let flags = header.flags.get();
    if flags & !FLAG_TERM_FREQUENCIES != 0 {
        return Err(format!("unknown flags {flags:#x}"));
    }
    let with_frequencies = flags & FLAG_TERM_FREQUENCIES != 0;
    let body_len = u32::try_from(header.body_len.get())
        .map_err(|_| format!("body of {} bytes exceeds 4 GiB", header.body_len.get()))?
        as usize;

    let body = match compression {
        Compression::Zstd => {
            let body = zstd::bulk::decompress(payload, body_len).map_err(|err| err.to_string())?;
            if body.len() != body_len {
                return Err(format!(
                    "{} bytes of entries where the header announces {body_len}",
                    body.len(),
                ));
            }
            body
        }
    };

    // Walk every entry once: lookups decode without checking, and rely on the
    // offsets landing where `postings.dat` and the frequency data end.
    let blocks = count.div_ceil(entries_per_block);
    let mut block_body_offsets = Vec::with_capacity(blocks);
    let mut block_data_offsets = Vec::with_capacity(blocks);
    let mut block_frequency_offsets = Vec::with_capacity(if with_frequencies { blocks } else { 0 });
    let mut rest = body.as_slice();
    let mut offset = start;
    let mut frequencies_offset = 0u64;
    for token_id in 0..count {
        if token_id % entries_per_block == 0 {
            block_body_offsets.push((body.len() - rest.len()) as u32);
            block_data_offsets.push(offset);
            if with_frequencies {
                block_frequency_offsets.push(frequencies_offset);
            }
        }
        let entry = next_entry(&mut rest, offset, with_frequencies)
            .ok_or_else(|| format!("entry {token_id} is truncated or malformed"))?;
        offset = offset
            .checked_add(entry.header.posting_size::<V>() as u64)
            .filter(|&end| end <= file_len)
            .ok_or_else(|| format!("posting list {token_id} runs past the postings file"))?;
        frequencies_offset += entry.frequencies_size;
    }
    if !rest.is_empty() {
        return Err(format!("{} bytes past the last entry", rest.len()));
    }
    if offset != file_len {
        return Err(format!(
            "the posting lists end at {offset} while the postings file ends at {file_len}",
        ));
    }
    if with_frequencies && frequencies_offset != header.frequencies_len.get() {
        return Err(format!(
            "the term frequencies end at {frequencies_offset} where the header announces {}",
            header.frequencies_len.get(),
        ));
    }

    Ok(PostingDirectory {
        body,
        block_body_offsets,
        block_data_offsets,
        frequencies: with_frequencies.then_some((block_frequency_offsets, frequencies_offset)),
        count,
        entries_per_block,
    })
}
