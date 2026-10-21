use zerocopy::IntoBytes as _;

use super::super::super::positions::Positions;
use super::entry::header_at;
use super::format::{data_start, encode, read};
use super::header::Header;
use super::*;

/// Headers laid out back to back, as `create_postings_file` writes them.
fn layout<V: ZerocopyPostingValue>(fields: &[(u32, u8, u32, u32)]) -> Vec<PostingListHeader> {
    let mut offset = data_start(fields.len());
    fields
        .iter()
        .map(|&(chunks, remainder, ids, var)| {
            let header = header_at(offset, chunks, remainder, ids, var);
            offset += header.posting_size::<V>() as u64;
            header
        })
        .collect()
}

fn end<V: ZerocopyPostingValue>(headers: &[PostingListHeader]) -> u64 {
    headers.last().map_or(data_start(0), |last| {
        last.offset + last.posting_size::<V>() as u64
    })
}

fn fields(count: usize) -> Vec<(u32, u8, u32, u32)> {
    (0..count)
        .map(|i| {
            let i = i as u32;
            (i % 7, (i % 128) as u8, (i * 37) % 4000, (i * 13) % 999)
        })
        .collect()
}

fn frequency_sizes(count: usize) -> Vec<u64> {
    (0..count as u64).map(|i| 5 + (i * 29) % 300).collect()
}

fn round_trip<V: ZerocopyPostingValue>(count: usize, with_frequencies: bool) {
    let headers = layout::<V>(&fields(count));
    let sizes = frequency_sizes(count);
    let sizes = with_frequencies.then_some(sizes.as_slice());
    let bytes = encode::<V>(&headers, sizes).unwrap();
    let directory = read::<V>(&bytes, count, end::<V>(&headers)).unwrap();
    let mut frequencies_offset = 0;
    for (token_id, expected) in headers.iter().enumerate() {
        let actual = directory.header::<V>(token_id as TokenId).unwrap();
        assert_eq!(actual.as_bytes(), expected.as_bytes(), "token {token_id}");
        let span = directory.frequency_span(token_id as TokenId);
        match sizes {
            None => assert!(span.is_none()),
            Some(sizes) => {
                let span = span.unwrap();
                assert_eq!(span.len, expected.posting_len());
                assert_eq!(span.offset, frequencies_offset, "token {token_id}");
                assert_eq!(span.size, sizes[token_id]);
                frequencies_offset += sizes[token_id];
            }
        }
    }
    assert_eq!(
        directory.frequencies_len(),
        sizes.map(|sizes| sizes.iter().sum::<u64>()),
    );
    assert!(directory.header::<V>(count as TokenId).is_none());
    assert!(directory.frequency_span(count as TokenId).is_none());
}

#[test]
fn answers_every_header_and_span_as_written() {
    for count in [0, 1, 15, 16, 17, 1000] {
        for with_frequencies in [false, true] {
            round_trip::<()>(count, with_frequencies);
            round_trip::<Positions>(count, with_frequencies);
        }
    }
}

#[test]
fn rejects_a_directory_of_another_postings_file() {
    let headers = layout::<()>(&fields(100));
    let bytes = encode::<()>(&headers, Some(&frequency_sizes(100))).unwrap();
    let file_len = end::<()>(&headers);
    assert!(read::<()>(&bytes, 100, file_len).is_ok());
    // Another number of lists, another file length, another value type.
    assert!(read::<()>(&bytes, 99, file_len).is_err());
    assert!(read::<()>(&bytes, 100, file_len + 4).is_err());
    assert!(read::<Positions>(&bytes, 100, file_len).is_err());
    // Truncated, padded or corrupted.
    for len in [0, 8, size_of::<Header>(), bytes.len() - 1] {
        assert!(
            read::<()>(&bytes[..len], 100, file_len).is_err(),
            "{len} bytes"
        );
    }
    let mut corrupted = bytes.clone();
    let last = corrupted.len() - 5;
    corrupted[last] ^= 1;
    assert!(read::<()>(&corrupted, 100, file_len).is_err());
    // A frequency total that disagrees with the entries.
    let mut recounted = bytes;
    recounted[std::mem::offset_of!(Header, frequencies_len)] ^= 1;
    assert!(read::<()>(&recounted, 100, file_len).is_err());
}

#[test]
fn refuses_headers_it_cannot_reproduce() {
    let mut headers = layout::<()>(&fields(10));
    headers[3].offset += 4;
    assert!(encode::<()>(&headers, None).is_err());
    let mut headers = layout::<()>(&fields(10));
    headers[5].alignment_bytes_count = (headers[5].alignment_bytes_count + 1) % 4;
    assert!(encode::<()>(&headers, None).is_err());
    // One frequency list per posting list.
    let headers = layout::<()>(&fields(10));
    assert!(encode::<()>(&headers, Some(&frequency_sizes(9))).is_err());
}
