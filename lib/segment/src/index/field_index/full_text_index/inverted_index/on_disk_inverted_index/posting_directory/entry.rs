use super::super::types::{ALIGNMENT, PostingListHeader};
use super::super::varint::read_varint;

/// One decoded entry.
pub(super) struct Entry {
    pub(super) header: PostingListHeader,
    /// Size of the term's frequency data, when the directory records it.
    pub(super) frequencies_size: u64,
}

/// The header of a posting list at `offset` from its four stored fields.
pub(super) fn header_at(
    offset: u64,
    chunks_count: u32,
    remainder_count: u8,
    ids_data_bytes_count: u32,
    var_size_data_bytes_count: u32,
) -> PostingListHeader {
    let data_len = ids_data_bytes_count as usize + var_size_data_bytes_count as usize;
    PostingListHeader {
        offset,
        chunks_count,
        ids_data_bytes_count,
        alignment_bytes_count: (data_len.next_multiple_of(ALIGNMENT) - data_len) as u8,
        remainder_count,
        _reserved: [0; 2],
        var_size_data_bytes_count,
    }
}

/// Split the next entry off `rest`, as the header of a list at `offset`.
pub(super) fn next_entry(rest: &mut &[u8], offset: u64, with_frequencies: bool) -> Option<Entry> {
    let chunks_count = read_varint(rest)?;
    let remainder_count = u8::try_from(read_varint(rest)?).ok()?;
    let ids_data_bytes_count = read_varint(rest)?;
    let var_size_data_bytes_count = read_varint(rest)?;
    let frequencies_size = if with_frequencies {
        u64::from(read_varint(rest)?)
    } else {
        0
    };
    Some(Entry {
        header: header_at(
            offset,
            chunks_count,
            remainder_count,
            ids_data_bytes_count,
            var_size_data_bytes_count,
        ),
        frequencies_size,
    })
}
