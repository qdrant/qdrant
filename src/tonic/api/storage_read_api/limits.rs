//! Request limits for the unary `StorageRead` reads.
//!
//! `ReadBytes`, `ReadWhole` and `ReadBatch` hold the full response in memory before sending it.
//! These limits bound that allocation, so that a small request cannot make the server allocate
//! an arbitrary amount of memory. Larger reads must use the streaming `ReadBytesStream` RPC.

use common::universal_io::ReadRange;
use tonic::Status;

/// Maximum number of bytes a single unary read request may return.
pub const MAX_READ_BYTES_PER_REQUEST: u64 = 128 * 1024 * 1024;

/// Maximum number of ranges in a single `ReadBatch` request.
pub const MAX_READ_BATCH_RANGES: usize = 1024;

/// Check the length of a `ReadBytes` request against the per-request byte limit.
pub fn check_read_bytes_length(length: u64) -> Result<(), Status> {
    if length > MAX_READ_BYTES_PER_REQUEST {
        return Err(Status::resource_exhausted(format!(
            "Requested {length} bytes, exceeding the limit of {MAX_READ_BYTES_PER_REQUEST} bytes \
             per request, use ReadBytesStream for larger reads",
        )));
    }
    Ok(())
}

/// Check the file length for a `ReadWhole` request against the per-request byte limit.
pub fn check_read_whole_length(file_length: u64) -> Result<(), Status> {
    if file_length > MAX_READ_BYTES_PER_REQUEST {
        return Err(Status::resource_exhausted(format!(
            "File is {file_length} bytes, exceeding the limit of {MAX_READ_BYTES_PER_REQUEST} \
             bytes per request, use ReadBytesStream for larger files",
        )));
    }
    Ok(())
}

/// Validate the ranges of a `ReadBatch` request.
///
/// Rejects requests with too many ranges or too many bytes in total, and requests with
/// duplicate or overlapping ranges. Without the latter, a client could repeat one range many
/// times to amplify the response size.
pub fn check_read_batch_ranges(ranges: &[ReadRange]) -> Result<(), Status> {
    if ranges.len() > MAX_READ_BATCH_RANGES {
        return Err(Status::resource_exhausted(format!(
            "Requested {} ranges, exceeding the limit of {MAX_READ_BATCH_RANGES} ranges per request",
            ranges.len(),
        )));
    }

    let total_length = ranges
        .iter()
        .try_fold(0u64, |total, range| total.checked_add(range.length));
    if total_length.is_none_or(|total| total > MAX_READ_BYTES_PER_REQUEST) {
        return Err(Status::resource_exhausted(format!(
            "Requested ranges exceed the limit of {MAX_READ_BYTES_PER_REQUEST} bytes per request, \
             use ReadBytesStream for larger reads",
        )));
    }

    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|range| (range.byte_offset, range.length));

    // Ranges are sorted by offset, so a range overlaps an earlier one if it starts before the
    // furthest end seen so far. That misses repeated empty ranges, so check duplicates as well.
    let mut previous = None;
    let mut furthest_end = 0;
    for range in sorted {
        let range_end = range
            .byte_offset
            .checked_add(range.length)
            .ok_or_else(|| Status::invalid_argument("Range end overflows u64"))?;
        if previous == Some(range) {
            return Err(Status::invalid_argument(format!(
                "Duplicate range {}..{range_end}",
                range.byte_offset,
            )));
        }
        if range.byte_offset < furthest_end {
            return Err(Status::invalid_argument(format!(
                "Range {}..{range_end} overlaps with another range",
                range.byte_offset,
            )));
        }
        furthest_end = furthest_end.max(range_end);
        previous = Some(range);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    fn ranges(ranges: &[(u64, u64)]) -> Vec<ReadRange> {
        ranges
            .iter()
            .map(|&(byte_offset, length)| ReadRange::new(byte_offset, length))
            .collect()
    }

    #[test]
    fn read_batch_accepts_disjoint_and_adjacent_ranges() {
        check_read_batch_ranges(&ranges(&[(10, 5), (0, 2), (2, 3), (15, 0), (100, 1)])).unwrap();
    }

    #[test]
    fn read_batch_rejects_duplicate_ranges() {
        for duplicated in [(4, 2), (4, 0)] {
            let err =
                check_read_batch_ranges(&ranges(&[duplicated, (10, 1), duplicated])).unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{duplicated:?}");
            assert!(err.message().contains("Duplicate"), "{}", err.message());
        }
    }

    #[test]
    fn read_batch_rejects_overlapping_ranges() {
        for overlapping in [
            [(0, 10), (9, 2)],
            [(0, 10), (0, 5)],
            [(5, 1), (0, 10)],
            [(0, 10), (5, 0)],
        ] {
            let err = check_read_batch_ranges(&ranges(&overlapping)).unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "{overlapping:?}");
            assert!(err.message().contains("overlaps"), "{}", err.message());
        }
    }

    #[test]
    fn read_batch_rejects_overlap_with_earlier_long_range() {
        // The overlap is with the first range, not with the one directly before it.
        let err = check_read_batch_ranges(&ranges(&[(0, 100), (10, 5), (50, 5)])).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn read_batch_rejects_too_many_ranges() {
        let at_limit = (0..MAX_READ_BATCH_RANGES as u64)
            .map(|i| ReadRange::new(i, 1))
            .collect::<Vec<_>>();
        check_read_batch_ranges(&at_limit).unwrap();

        let over_limit = (0..=MAX_READ_BATCH_RANGES as u64)
            .map(|i| ReadRange::new(i, 1))
            .collect::<Vec<_>>();
        let err = check_read_batch_ranges(&over_limit).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn read_batch_rejects_too_many_bytes() {
        let half = MAX_READ_BYTES_PER_REQUEST / 2;
        check_read_batch_ranges(&ranges(&[(0, half), (half, half)])).unwrap();

        let err = check_read_batch_ranges(&ranges(&[(0, half), (half, half + 1)])).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn read_batch_rejects_total_length_overflow() {
        let err = check_read_batch_ranges(&ranges(&[(0, u64::MAX), (u64::MAX, 1)])).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn read_bytes_length_limit() {
        check_read_bytes_length(MAX_READ_BYTES_PER_REQUEST).unwrap();
        let err = check_read_bytes_length(MAX_READ_BYTES_PER_REQUEST + 1).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn read_whole_length_limit() {
        check_read_whole_length(MAX_READ_BYTES_PER_REQUEST).unwrap();
        let err = check_read_whole_length(MAX_READ_BYTES_PER_REQUEST + 1).unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
        assert!(
            err.message().contains("ReadBytesStream"),
            "{}",
            err.message()
        );
    }
}
