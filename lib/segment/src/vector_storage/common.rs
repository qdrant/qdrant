use std::sync::atomic::{AtomicBool, Ordering};

use common::prefetch::{
    MAX_UNPREFETCHED_BATCH, MIN_PREFETCH_STORAGE_BYTES, prefetch_slice, prefetch_slice_l2,
    prefetch_windows,
};

use crate::common::operation_error::OperationError;

static ASYNC_SCORER: AtomicBool = AtomicBool::new(false);

pub fn set_async_scorer(async_scorer: bool) {
    ASYNC_SCORER.store(async_scorer, Ordering::Relaxed);
}

pub fn get_async_scorer() -> bool {
    ASYNC_SCORER.load(Ordering::Relaxed)
}

/// Minimal number of bytes we read from disk in one go
/// WARN: this might be system dependent, so we assume 4Kb, which might be wrong
/// ToDo: read this from system
pub const PAGE_SIZE_BYTES: usize = 4096;

/// Number of vectors we read from storage in one batch
/// in case we need to score an iterator of vector ids
pub const VECTOR_READ_BATCH_SIZE: usize = 64;

#[cfg(any(test, feature = "testing"))]
pub const CHUNK_SIZE: usize = 512 * 1024;

/// Vector storage chunk size in bytes
#[cfg(not(any(test, feature = "testing")))]
pub const CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Call `f(index, vector)` for each of `vectors` in order, prefetching the
/// vectors ahead of it.
///
/// The dense storages collect a batch of borrowed vectors before scoring it,
/// which reads no vector data, so every line was a demand miss once the scorer
/// reached it. The windows and the cases that go without hints are the
/// quantized storages' (`common::prefetch`): tiny batches, storages small
/// enough to stay cache resident, and batches the access pattern already
/// streams (`sequential`).
pub fn for_each_with_prefetch<V>(
    vectors: &[V],
    sequential: bool,
    storage_bytes: usize,
    as_bytes: impl Fn(&V) -> &[u8],
    mut f: impl FnMut(usize, &V),
) {
    if sequential
        || vectors.len() <= MAX_UNPREFETCHED_BATCH
        || storage_bytes < MIN_PREFETCH_STORAGE_BYTES
    {
        for (index, vector) in vectors.iter().enumerate() {
            f(index, vector);
        }
        return;
    }

    // Warm-up fills the initial windows: the first `near` vectors go straight
    // to L1, the rest of the far window to L2.
    let (near, far) = prefetch_windows(as_bytes(&vectors[0]).len());
    for vector in vectors.iter().take(far).skip(near) {
        prefetch_slice_l2(as_bytes(vector));
    }
    for vector in vectors.iter().take(near) {
        prefetch_slice(as_bytes(vector));
    }

    for (index, vector) in vectors.iter().enumerate() {
        if far > 0
            && let Some(upcoming) = vectors.get(index + far)
        {
            prefetch_slice_l2(as_bytes(upcoming));
        }
        if let Some(upcoming) = vectors.get(index + near) {
            prefetch_slice(as_bytes(upcoming));
        }
        f(index, vector);
    }
}

pub fn error_immutable_insert() -> OperationError {
    OperationError::service_error("Cannot insert into an immutable vector storage")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Prefetching is a hint: every vector reaches `f` once, in order, with
    /// its own index, whether or not hints were issued.
    #[test]
    fn test_for_each_with_prefetch_visits_every_vector_in_order() {
        let vectors: Vec<Vec<u8>> = (0..VECTOR_READ_BATCH_SIZE as u8)
            .map(|i| vec![i; 512])
            .collect();
        let cases = [
            // Prefetched: random, large storage, a full batch.
            (false, MIN_PREFETCH_STORAGE_BYTES, vectors.len()),
            // Skipped: sequential, cache-resident storage, tiny batch.
            (true, MIN_PREFETCH_STORAGE_BYTES, vectors.len()),
            (false, MIN_PREFETCH_STORAGE_BYTES - 1, vectors.len()),
            (false, MIN_PREFETCH_STORAGE_BYTES, MAX_UNPREFETCHED_BATCH),
            // Windows wider than the batch.
            (
                false,
                MIN_PREFETCH_STORAGE_BYTES,
                MAX_UNPREFETCHED_BATCH + 1,
            ),
        ];
        for (sequential, storage_bytes, len) in cases {
            let mut seen = Vec::new();
            for_each_with_prefetch(
                &vectors[..len],
                sequential,
                storage_bytes,
                |v| v.as_slice(),
                |index, v| seen.push((index, v[0])),
            );
            let want: Vec<_> = (0..len).map(|i| (i, i as u8)).collect();
            assert_eq!(
                seen, want,
                "sequential={sequential} storage_bytes={storage_bytes} len={len}"
            );
        }
    }

    #[test]
    fn test_for_each_with_prefetch_handles_small_vectors() {
        // Sub-cache-line vectors get no far window (`prefetch_windows`).
        let vectors: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 8]).collect();
        let mut count = 0;
        for_each_with_prefetch(
            &vectors,
            false,
            usize::MAX,
            |v| v.as_slice(),
            |_, _| count += 1,
        );
        assert_eq!(count, vectors.len());
    }
}
