//! Shared lazy read core over the [on-disk format](super::on_disk_format)
//! mapping files (`i2e` + `e2i` + `is_uuid`).
//!
//! Holds the two headers, the e2i sparse block index, and the whole `is_uuid`
//! bitmap in RAM; every lookup reads at most one data block through the
//! backing [`UniversalRead`] handle. Both the writable
//! [`DiskIdTracker`](super::DiskIdTracker) and the read-only
//! [`ReadOnlyDiskIdTracker`](super::read_only::ReadOnlyDiskIdTracker)
//! embed this and layer their own deletion/version handling on top.
//!
//! Deletion is deliberately NOT applied here: `lookup`/`external_id`/`iter_from`
//! return build-time-live entries, and each tracker filters with its own deleted
//! source (a resident bitvec for the writable tracker, a lazy `get_bit` for the
//! reader).

mod iter;
mod lifecycle;
mod lookup;

use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;

pub use self::iter::iter_random;
use super::on_disk_format::{E2iHeader, I2eHeader};

/// Lazy read core over the `i2e`/`e2i` files.
#[derive(Debug)]
pub struct DiskMappingReader<S: UniversalRead> {
    i2e: S,
    e2i: S,
    i2e_header: I2eHeader,
    e2i_header: E2iHeader,
    /// First numeric key of every numeric block.
    num_sparse: Vec<u64>,
    /// First UUID key (`as_u128`) of every UUID block.
    uuid_sparse: Vec<u128>,
    /// Offsets of the UUID-typed i2e slots. Loaded whole from the `is_uuid`
    /// file on open and kept resident, so decoding a slot never goes to disk
    /// for the flag.
    is_uuid: RoaringBitmap,
}

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Total number of internal ids (including build-time-deleted slots).
    pub fn total_point_count(&self) -> u64 {
        self.i2e_header.total
    }

    /// Resident RAM held by the reader: the e2i sparse block index and the
    /// `is_uuid` bitmap. The mapping data itself stays on disk.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            i2e: _,        // on-disk handle
            e2i: _,        // on-disk handle
            i2e_header: _, // constant-size
            e2i_header: _, // constant-size
            num_sparse,
            uuid_sparse,
            is_uuid,
        } = self;
        num_sparse.capacity() * size_of::<u64>()
            + uuid_sparse.capacity() * size_of::<u128>()
            + is_uuid.serialized_size()
    }
}
