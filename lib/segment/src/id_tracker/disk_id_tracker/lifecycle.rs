//! Building (from a `CompressedPointMappings`) and opening the writable
//! disk-resident tracker.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use common::bitvec::BitVec;
use common::mmap::{AdviceSetting, create_and_ensure_length};
use common::universal_io::{
    OpenOptions, Populate, SliceBufferedUpdateWrapper, TypedStorage, UniversalWrite,
};
use fs_err::File;

use super::DiskIdTracker;
use super::on_disk_format::{e2i_path, i2e_path, store_e2i, store_i2e, store_is_uuid};
use super::reader::DiskMappingReader;
use crate::common::buffered_update_bitslice::{BitmaskFormat, BufferedUpdateBitSlice};
use crate::common::operation_error::OperationResult;
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::immutable_id_tracker::{
    deleted_offsets, deleted_paths, version_mapping_path,
};
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::types::SeqNumberType;

impl<S> DiskIdTracker<S>
where
    S: UniversalWrite + Send + Sync + 'static,
{
    pub fn from_in_memory_tracker(
        fs: &S::Fs,
        in_memory_tracker: InMemoryIdTracker,
        path: &Path,
    ) -> OperationResult<Self> {
        let (internal_to_version, mappings) = in_memory_tracker.into_internal();
        let compressed_mappings = CompressedPointMappings::from_mappings(mappings);
        Self::new(fs, path, &internal_to_version, compressed_mappings)
    }

    /// Open an existing disk-resident id tracker: `deleted` and `versions` are
    /// read into RAM (small, mutated in place), the mapping stays on disk.
    pub fn open(fs: &S::Fs, segment_path: &Path) -> OperationResult<Self> {
        let deleted_wrapper = BufferedUpdateBitSlice::open(
            fs,
            &deleted_paths(segment_path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Blocking,
                advice: AdviceSetting::Global,
            },
        )?;
        let deleted = deleted_wrapper.read_all()?;

        let internal_to_version_file = TypedStorage::<S, SeqNumberType>::open(
            fs,
            version_mapping_path(segment_path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Blocking,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);
        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner)?;

        let reader = DiskMappingReader::open(fs, segment_path)?;

        Ok(Self {
            path: segment_path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            internal_to_version_wrapper,
        })
    }

    pub fn new(
        fs: &S::Fs,
        path: &Path,
        internal_to_version: &[SeqNumberType],
        mappings: CompressedPointMappings,
    ) -> OperationResult<Self> {
        let total = mappings.total_point_count();
        debug_assert!(mappings.deleted().len() <= total);

        // Deleted flags: one per point, the trailing ones beyond
        // `mappings.deleted()` all deleted.
        let deleted_wrapper = BufferedUpdateBitSlice::create(
            fs,
            &deleted_paths(path),
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::Auto,
                advice: AdviceSetting::Global,
            },
            // Kept in the legacy dense format: the read-only trackers open
            // this file as a plain bitslice.
            BitmaskFormat::Raw,
            total,
            deleted_offsets(mappings.deleted(), total),
        )?;

        // Resident deleted mirror: same bits as on disk.
        let mut deleted = BitVec::new();
        deleted.extend_from_bitslice(mappings.deleted());
        deleted.resize(total, true);

        // Versions file: one `u64` per point.
        let version_filepath = version_mapping_path(path);
        let versions_count = internal_to_version.len().max(total);
        create_and_ensure_length(
            &version_filepath,
            versions_count * size_of::<SeqNumberType>(),
        )?;
        let mut internal_to_version_file = TypedStorage::<S, SeqNumberType>::open(
            fs,
            &version_filepath,
            OpenOptions {
                writeable: true,
                need_sequential: false,
                populate: Populate::No,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )?;
        internal_to_version_file.write(0, internal_to_version)?;
        let internal_to_version =
            CompressedVersions::from_slice(&internal_to_version_file.read_whole()?);
        debug_assert_eq!(internal_to_version.len(), versions_count);
        let internal_to_version_wrapper =
            SliceBufferedUpdateWrapper::new(internal_to_version_file.inner)?;

        // Mapping files (immutable): i2e + e2i + the is_uuid sidecar.
        write_mapping_file(i2e_path(path), |writer| store_i2e(&mappings, writer))?;
        write_mapping_file(e2i_path(path), |writer| store_e2i(&mappings, writer))?;
        store_is_uuid(fs, path, &mappings)?;

        deleted_wrapper.flusher()()?;
        internal_to_version_wrapper.flusher()()?;

        let reader = DiskMappingReader::open(fs, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            reader,
            deleted,
            deleted_wrapper,
            internal_to_version,
            internal_to_version_wrapper,
        })
    }
}

/// Create a mapping file and write it with an explicit fsync.
fn write_mapping_file(
    path: PathBuf,
    write: impl FnOnce(&mut BufWriter<File>) -> OperationResult<()>,
) -> OperationResult<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    write(&mut writer)?;
    writer.flush()?;
    writer.into_inner().unwrap().sync_all()?;
    Ok(())
}
