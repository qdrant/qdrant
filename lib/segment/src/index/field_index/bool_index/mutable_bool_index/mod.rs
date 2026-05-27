use std::path::PathBuf;

use common::universal_io::MmapFile;

use crate::common::flags::roaring_flags::RoaringFlags;

mod lifecycle;
mod read_ops;

pub use self::lifecycle::MutableBoolIndexBuilder;

pub(super) const TRUES_DIRNAME: &str = "trues";
pub(super) const FALSES_DIRNAME: &str = "falses";

/// Payload index for boolean values, in-memory via roaring bitmaps, stored in memory-mapped bitslices.
pub struct MutableBoolIndex {
    pub(super) base_dir: PathBuf,
    pub(super) indexed_count: usize,
    pub(super) trues_count: usize,
    pub(super) falses_count: usize,
    pub(super) storage: Storage<MmapFile>,
}

pub(super) struct Storage<S: common::universal_io::UniversalRead> {
    pub(super) trues_flags: RoaringFlags<S>,
    pub(super) falses_flags: RoaringFlags<S>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::TempDir;
    use walkdir::WalkDir;

    use super::MutableBoolIndex;
    use crate::index::field_index::PayloadFieldIndex;

    #[test]
    fn test_files() {
        let dir = TempDir::with_prefix("test_mmap_bool_index").unwrap();
        let index = MutableBoolIndex::open(dir.path(), true).unwrap().unwrap();

        let reported = index.files().into_iter().collect::<HashSet<_>>();

        let actual = WalkDir::new(dir.path())
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                entry.path().is_file().then_some(entry.into_path())
            })
            .collect::<HashSet<_>>();

        assert_eq!(reported, actual);
    }
}
