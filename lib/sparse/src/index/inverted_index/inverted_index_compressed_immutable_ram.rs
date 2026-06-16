use std::borrow::Cow;
use std::path::Path;

use blink_alloc::Blink;
use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::VecExt;
use common::types::PointOffsetType;
use common::universal_io::{MmapFs, Result, UniversalRead, UniversalWrite, UserData};

use super::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use super::inverted_index_ram::InvertedIndexRam;
use super::{InvertedIndex, out_of_bounds};
use crate::common::sparse_vector::RemappedSparseVector;
use crate::common::types::{DimOffset, Weight};
use crate::index::compressed_posting_list::{
    CompressedPostingBuilder, CompressedPostingList, CompressedPostingListIterator,
    CompressedPostingListView,
};
use crate::index::inverted_index::inverted_index_compressed_mmap::Version;
use crate::index::inverted_index::{InvertedIndexReadOnly, InvertedIndexReadWrite};

#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexCompressedImmutableRam<W: Weight> {
    pub(super) postings: Vec<CompressedPostingList<W>>,
    pub(super) vector_count: usize,
    pub(super) total_sparse_size: usize,
}

type Storage = common::universal_io::MmapFile;

impl<W: Weight, S: UniversalRead + 'static> InvertedIndexReadOnly<S>
    for InvertedIndexCompressedImmutableRam<W>
{
    fn open_ro_impl(fs: &S::Fs, path: &Path) -> Result<Self> {
        let mmap_inverted_index = InvertedIndexCompressedMmap::<W, S>::open_ro(fs, path)?;
        Self::from_mmap_index(mmap_inverted_index)
    }
}

impl<W: Weight, S: UniversalWrite + 'static> InvertedIndexReadWrite<S>
    for InvertedIndexCompressedImmutableRam<W>
{
    fn open_rw_impl(fs: &<S as UniversalRead>::Fs, path: &Path) -> Result<Self> {
        let mmap_inverted_index = InvertedIndexCompressedMmap::<W, S>::open_rw(fs, path)?;
        Self::from_mmap_index(mmap_inverted_index)
    }

    fn from_ram_index_impl<P: AsRef<Path>>(
        _fs: &S::Fs,
        ram_index: Cow<InvertedIndexRam>,
        _path: P,
    ) -> Result<Self> {
        let mut postings = Vec::with_capacity(ram_index.postings.len());
        for old_posting_list in &ram_index.postings {
            let mut new_posting_list = CompressedPostingBuilder::new();
            for elem in &old_posting_list.elements {
                new_posting_list.add(elem.record_id, elem.weight);
            }
            postings.push(new_posting_list.build());
        }

        let hw_counter = HardwareCounterCell::disposable();

        let total_sparse_size = postings
            .iter()
            .map(|p| p.view(&hw_counter).store_size().total)
            .sum();

        Ok(InvertedIndexCompressedImmutableRam {
            postings,
            vector_count: ram_index.vector_count,
            total_sparse_size,
        })
    }
}

impl<W: Weight> InvertedIndex for InvertedIndexCompressedImmutableRam<W> {
    type Iter<'a> = CompressedPostingListIterator<'a, W>;

    type Version = Version;

    fn is_on_disk(&self) -> bool {
        false
    }

    fn save(&self, path: &Path) -> Result<()> {
        InvertedIndexCompressedMmap::<W, Storage>::convert_and_save(&MmapFs, self, path)?;
        Ok(())
    }

    fn get_batch<'a, U: UserData>(
        &'a self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        _arena: &'a Blink,
        hw_counter: &'a HardwareCounterCell, // Ignored for in-ram index
        mut callback: impl FnMut(U, Self::Iter<'a>) -> Result<()>,
    ) -> Result<()> {
        for (user_data, id) in ids {
            callback(user_data, self.get(id, hw_counter)?.iter())?;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.postings.len()
    }

    fn posting_list_len_batch<U: UserData>(
        &self,
        ids: impl Iterator<Item = (U, DimOffset)>,
        hw_counter: &HardwareCounterCell,
        mut callback: impl FnMut(U, usize) -> Result<()>,
    ) -> Result<()> {
        for (user_data, id) in ids {
            callback(user_data, self.get(id, hw_counter)?.len())?;
        }
        Ok(())
    }

    fn files(path: &Path) -> Vec<std::path::PathBuf> {
        InvertedIndexCompressedMmap::<W, Storage>::files(path)
    }

    fn immutable_files(path: &Path) -> Vec<std::path::PathBuf> {
        // `InvertedIndexCompressedImmutableRam` is always immutable
        InvertedIndexCompressedMmap::<W, Storage>::immutable_files(path)
    }

    fn remove(&mut self, _id: PointOffsetType, _old_vector: RemappedSparseVector) {
        panic!("Cannot remove from a read-only RAM inverted index")
    }

    fn upsert(
        &mut self,
        _id: PointOffsetType,
        _vector: RemappedSparseVector,
        _old_vector: Option<RemappedSparseVector>,
    ) {
        panic!("Cannot upsert into a read-only RAM inverted index")
    }

    fn vector_count(&self) -> usize {
        self.vector_count
    }

    fn total_sparse_vectors_size(&self) -> usize {
        self.total_sparse_size
    }

    fn max_index(&self) -> Option<DimOffset> {
        self.postings
            .len()
            .checked_sub(1)
            .map(|len| len as DimOffset)
    }
}

impl<W: Weight> InvertedIndexCompressedImmutableRam<W> {
    /// Materialize an mmap-layout index into owned in-RAM postings.
    fn from_mmap_index<S>(mmap_inverted_index: InvertedIndexCompressedMmap<W, S>) -> Result<Self>
    where
        S: UniversalRead + 'static,
    {
        let hw_counter = HardwareCounterCell::disposable();
        let mut postings = vec![None; mmap_inverted_index.file_header.posting_count];
        mmap_inverted_index.for_each_view(&hw_counter, |id, view| {
            postings[id as usize] = Some(view.to_owned());
            Ok(())
        })?;

        mmap_inverted_index.clear_cache()?;

        Ok(InvertedIndexCompressedImmutableRam {
            postings: postings.transform_in_place(Option::unwrap),
            vector_count: mmap_inverted_index.file_header.vector_count,
            // populated by `load`/`load_universal` when missing from a legacy header
            total_sparse_size: mmap_inverted_index
                .file_header
                .total_sparse_size
                .unwrap_or(0),
        })
    }

    #[inline]
    fn get<'a>(
        &'a self,
        id: DimOffset,
        hw_counter: &'a HardwareCounterCell,
    ) -> Result<CompressedPostingListView<'a, W>> {
        let Some(posting) = self.postings.get(id as usize) else {
            return Err(out_of_bounds(id, self.len()));
        };
        Ok(posting.view(hw_counter))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::sparse_vector_fixture::random_sparse_vector;
    use crate::common::types::QuantizedU8;
    use crate::index::inverted_index::inverted_index_ram_builder::InvertedIndexBuilder;

    #[test]
    fn test_save_load_tiny() {
        let mut builder = InvertedIndexBuilder::new();
        builder.add(1, vec![(1, 10.0), (2, 10.0), (3, 10.0)].try_into().unwrap());
        builder.add(2, vec![(1, 20.0), (2, 20.0), (3, 20.0)].try_into().unwrap());
        builder.add(3, vec![(1, 30.0), (2, 30.0), (3, 30.0)].try_into().unwrap());
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    #[test]
    fn test_save_load_large() {
        let mut rnd_gen = rand::rng();
        let mut builder = InvertedIndexBuilder::new();
        // Enough elements to put some of them into chunks
        for i in 0..1024 {
            builder.add(i, random_sparse_vector(&mut rnd_gen, 3).into_remapped());
        }
        let inverted_index_ram = builder.build();

        check_save_load::<f32>(&inverted_index_ram);
        check_save_load::<half::f16>(&inverted_index_ram);
        check_save_load::<u8>(&inverted_index_ram);
        check_save_load::<QuantizedU8>(&inverted_index_ram);
    }

    fn check_save_load<W: Weight>(inverted_index_ram: &InvertedIndexRam) {
        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();
        let inverted_index_immutable_ram =
            InvertedIndexCompressedImmutableRam::<W>::from_ram_index(
                &MmapFs,
                Cow::Borrowed(inverted_index_ram),
                tmp_dir_path.path(),
            )
            .unwrap();
        inverted_index_immutable_ram
            .save(tmp_dir_path.path())
            .unwrap();

        let loaded_inverted_index =
            InvertedIndexCompressedImmutableRam::<W>::open_ro(&MmapFs, tmp_dir_path.path())
                .unwrap();
        assert_eq!(inverted_index_immutable_ram, loaded_inverted_index);
    }
}
