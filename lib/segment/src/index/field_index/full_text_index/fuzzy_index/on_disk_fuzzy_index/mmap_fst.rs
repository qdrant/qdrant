use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use common::mmap::{Advice, AdviceSetting, Madviseable, open_read_mmap};
use fs_err::File;
use fst::Set;
use fst::raw::Fst;
use memmap2::Mmap;

use crate::index::field_index::full_text_index::fuzzy_index::ImmutableFuzzyIndex;

pub struct MmapFst {
    fst: Set<Mmap>,
}

impl MmapFst {
    pub fn get_fst(&self) -> &Fst<Mmap> {
        self.fst.as_fst()
    }

    pub fn fst_bytes(&self) -> &[u8] {
        self.fst.as_fst().as_bytes()
    }

    pub fn create(path: PathBuf, fuzzy_index: &ImmutableFuzzyIndex) -> std::io::Result<()> {
        let (file, temp_path) = tempfile::Builder::new()
            .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
            .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?
            .into_parts();
        let file = File::from_parts::<&Path>(file, temp_path.as_ref());
        let mut bufw = io::BufWriter::new(&file);

        bufw.write_all(fuzzy_index.get_index_as_bytes())?;

        bufw.flush()?;
        drop(bufw);

        file.sync_all()?;
        drop(file);
        temp_path.persist(path)?;

        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>, populate: bool) -> io::Result<Self> {
        let path = path.into();
        let mmap = open_read_mmap(&path, AdviceSetting::Advice(Advice::Normal), populate)?;

        let fst = Set::new(mmap).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Create fst from mmap Failed {}", path.display()),
            )
        })?;

        Ok(Self { fst })
    }

    pub fn populate(&self) {
        self.fst.as_fst().as_inner().populate();
    }
}
