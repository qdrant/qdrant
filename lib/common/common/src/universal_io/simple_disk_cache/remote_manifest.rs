use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aligned_vec::{AVec, RuntimeAlign};

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::Random;
use crate::mmap::AdviceSetting;
use crate::universal_io::simple_disk_cache::BLOCK_SIZE;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::{
    OpenExtra, OpenOptions, Populate, ReadPipeline, Result, UniversalRead, UniversalReadFs,
};

#[derive(Debug)]
struct FileInfo {
    /// Length in bytes of the entire file
    size: u64,
    /// First `BLOCK_SIZE` of the file, (or `size` if `size` < `BLOCK_SIZE`)
    first_block: AVec<u8, RuntimeAlign>,
}

#[derive(Debug)]
pub struct RemoteManifest {
    files: HashMap<PathBuf, FileInfo>,
}

impl RemoteManifest {
    pub fn new<Fs: UniversalReadFs>(fs: &Fs, prefix_path: &Path) -> Result<Self> {
        // List all files
        let list = fs.list_files(prefix_path)?;

        // Open all files
        let extra = Fs::OpenExtra::default().with_prevent_caching(true);
        let files = list
            .iter()
            .map(|(path, _)| {
                fs.open(
                    path,
                    OpenOptions {
                        writeable: false,
                        need_sequential: false,
                        populate: Populate::No,
                        advice: AdviceSetting::Global,
                    },
                    extra.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // Submit all reads and wait concurrently
        let mut acc = HashMap::new();
        let mut callback = |(path, size), read| {
            let first_block = match read {
                ACow::Borrowed(slice) => AVec::from_slice(REMOTE_READ_ALIGNMENT, slice),
                ACow::Owned(vec) => vec,
            };
            let info = FileInfo { size, first_block };

            acc.insert(path, info);
        };

        let mut reads = files.iter().zip(list);
        let mut pipeline = <Fs::File as UniversalRead>::ReadPipeline::new()?;
        loop {
            while pipeline.can_schedule()
                && let Some((file, (path, size))) = reads.next()
            {
                let range_end = size.min(BLOCK_SIZE as u64);
                if let Err(err) = pipeline.schedule::<Random>(
                    (path, size),
                    file,
                    0..range_end,
                    REMOTE_READ_ALIGNMENT,
                ) {
                    return Err(err);
                }
            }

            let Some((user_data, read)) = pipeline.wait()? else {
                break;
            };

            callback(user_data, read);
        }

        Ok(Self { files: acc })
    }
}
