use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Cursor, Write as _};
use std::mem::size_of;
use std::path::Path;
use std::str;

use memmap2::Mmap;
use ph::fmph::Function;
#[cfg(test)]
use rand::rngs::StdRng;
#[cfg(test)]
use rand::{Rng as _, SeedableRng as _};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

/// On-disk hash map baked by a memory-mapped file.
pub struct MmapHashMap {
    mmap: Mmap,
    header: Header,
    phf: Function,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, FromZeroes)]
struct Header {
    buckets_pos: u64,
    buckets_count: u64,
}

const PADDING_SIZE: usize = 4096;

impl MmapHashMap {
    /// Save `map` contents to `path`.
    pub fn create(path: &Path, map: BTreeMap<String, BTreeSet<u32>>) -> io::Result<()> {
        let phf = Function::from(map.keys().collect::<Vec<_>>().as_slice());

        // == First pass ==

        let mut file_size = 0;
        // 1. Header
        file_size += size_of::<Header>();

        // 2. PHF
        file_size += phf.write_bytes();

        // 3. Padding
        let padding_len = (PADDING_SIZE - (file_size % PADDING_SIZE)) % PADDING_SIZE;
        file_size += padding_len;

        // 4. Buckets
        let buckets_pos = file_size;
        file_size += map.len() * size_of::<u64>();

        // 5. Data
        let mut buckets = vec![0u64; map.len()];
        let mut last_bucket = 0;
        for (k, v) in map.iter() {
            buckets[phf.get(k).expect("Key not found in phf") as usize] = last_bucket as u64;
            last_bucket += entry_size(k, v);
        }
        file_size += last_bucket;
        _ = file_size;

        // == Second pass ==
        let file = tempfile::Builder::new()
            .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
            .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?;
        let mut bufw = io::BufWriter::new(&file);

        // 1. Header
        let header = Header {
            buckets_pos: buckets_pos as u64,
            buckets_count: map.len() as u64,
        };
        bufw.write_all(header.as_bytes())?;

        // 2. PHF
        phf.write(&mut bufw)?;

        // 3. Padding
        bufw.write_all(zeroes(padding_len))?;

        // 4. Buckets
        bufw.write_all(buckets.as_bytes())?;

        // 5. Data
        let mut pos = 0;
        for (k, v) in map.iter() {
            buckets.push(pos as u64);
            pos += entry_size(k, v);

            // Lengths
            bufw.write_all((k.len() as u64).as_bytes())?;
            bufw.write_all((v.len() as u64).as_bytes())?;

            // Key
            bufw.write_all(k.as_bytes())?;

            bufw.write_all(zeroes(k.len().next_multiple_of(4) - k.len()))?;

            // Values
            for i in v.iter() {
                bufw.write_all(i.as_bytes())?;
            }
        }

        drop(bufw);
        file.persist(path)?;

        Ok(())
    }

    /// Load the hash map from file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;

        // SAFETY: Assume other processes do not modify the file.
        // See https://docs.rs/memmap2/latest/memmap2/struct.Mmap.html#safety
        let mmap = unsafe { Mmap::map(&file)? };

        let header = Header::read_from_prefix(mmap.as_ref()).ok_or(io::ErrorKind::InvalidData)?;

        let phf = Function::read(&mut Cursor::new(
            &mmap
                .get(size_of::<Header>()..header.buckets_pos as usize)
                .ok_or(io::ErrorKind::InvalidData)?,
        ))?;

        Ok(MmapHashMap { mmap, header, phf })
    }

    /// Get the values associated with the `key`.
    pub fn get(&self, key: &str) -> io::Result<Option<&[u32]>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let bucket_val = self
            .mmap
            .get(
                self.header.buckets_pos as usize
                    ..self.header.buckets_pos as usize
                        + self.header.buckets_count as usize * size_of::<u64>(),
            )
            .and_then(u64::slice_from)
            .and_then(|buckets| buckets.get(hash as usize).copied())
            .ok_or(io::ErrorKind::InvalidData)?;

        let entry = self
            .mmap
            .get(
                self.header.buckets_pos as usize
                    + self.header.buckets_count as usize * size_of::<u64>()
                    + bucket_val as usize..,
            )
            .ok_or(io::ErrorKind::InvalidData)?;

        let key_len = entry
            .get(0..size_of::<u64>())
            .and_then(u64::read_from)
            .ok_or(io::ErrorKind::InvalidData)? as usize;
        let values_len = entry
            .get(size_of::<u64>()..2 * size_of::<u64>())
            .and_then(u64::read_from)
            .ok_or(io::ErrorKind::InvalidData)? as usize;

        let hash_key = entry
            .get(2 * size_of::<u64>()..2 * size_of::<u64>() + key_len)
            .ok_or(io::ErrorKind::InvalidData)?;
        if hash_key != key.as_bytes() {
            return Ok(None);
        }

        let result = entry
            .get(
                2 * size_of::<u64>() + key_len.next_multiple_of(4)
                    ..2 * size_of::<u64>()
                        + key_len.next_multiple_of(4)
                        + values_len * size_of::<u32>(),
            )
            .and_then(u32::slice_from)
            .ok_or(io::ErrorKind::InvalidData)?;
        Ok(Some(result))
    }
}

fn entry_size(key: &str, values: &BTreeSet<u32>) -> usize {
    2 * size_of::<u64>()
        + key.as_bytes().len().next_multiple_of(4)
        + values.len() * size_of::<u32>()
}

/// Returns a reference to a slice of zeroes of length `len`.
#[inline]
fn zeroes(len: usize) -> &'static [u8] {
    const ZEROES: [u8; PADDING_SIZE] = [0u8; PADDING_SIZE];
    &ZEROES[..len]
}

#[cfg(test)]
fn random_map(rng: &mut StdRng, count: usize) -> BTreeMap<String, BTreeSet<u32>> {
    let mut map = BTreeMap::new();

    for _ in 0..count {
        let ident = rand_ident(rng, |ident| !map.contains_key(ident));
        let set = (0..rng.gen_range(1..=100))
            .map(|_| rng.gen_range(0..=1000))
            .collect::<BTreeSet<_>>();
        map.insert(ident, set);
    }

    map
}

#[cfg(test)]
fn rand_ident(rng: &mut StdRng, cond: impl Fn(&str) -> bool) -> String {
    loop {
        let ident = (0..rng.gen_range(5..=32))
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect::<String>();
        if cond(&ident) {
            return ident;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmap_hash() {
        let mut rng = StdRng::seed_from_u64(42);
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let map = random_map(&mut rng, 1000);
        MmapHashMap::create(&tmpdir.path().join("map"), map.clone()).unwrap();
        let mmap = MmapHashMap::open(&tmpdir.path().join("map")).unwrap();

        // Non-existing keys should return None
        for _ in 0..1000 {
            let key = rand_ident(&mut rng, |i| !map.contains_key(i));
            assert!(mmap.get(&key).unwrap().is_none());
        }

        // Existing keys should return the correct values
        for (k, v) in map.into_iter() {
            assert_eq!(
                mmap.get(&k).unwrap().unwrap(),
                &v.into_iter().collect::<Vec<_>>()
            );
        }
    }
}
