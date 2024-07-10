#[cfg(any(test, feature = "testing"))]
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Cursor, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;
use std::str;

use memmap2::Mmap;
use ph::fmph::Function;
#[cfg(any(test, feature = "testing"))]
use rand::rngs::StdRng;
#[cfg(any(test, feature = "testing"))]
use rand::Rng as _;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::types::PointOffsetType;

/// On-disk hash map baked by a memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// | header     | phf | padding       | buckets | entries   |
/// |------------|-----|---------------|---------|-----------|
/// | [`Header`] |     | `u8[0..4095]` | `u32[]` | See below |
///
/// ## Entry format for the `str` key
///
/// | key    | `'\0xff'` | padding | values_len | values              |
/// |--------|-----------|---------|------------|---------------------|
/// | `u8[]` | `u8`      | `u8[]`  | `u32`      | `PointOffsetType[]` |
///
/// ## Entry format for the `u32` key
///
/// | key   | values_len | values              |
/// |-------|------------|---------------------|
/// | `u32` | `u32`      | `PointOffsetType[]` |
pub struct MmapHashMap<K: ?Sized> {
    mmap: Mmap,
    header: Header,
    phf: Function,
    _phantom: PhantomData<K>,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, FromZeroes)]
struct Header {
    key_type: [u8; 8],
    buckets_pos: u64,
    buckets_count: u64,
}

const PADDING_SIZE: usize = 4096;

type BucketOffset = u64;

impl<K: Key + ?Sized + 'static> MmapHashMap<K> {
    /// Save `map` contents to `path`.
    pub fn create<'a, 'b>(
        path: &'a Path,
        map: impl Iterator<Item = (&'b K, impl ExactSizeIterator<Item = PointOffsetType> + 'b)> + Clone,
    ) -> io::Result<()> {
        let keys_vec = map.clone().map(|(k, _)| k).collect::<Vec<_>>();
        let keys_count = keys_vec.len();
        let phf = Function::from(keys_vec);

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
        file_size += keys_count * size_of::<BucketOffset>();

        // 5. Data
        let mut buckets = vec![0 as BucketOffset; keys_count];
        let mut last_bucket = 0;
        for (k, v) in map.clone() {
            buckets[phf.get(k).expect("Key not found in phf") as usize] =
                last_bucket as BucketOffset;
            last_bucket += Self::entry_bytes(k, v.len()).0;
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
            key_type: K::name(),
            buckets_pos: buckets_pos as u64,
            buckets_count: keys_count as u64,
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
        for (key, values) in map {
            buckets.push(pos as BucketOffset);

            let (entry_size, padding) = Self::entry_bytes(key, values.len());
            pos += entry_size;
            key.write(&mut bufw)?;
            bufw.write_all(zeroes(padding))?;
            bufw.write_all((values.len() as PointOffsetType).as_bytes())?;
            for i in values {
                bufw.write_all(AsBytes::as_bytes(&i))?;
            }
        }

        drop(bufw);
        file.persist(path)?;

        Ok(())
    }

    fn entry_bytes(key: &K, values_len: usize) -> (usize, usize) {
        let key_size = key.write_bytes();
        let padding_bytes = key_size.next_multiple_of(size_of::<PointOffsetType>()) - key_size;
        let total_bytes = key_size
            + padding_bytes
            + size_of::<PointOffsetType>()
            + values_len * size_of::<PointOffsetType>();
        (total_bytes, padding_bytes)
    }

    /// Load the hash map from file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;

        // SAFETY: Assume other processes do not modify the file.
        // See https://docs.rs/memmap2/latest/memmap2/struct.Mmap.html#safety
        let mmap = unsafe { Mmap::map(&file)? };

        let header = Header::read_from_prefix(mmap.as_ref()).ok_or(io::ErrorKind::InvalidData)?;

        if header.key_type != K::name() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key type mismatch",
            ));
        }

        let phf = Function::read(&mut Cursor::new(
            &mmap
                .get(size_of::<Header>()..header.buckets_pos as usize)
                .ok_or(io::ErrorKind::InvalidData)?,
        ))?;

        Ok(MmapHashMap {
            mmap,
            header,
            phf,
            _phantom: PhantomData,
        })
    }

    pub fn keys_count(&self) -> usize {
        self.header.buckets_count as usize
    }

    /// Get the values associated with the `key`.
    #[inline(never)]
    pub fn get(&self, key: &K) -> io::Result<Option<&[PointOffsetType]>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let bucket_val = self
            .mmap
            .get(
                self.header.buckets_pos as usize
                    ..self.header.buckets_pos as usize
                        + self.header.buckets_count as usize * size_of::<BucketOffset>(),
            )
            .and_then(BucketOffset::slice_from)
            .and_then(|buckets| buckets.get(hash as usize).copied())
            .ok_or(io::ErrorKind::InvalidData)?;

        let entry = self
            .mmap
            .get(
                self.header.buckets_pos as usize
                    + self.header.buckets_count as usize * size_of::<BucketOffset>()
                    + bucket_val as usize..,
            )
            .ok_or(io::ErrorKind::InvalidData)?;
        let entry_start = entry.as_ptr() as usize;

        if !key.matches(entry) {
            return Ok(None);
        }
        let entry = &entry[key.write_bytes()..];

        let padding = {
            let pos = entry.as_ptr() as usize - entry_start;
            pos.next_multiple_of(4) - pos
        };
        let entry = entry.get(padding..).ok_or(io::ErrorKind::InvalidData)?;

        let values_len =
            PointOffsetType::read_from_prefix(entry).ok_or(io::ErrorKind::InvalidData)? as usize;
        let entry = entry
            .get(
                size_of::<PointOffsetType>()
                    ..size_of::<PointOffsetType>() + values_len * size_of::<PointOffsetType>(),
            )
            .ok_or(io::ErrorKind::InvalidData)?;
        let result = PointOffsetType::slice_from(entry).ok_or(io::ErrorKind::InvalidData)?;
        Ok(Some(result))
    }
}

/// A key that can be stored in the hash map.
pub trait Key: Sync + Hash {
    fn name() -> [u8; 8];

    /// Returns number of bytes which `write` will write.
    fn write_bytes(&self) -> usize;

    /// Write the key to `buf`.
    fn write(&self, buf: &mut impl Write) -> io::Result<()>;

    /// Check whether the first [`Key::write_bytes()`] of `buf` match the key.
    fn matches(&self, buf: &[u8]) -> bool;
}

impl Key for str {
    fn name() -> [u8; 8] {
        *b"str\0\0\0\0\0"
    }

    fn write_bytes(&self) -> usize {
        self.len() + 1
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(self.as_bytes())?;
        buf.write_all(&[0xff])?; // Not a valid UTF-8 byte
        Ok(())
    }

    fn matches(&self, buf: &[u8]) -> bool {
        if buf.len() < self.write_bytes() {
            return false;
        }
        &buf[..self.len()] == self.as_bytes() && buf[self.len()] == 0xff
    }
}

impl Key for i64 {
    fn name() -> [u8; 8] {
        let name = stringify!(i64).as_bytes();
        let mut result = [0; 8];
        result[..name.len()].copy_from_slice(name);
        result
    }

    fn write_bytes(&self) -> usize {
        size_of::<i64>()
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(AsBytes::as_bytes(self))
    }

    fn matches(&self, buf: &[u8]) -> bool {
        if buf.len() < self.write_bytes() {
            return false;
        }
        &buf[..size_of::<i64>()] == AsBytes::as_bytes(self)
    }
}

/// Returns a reference to a slice of zeroes of length `len`.
#[inline]
fn zeroes(len: usize) -> &'static [u8] {
    const ZEROES: [u8; PADDING_SIZE] = [0u8; PADDING_SIZE];
    &ZEROES[..len]
}

#[cfg(any(test, feature = "testing"))]
pub fn gen_map<T: Eq + Ord + Hash>(
    rng: &mut StdRng,
    gen_key: impl Fn(&mut StdRng) -> T,
    count: usize,
) -> BTreeMap<T, BTreeSet<u32>> {
    let mut map = BTreeMap::new();

    for _ in 0..count {
        let key = repeat_until(|| gen_key(rng), |key| !map.contains_key(key));
        let set = (0..rng.gen_range(1..=100))
            .map(|_| rng.gen_range(0..=1000))
            .collect::<BTreeSet<_>>();
        map.insert(key, set);
    }

    map
}

#[cfg(any(test, feature = "testing"))]
pub fn gen_ident(rng: &mut StdRng) -> String {
    (0..rng.gen_range(5..=32))
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect()
}

#[cfg(any(test, feature = "testing"))]
fn repeat_until<T>(mut f: impl FnMut() -> T, cond: impl Fn(&T) -> bool) -> T {
    std::iter::from_fn(|| Some(f())).find(|v| cond(v)).unwrap()
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;

    use super::*;

    #[test]
    fn test_mmap_hash() {
        test_mmap_hash_impl(gen_ident, |s| s.as_str());
        test_mmap_hash_impl(|rng| rng.gen::<i64>(), |i| i);
    }

    fn test_mmap_hash_impl<K: Key + ?Sized + 'static, K1: Ord + Hash>(
        gen: impl Clone + Fn(&mut StdRng) -> K1,
        as_ref: impl Fn(&K1) -> &K,
    ) {
        let mut rng = StdRng::seed_from_u64(42);
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let map = gen_map(&mut rng, gen.clone(), 1000);
        MmapHashMap::<K>::create(
            &tmpdir.path().join("map"),
            map.iter().map(|(k, v)| (as_ref(k), v.iter().copied())),
        )
        .unwrap();
        let mmap = MmapHashMap::<K>::open(&tmpdir.path().join("map")).unwrap();

        // Non-existing keys should return None
        for _ in 0..1000 {
            let key = repeat_until(|| gen(&mut rng), |key| !map.contains_key(key));
            assert!(mmap.get(as_ref(&key)).unwrap().is_none());
        }

        // Existing keys should return the correct values
        for (k, v) in map.into_iter() {
            assert_eq!(
                mmap.get(as_ref(&k)).unwrap().unwrap(),
                &v.into_iter().collect::<Vec<_>>()
            );
        }
    }
}
