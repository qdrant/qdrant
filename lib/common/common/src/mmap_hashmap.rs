#[cfg(any(test, feature = "testing"))]
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Cursor, Write};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::path::Path;
use std::str;

use memmap2::Mmap;
use ph::fmph::Function;
#[cfg(any(test, feature = "testing"))]
use rand::rngs::StdRng;
#[cfg(any(test, feature = "testing"))]
use rand::Rng as _;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::zeros::WriteZerosExt as _;

type ValuesLen = u32;

/// On-disk hash map backed by a memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// | header     | phf | padding       | buckets | entries   |
/// |------------|-----|---------------|---------|-----------|
/// | [`Header`] |     | `u8[0..4095]` | `u32[]` | See below |
///
/// ## Entry format for the `str` key
///
/// | key    | `'\0xff'` | padding | values_len | padding | values |
/// |--------|-----------|---------|------------|---------|--------|
/// | `u8[]` | `u8`      | `u8[]`  | `u32`      | `u8[]`  | `V[]`  |
///
/// ## Entry format for the `i64` key
///
/// | key   | values_len | padding | values |
/// |-------|------------|---------|--------|
/// | `i64` | `u32`      | `u8[]`  | `V[]`  |
pub struct MmapHashMap<K: ?Sized, V: Sized + AsBytes + FromBytes> {
    mmap: Mmap,
    header: Header,
    phf: Function,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
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

impl<K: Key + ?Sized, V: Sized + AsBytes + FromBytes> MmapHashMap<K, V> {
    /// Save `map` contents to `path`.
    pub fn create<'a>(
        path: &Path,
        map: impl Iterator<Item = (&'a K, impl ExactSizeIterator<Item = V>)> + Clone,
    ) -> io::Result<()>
    where
        K: 'a,
    {
        let keys_vec: Vec<_> = map.clone().map(|(k, _)| k).collect();
        let keys_count = keys_vec.len();
        let phf = Function::from(keys_vec);

        // == First pass ==

        let mut file_size = 0;
        // 1. Header
        file_size += size_of::<Header>();

        // 2. PHF
        file_size += phf.write_bytes();

        // 3. Padding
        let padding_len = file_size.next_multiple_of(PADDING_SIZE) - file_size;
        file_size += padding_len;

        // 4. Buckets
        let buckets_pos = file_size;
        file_size += keys_count * size_of::<BucketOffset>();

        // 5. Data
        let mut buckets = vec![0 as BucketOffset; keys_count];
        let mut last_bucket = 0usize;
        for (k, v) in map.clone() {
            last_bucket = last_bucket.next_multiple_of(K::ALIGN);
            buckets[phf.get(k).expect("Key not found in phf") as usize] =
                last_bucket as BucketOffset;
            last_bucket += Self::entry_bytes(k, v.len());
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
            key_type: K::NAME,
            buckets_pos: buckets_pos as u64,
            buckets_count: keys_count as u64,
        };
        bufw.write_all(header.as_bytes())?;

        // 2. PHF
        phf.write(&mut bufw)?;

        // 3. Padding
        bufw.write_zeros(padding_len)?;

        // 4. Buckets
        bufw.write_all(buckets.as_bytes())?;

        // 5. Data
        let mut pos = 0usize;
        for (key, values) in map {
            let next_pos = pos.next_multiple_of(K::ALIGN);
            if next_pos > pos {
                bufw.write_zeros(next_pos - pos)?;
                pos = next_pos;
            }

            let entry_size = Self::entry_bytes(key, values.len());
            pos += entry_size;

            key.write(&mut bufw)?;
            bufw.write_zeros(Self::key_padding_bytes(key))?;
            bufw.write_all((values.len() as ValuesLen).as_bytes())?;
            bufw.write_zeros(Self::values_len_padding_bytes())?;
            for i in values {
                bufw.write_all(AsBytes::as_bytes(&i))?;
            }
        }

        drop(bufw);
        file.persist(path)?;

        Ok(())
    }

    const VALUES_LEN_SIZE: usize = size_of::<ValuesLen>();
    const VALUE_SIZE: usize = size_of::<V>();

    fn key_size_with_padding(key: &K) -> usize {
        let key_size = key.write_bytes();
        key_size.next_multiple_of(Self::VALUE_SIZE)
    }

    fn key_padding_bytes(key: &K) -> usize {
        let key_size = key.write_bytes();
        key_size.next_multiple_of(Self::VALUE_SIZE) - key_size
    }

    const fn values_len_size_with_padding() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE)
    }

    const fn values_len_padding_bytes() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE) - Self::VALUES_LEN_SIZE
    }

    /// Return the total size of the entry in bytes, including: key, values_len, values, all with
    /// padding.
    fn entry_bytes(key: &K, values_len: usize) -> usize {
        Self::key_size_with_padding(key)
            + Self::values_len_size_with_padding()
            + values_len * Self::VALUE_SIZE
    }

    /// Load the hash map from file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;

        // SAFETY: Assume other processes do not modify the file.
        // See https://docs.rs/memmap2/latest/memmap2/struct.Mmap.html#safety
        let mmap = unsafe { Mmap::map(&file)? };

        let header = Header::read_from_prefix(mmap.as_ref()).ok_or(io::ErrorKind::InvalidData)?;

        if header.key_type != K::NAME {
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
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }

    pub fn keys_count(&self) -> usize {
        self.header.buckets_count as usize
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        (0..self.keys_count()).filter_map(|i| match self.get_entry(i) {
            Ok(entry) => K::from_bytes(entry),
            Err(err) => {
                debug_assert!(false, "Error reading entry for key {i}: {err}");
                log::error!("Error reading entry for key {i}: {err}");
                None
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &[V])> {
        (0..self.keys_count()).filter_map(|i| {
            let entry = self.get_entry(i).ok()?;
            let key = K::from_bytes(entry)?;
            let values = Self::get_values_from_entry(entry, key).ok()?;
            Some((key, values))
        })
    }

    /// Get the values associated with the `key`.
    pub fn get(&self, key: &K) -> io::Result<Option<&[V]>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let entry = self.get_entry(hash as usize)?;

        if !key.matches(entry) {
            return Ok(None);
        }

        Ok(Some(Self::get_values_from_entry(entry, key)?))
    }

    fn get_values_from_entry<'a>(entry: &'a [u8], key: &K) -> io::Result<&'a [V]> {
        // ## Entry format for the `i64` key
        //
        // | key   | values_len | padding | values |
        // |-------|------------|---------|--------|
        // | `i64` | `u32`      | u8[]    | `V[]`  |

        let key_size = key.write_bytes();
        let key_size_with_padding = key_size.next_multiple_of(Self::VALUE_SIZE);

        let entry = entry.get(key_size_with_padding..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Can't read entry from mmap, \
                         key_size_with_padding {key_size_with_padding} is out of bounds"
                ),
            )
        })?;

        let values_len = ValuesLen::read_from_prefix(entry).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Can't read values_len from mmap",
            )
        })? as usize;

        let values_from = Self::values_len_size_with_padding();
        let values_to = values_from + values_len * Self::VALUE_SIZE;

        let entry = entry.get(values_from..values_to).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't read values from mmap, relative range: {values_from}:{values_to}"),
            )
        })?;

        let result = V::slice_from(entry).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Can't convert mmap range into slice",
            )
        })?;
        Ok(result)
    }

    fn get_entry(&self, index: usize) -> io::Result<&[u8]> {
        // Absolute position of the bucket array in the mmap.
        let bucket_from = self.header.buckets_pos as usize;
        let bucket_to =
            bucket_from + self.header.buckets_count as usize * size_of::<BucketOffset>();

        let bucket_val = self
            .mmap
            .get(bucket_from..bucket_to)
            .and_then(BucketOffset::slice_from)
            .and_then(|buckets| buckets.get(index).copied())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Can't read bucket from mmap, pos: {bucket_from}:{bucket_to}"),
                )
            })?;

        let entry_start = self.header.buckets_pos as usize
            + self.header.buckets_count as usize * size_of::<BucketOffset>()
            + bucket_val as usize;

        self.mmap.get(entry_start..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't read entry from mmap, bucket_val {entry_start} is out of bounds"),
            )
        })
    }
}

/// A key that can be stored in the hash map.
pub trait Key: Sync + Hash {
    const ALIGN: usize;

    const NAME: [u8; 8];

    /// Returns number of bytes which `write` will write.
    fn write_bytes(&self) -> usize;

    /// Write the key to `buf`.
    fn write(&self, buf: &mut impl Write) -> io::Result<()>;

    /// Check whether the first [`Key::write_bytes()`] of `buf` match the key.
    fn matches(&self, buf: &[u8]) -> bool;

    /// Try to read the key from `buf`.
    fn from_bytes(buf: &[u8]) -> Option<&Self>;
}

impl Key for str {
    const ALIGN: usize = align_of::<u8>();

    const NAME: [u8; 8] = *b"str\0\0\0\0\0";

    fn write_bytes(&self) -> usize {
        self.len() + 1
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(self.as_bytes())?;
        buf.write_all(&[0xFF])?; // 0xFF is not a valid leading byte of a UTF-8 sequence.
        Ok(())
    }

    fn matches(&self, buf: &[u8]) -> bool {
        // The sentinel value 0xFF is used to ensure that `self` has the same length as the string
        // in the entry buffer.
        //
        // Suppose `self` is a prefix of the string in the entry buffer. (it's not very likely since
        // it would require a PHF collision, but it is still possible).
        // We'd like this method to return `false` in this case. So we need not just check that the
        // first `self.len()` bytes of `buf` are equal to `self`, but also that they have the same
        // length. To achieve that, we compare `self + [0xFF]` with `buf + [0xFF]`.
        //
        // ┌───self────┐       ┌───self────┐                 ┌─────self─────┐
        //  'f' 'o' 'o' FF      'f' 'o' 'o' FF                'f' 'o' 'o' FF
        //  'f' 'o' 'o' FF      'f' 'o' 'o' 'b' 'a' 'r' FF    'f' 'o' 'o' FF 'b' 'a' 'r' FF
        // └───entry───┘       └─────────entry─────────┘     └───────────entry──────────┘
        //    Case 1                    Case 2                          Case 3
        //    (happy)                 (collision)                   (never happens)
        //
        // 1. The case 1 is the happy path. This function returns `true`.
        // 2. In the case 2, `self` is a prefix of `entry`, but since we are also checking the
        //    sentinel, this function returns `false`. (0xFF != 'b')
        // 3. Hypothetical case 3 might never happen unless the index data is corrupted. This is
        //    because it assumes that `entry` is a concatenation of three parts: a valid UTF-8
        //    string ('foo'), a byte 0xFF, and the rest ('bar'). Concatenating a valid UTF-8 string
        //    with 0xFF will always result in an invalid UTF-8 string. Such string could not be
        //    added to the index since we are adding only valid UTF-8 strings as Rust enforces the
        //    validity of `str`/`String` types.
        buf.get(..self.len()) == Some(AsBytes::as_bytes(self)) && buf.get(self.len()) == Some(&0xFF)
    }

    fn from_bytes(buf: &[u8]) -> Option<&Self> {
        let len = buf.iter().position(|&b| b == 0xFF)?;
        str::from_utf8(&buf[..len]).ok()
    }
}

impl Key for i64 {
    const ALIGN: usize = align_of::<i64>();

    const NAME: [u8; 8] = *b"i64\0\0\0\0\0";

    fn write_bytes(&self) -> usize {
        size_of::<i64>()
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(AsBytes::as_bytes(self))
    }

    fn matches(&self, buf: &[u8]) -> bool {
        buf.get(..size_of::<i64>()) == Some(AsBytes::as_bytes(self))
    }

    fn from_bytes(buf: &[u8]) -> Option<&Self> {
        buf.get(..size_of::<i64>()).and_then(FromBytes::ref_from)
    }
}

impl Key for u128 {
    const ALIGN: usize = align_of::<u128>();

    const NAME: [u8; 8] = *b"u128\0\0\0\0";

    fn write_bytes(&self) -> usize {
        size_of::<u128>()
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(AsBytes::as_bytes(self))
    }

    fn matches(&self, buf: &[u8]) -> bool {
        buf.get(..size_of::<u128>()) == Some(AsBytes::as_bytes(self))
    }

    fn from_bytes(buf: &[u8]) -> Option<&Self> {
        buf.get(..size_of::<u128>()).and_then(FromBytes::ref_from)
    }
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
    use std::collections::HashMap;

    use rand::SeedableRng as _;

    use super::*;

    #[test]
    fn test_mmap_hash() {
        test_mmap_hash_impl(gen_ident, |s| s.as_str(), |s| s.to_owned());
        test_mmap_hash_impl(|rng| rng.gen::<i64>(), |i| i, |i| *i);
    }

    fn test_mmap_hash_impl<K: Key + ?Sized, K1: Ord + Hash>(
        gen: impl Clone + Fn(&mut StdRng) -> K1,
        as_ref: impl Fn(&K1) -> &K,
        from_ref: impl Fn(&K) -> K1,
    ) {
        let mut rng = StdRng::seed_from_u64(42);
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let map = gen_map(&mut rng, gen.clone(), 1000);
        MmapHashMap::<K, u32>::create(
            &tmpdir.path().join("map"),
            map.iter().map(|(k, v)| (as_ref(k), v.iter().copied())),
        )
        .unwrap();
        let mmap = MmapHashMap::<K, u32>::open(&tmpdir.path().join("map")).unwrap();

        // Non-existing keys should return None
        for _ in 0..1000 {
            let key = repeat_until(|| gen(&mut rng), |key| !map.contains_key(key));
            assert!(mmap.get(as_ref(&key)).unwrap().is_none());
        }

        // check keys iterator
        for key in mmap.keys() {
            let key = from_ref(key);
            assert!(map.contains_key(&key));
        }
        assert_eq!(mmap.keys_count(), map.len());
        assert_eq!(mmap.keys().count(), map.len());

        for (k, v) in mmap.iter() {
            let v = v.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(map.get(&from_ref(k)).unwrap(), &v);
        }

        // Existing keys should return the correct values
        for (k, v) in map {
            assert_eq!(
                mmap.get(as_ref(&k)).unwrap().unwrap(),
                &v.into_iter().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_mmap_hash_impl_u64_value() {
        let mut rng = StdRng::seed_from_u64(42);
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut map: HashMap<i64, BTreeSet<u64>> = Default::default();

        for key in 0..10i64 {
            map.insert(key, (0..100).map(|_| rng.gen_range(0..=1000)).collect());
        }

        MmapHashMap::<i64, u64>::create(
            &tmpdir.path().join("map"),
            map.iter().map(|(k, v)| (k, v.iter().copied())),
        )
        .unwrap();

        let mmap = MmapHashMap::<i64, u64>::open(&tmpdir.path().join("map")).unwrap();

        for (k, v) in map {
            assert_eq!(
                mmap.get(&k).unwrap().unwrap(),
                &v.into_iter().collect::<Vec<_>>()
            );
        }

        assert!(mmap.get(&100).unwrap().is_none())
    }
}
