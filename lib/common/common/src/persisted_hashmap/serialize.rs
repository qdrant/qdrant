use std::io::{self, Write};
use std::mem::size_of;
use std::path::Path;

use fs_err::File;
use ph::fmph::Function;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{BucketOffset, Header, Key, ValuesLen};
use crate::zeros::WriteZerosExt as _;

const PADDING_SIZE: usize = 4096;

/// Save `map` contents to `path`.
pub fn serialize_hashmap<'a, K, V>(
    path: &Path,
    map: impl Iterator<Item = (&'a K, impl ExactSizeIterator<Item = V>)> + Clone,
) -> io::Result<()>
where
    K: Key + ?Sized + 'a,
    V: Sized + FromBytes + Immutable + IntoBytes + KnownLayout,
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
    let buckets_size = keys_count * size_of::<BucketOffset>();
    let bucket_align = buckets_size.next_multiple_of(K::ALIGN) - buckets_size;
    file_size += bucket_align;
    // Important: Bucket Position points after the alignment for backward compatibility.
    let buckets_pos = file_size;
    file_size += buckets_size;

    // 5. Data
    let mut buckets = vec![0 as BucketOffset; keys_count];
    let mut last_bucket = 0usize;
    for (k, v) in map.clone() {
        last_bucket = last_bucket.next_multiple_of(K::ALIGN);
        buckets[phf.get(k).expect("Key not found in phf") as usize] = last_bucket as BucketOffset;
        last_bucket += entry_bytes::<K, V>(k, v.len());
    }
    file_size += last_bucket;
    _ = file_size;

    // == Second pass ==
    let (file, temp_path) = tempfile::Builder::new()
        .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
        .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?
        .into_parts();
    let file = File::from_parts::<&Path>(file, temp_path.as_ref());
    let mut bufw = io::BufWriter::new(file);

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
    // Align the buckets to `K::ALIGN`, to make sure Entry.key is aligned.
    bufw.write_zeros(bucket_align)?;
    bufw.write_all(buckets.as_bytes())?;

    // 5. Data
    let mut pos = 0usize;
    for (key, values) in map {
        let next_pos = pos.next_multiple_of(K::ALIGN);
        if next_pos > pos {
            bufw.write_zeros(next_pos - pos)?;
            pos = next_pos;
        }

        let entry_size = entry_bytes::<K, V>(key, values.len());
        pos += entry_size;

        key.write(&mut bufw)?;
        bufw.write_zeros(key_padding_bytes::<K, V>(key))?;
        bufw.write_all((values.len() as ValuesLen).as_bytes())?;
        bufw.write_zeros(values_len_padding_bytes::<V>())?;
        for i in values {
            bufw.write_all(i.as_bytes())?;
        }
    }

    // Explicitly flush write buffer so we can catch IO errors
    bufw.flush()?;
    let file = bufw.into_inner().unwrap();

    file.sync_all()?;
    drop(file);
    temp_path.persist(path)?;

    Ok(())
}

/// Return the total size of the entry in bytes, including: key, values_len, values, all with
/// padding.
fn entry_bytes<K: Key + ?Sized, V>(key: &K, values_len: usize) -> usize {
    key_size_with_padding::<K, V>(key)
        + values_len_size_with_padding::<V>()
        + values_len * size_of::<V>()
}

fn key_size_with_padding<K: Key + ?Sized, V>(key: &K) -> usize {
    let key_size = key.write_bytes();
    key_size.next_multiple_of(size_of::<V>())
}

fn key_padding_bytes<K: Key + ?Sized, V>(key: &K) -> usize {
    let key_size = key.write_bytes();
    key_size.next_multiple_of(size_of::<V>()) - key_size
}

fn values_len_size_with_padding<V>() -> usize {
    size_of::<ValuesLen>().next_multiple_of(size_of::<V>())
}

fn values_len_padding_bytes<V>() -> usize {
    size_of::<ValuesLen>().next_multiple_of(size_of::<V>()) - size_of::<ValuesLen>()
}
