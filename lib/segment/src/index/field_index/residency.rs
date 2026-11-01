//! Page-cache residency of payload index files, for IO-aware filter planning.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// How long a residency probe stays valid. A probe is one `mincore` over every file of
/// an index, so it is too expensive to repeat for every search.
const PROBE_TTL: Duration = Duration::from_secs(1);

/// Fraction of an index's bytes that must be in the page cache for per-point checks
/// to be treated as memory lookups rather than disk reads.
const RESIDENT_FRACTION: f64 = 0.9;

/// Last probe per index, keyed by the index's first file.
// ponytail: entries of dropped indexes are never removed; one small entry per index
// ever probed. Move the cache into the index if that grows.
static PROBES: LazyLock<Mutex<HashMap<PathBuf, (Instant, bool)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Whether at least [`RESIDENT_FRACTION`] of the bytes of `files` are in the page cache.
/// The answer is reused for [`PROBE_TTL`]. Files that can't be probed count as not
/// resident, so an error makes the planner assume disk reads.
pub fn mostly_resident(files: &[PathBuf]) -> bool {
    let Some(key) = files.first() else {
        return true;
    };
    let now = Instant::now();
    if let Some(&(probed_at, resident)) = PROBES.lock().get(key)
        && now.duration_since(probed_at) < PROBE_TTL
    {
        return resident;
    }
    let resident = probe(files);
    PROBES.lock().insert(key.clone(), (now, resident));
    resident
}

#[cfg(unix)]
fn probe(files: &[PathBuf]) -> bool {
    let mut total = 0u64;
    let mut resident = 0u64;
    for file in files {
        let Ok((file_total, file_resident)) =
            common::universal_io::MmapFile::probe_memory_stats(file)
        else {
            return false;
        };
        total += file_total;
        resident += file_resident;
    }
    total == 0 || resident as f64 >= total as f64 * RESIDENT_FRACTION
}

#[cfg(not(unix))]
fn probe(_files: &[PathBuf]) -> bool {
    false
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn test_freshly_written_file_is_resident() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.bin");
        fs_err::write(&path, vec![1u8; 64 * 1024]).unwrap();
        assert!(mostly_resident(std::slice::from_ref(&path)));

        let missing = dir.path().join("missing.bin");
        assert!(!mostly_resident(&[missing]));
    }
}
