//! On-disk cache for expensive benchmark setup.

use std::fmt::Debug;
use std::path::Path;
use std::sync::Once;
use std::time::{Duration, Instant};

use fs_err as fs;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Where to store the cache. Accepts [`format!`]-style arguments.
///
/// `cache_path!("foo")` -> `target/tmp/segment/hnsw_search_graph/foo`
#[macro_export]
#[doc(hidden)]
macro_rules! __cache_path {
    ($($args:tt)*) => {
        ::std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(env!("CARGO_PKG_NAME"))
            .join(env!("CARGO_CRATE_NAME"))
            .join(format!($($args)*))
    };
}
pub use __cache_path as cache_path;

static HINT: Once = Once::new();

static WARN_MSG: &str = r"
note: $BENCH_CACHE is set => using the cache from previous runs. The stale cache
      might screw the results. It's on you to delete the stale cache manually.
";

/// Preserve/reuse benchmark setup between runs.
///
/// By default, cache is written to `path` but not reused.
/// Set `$BENCH_CACHE` to skip `build()` if the `path` already exists.
///
/// `build(arg)` should create file/dir at `arg`, not `path`.
pub fn build_once<P: AsRef<Path> + Debug>(path: P, build: impl FnOnce(&Path)) -> P {
    let using_cache = std::env::var_os("BENCH_CACHE").is_some();
    let path_old = path.as_ref().with_added_extension("old");
    let path_tmp = path.as_ref().with_added_extension("tmp");

    rm_rf(&path_tmp).unwrap();

    if path.as_ref().exists() {
        rm_rf(&path_old).unwrap();
    } else if path_old.exists() {
        fs::rename(&path_old, path.as_ref()).unwrap();
    }

    if path.as_ref().exists() && using_cache {
        let age = fs::metadata(path.as_ref()).and_then(|meta| meta.modified());
        let age = age.map_or(Duration::ZERO, |time| time.elapsed().unwrap_or_default());
        let age = humantime::format_duration(Duration::from_secs(age.as_secs() / 60 * 60));
        eprintln!("build_once: using cache {path:?}, built {age} ago.");
        HINT.call_once(|| eprintln!("{}", WARN_MSG.trim()));
        return path;
    }

    eprintln!("build_once: Building {path:?}...");
    if !using_cache {
        // Discoverability hint.
        HINT.call_once(|| eprintln!("hint: Set BENCH_CACHE=1 to reuse caches between runs."));
    }

    fs::create_dir_all(path.as_ref().parent().unwrap()).unwrap();
    if path.as_ref().exists() {
        // We won't use the existing cache this time (BENCH_CACHE is not set),
        // but still preserve it in case the build fails/interrupted.
        // Give the user the last chance to press ^C and re-run with BENCH_CACHE=1.
        fs::rename(path.as_ref(), &path_old).unwrap();
    }

    let started = Instant::now();
    build(&path_tmp);
    if path.as_ref().exists() {
        // Catch misbehaving `build()`.
        rm_rf(path.as_ref()).unwrap();
        panic!("build_once: build() should not create the final path");
    }
    fs::rename(&path_tmp, path.as_ref()).unwrap();
    rm_rf(&path_old).unwrap();
    eprintln!("build_once: took {:?} to build {path:?}", started.elapsed());

    path
}

/// [`build_once`], for values stored as JSON.
pub fn cached_json<T: Serialize + DeserializeOwned>(path: &Path, build: impl FnOnce() -> T) -> T {
    build_once(path, |path| {
        fs::write(path, serde_json::to_vec(&build()).unwrap()).unwrap();
    });
    crate::fs::read_json(path).unwrap()
}

fn rm_rf(path: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
