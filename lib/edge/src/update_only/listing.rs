//! One directory listing shared by every segment of the shard.
//!
//! Each segment's [`CachedFs`](common::universal_io::CachedFs) needs a LIST
//! snapshot of its directory, at open and on every live reload. Taken per
//! segment, that is one LIST per segment — and on an object store, listing
//! `segments/<uuid>` already lists all of `segments/` (object stores list by
//! whole directory and filter by name, see the object-store bridge), so every
//! one of those LISTs pays for the whole shard. Listing the common parent once
//! and handing each segment its part costs a single LIST instead.
//!
//! The common parent is listed *as a directory* — with a trailing separator,
//! see [`ListPrefix`](common::universal_io::ListPrefix). Listed as a name
//! prefix, `<shard>/segments` would make an object store list all of
//! `<shard>/`, which also holds the shard's WAL, cursor and, in serverless,
//! the whole backlog of pending update requests.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::universal_io::{ListedFile, UniversalReadFsAsync};
use segment::common::operation_error::OperationResult;

/// A LIST covering every segment directory in `segment_paths`: the
/// directory itself when there is just one, otherwise one LIST per distinct
/// parent — a single one for a shard, whose segments all live under
/// `segments/`. No segments, no LIST.
///
/// A lone segment is listed exactly, since backends that list by prefix (the
/// gRPC bridge, local disks) would return every sibling's files too when
/// listing the parent.
pub(super) fn list_segment_dirs<'a, Fs: UniversalReadFsAsync>(
    fs: &Fs,
    segment_paths: impl IntoIterator<Item = &'a Path>,
) -> OperationResult<Vec<ListedFile>> {
    let segment_paths: BTreeSet<&Path> = segment_paths.into_iter().collect();
    let prefixes: BTreeSet<&Path> = if segment_paths.len() == 1 {
        segment_paths
    } else {
        segment_paths
            .into_iter()
            .map(|path| {
                // A segment path without a parent lists itself.
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(path)
            })
            .collect()
    };

    // Each prefix as a directory: exactly its tree, not its siblings'.
    let prefixes: Vec<PathBuf> = prefixes.into_iter().map(|p| p.join("")).collect();

    futures::executor::block_on(async {
        let listings =
            futures::future::try_join_all(prefixes.iter().map(|p| fs.list_files_async(p))).await?;
        Ok(listings.into_iter().flatten().collect())
    })
}
