//! How a [`list_files`](super::UniversalReadFs::list_files) prefix splits into
//! the directory to list and the name prefix entries in it must start with.

use std::borrow::Cow;
use std::path::Path;

/// The parts of a list prefix: entries of `dir` whose name starts with
/// `name_prefix` match, together with the whole tree beneath each matching
/// directory.
///
/// `dir/chunk_` splits into `dir` and `chunk_`. A prefix ending with a path
/// separator names a directory itself — `dir/segments/` (as built by
/// `dir.join("segments").join("")`) splits into `dir/segments` and an empty
/// name prefix, so it matches exactly the tree under `dir/segments`, and not
/// its siblings such as `dir/segments_manifest.json`. Without the trailing
/// separator, `dir/segments` would list everything in `dir` starting with
/// `segments` — which on an object store means listing all of `dir`.
pub struct ListPrefix<'a> {
    pub dir: &'a Path,
    pub name_prefix: Cow<'a, str>,
}

impl<'a> ListPrefix<'a> {
    pub fn new(prefix_path: &'a Path) -> Self {
        if names_directory(prefix_path) {
            return Self {
                dir: prefix_path,
                name_prefix: Cow::Borrowed(""),
            };
        }
        Self {
            dir: prefix_path.parent().unwrap_or(Path::new("")),
            name_prefix: prefix_path
                .file_name()
                .map(|name| name.to_string_lossy())
                .unwrap_or_default(),
        }
    }

    /// Whether the listing names a whole directory (the prefix ends with a
    /// separator) rather than a name prefix within its parent.
    pub fn is_directory(&self) -> bool {
        self.name_prefix.is_empty()
    }
}

/// Whether `prefix_path` ends with a path separator.
fn names_directory(prefix_path: &Path) -> bool {
    prefix_path
        .as_os_str()
        .as_encoded_bytes()
        .last()
        .is_some_and(|&byte| std::path::is_separator(char::from(byte)))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn name_prefix_splits_at_the_parent() {
        let prefix = ListPrefix::new(Path::new("dir/chunk_"));
        assert_eq!(prefix.dir, Path::new("dir"));
        assert_eq!(prefix.name_prefix, "chunk_");
        assert!(!prefix.is_directory());
    }

    #[test]
    fn trailing_separator_names_the_directory() {
        let path = PathBuf::from("dir").join("segments").join("");
        let prefix = ListPrefix::new(&path);
        assert_eq!(prefix.dir, Path::new("dir/segments"));
        assert_eq!(prefix.name_prefix, "");
        assert!(prefix.is_directory());
    }

    #[test]
    fn bare_name_lists_the_current_directory() {
        let prefix = ListPrefix::new(Path::new("segments"));
        assert_eq!(prefix.dir, Path::new(""));
        assert_eq!(prefix.name_prefix, "segments");
    }
}
