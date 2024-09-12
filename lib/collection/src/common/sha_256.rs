use std::io;
use std::path::Path;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

/// Compute sha256 hash for the given file
pub async fn hash_file(file_path: &Path) -> io::Result<String> {
    log::debug!("Computing checksum for file: {file_path:?}");

    const ONE_MB: usize = 1024 * 1024;
    let input_file = tokio::fs::File::open(file_path).await?;
    let mut reader = tokio::io::BufReader::new(input_file);
    let mut sha = Sha256::new();
    let mut buf = BytesMut::with_capacity(ONE_MB);
    loop {
        buf.clear();
        let len = reader.read_buf(&mut buf).await?;
        if len == 0 {
            break;
        }
        sha.update(&buf[0..len]);
    }
    let hash = sha.finalize();
    Ok(format!("{hash:x}"))
}

/// Compare two hashes, ignoring whitespace and case
pub fn hashes_equal(a: &str, b: &str) -> bool {
    Iterator::eq(
        a.chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| c.to_ascii_lowercase()),
        b.chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| c.to_ascii_lowercase()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_hash() {
        assert!(hashes_equal("0123abc", "0123abc"));
        assert!(hashes_equal("0123abc", "0123ABC"));
        assert!(hashes_equal("0123abc", "0123abc "));
        assert!(!hashes_equal("0123abc", "0123abd"));
    }
}
