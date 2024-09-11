use std::io;
use std::path::Path;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

const SHA256_HASH_LENGTH: usize = 64;

/// Compute sha256 hash for the given file
pub async fn hash_file(file_path: &Path) -> io::Result<String> {
    log::debug!("Computing checksum for file: {file_path:?}");

    // On unix in debug builds, get hash through sha256sum because it's significantly faster
    #[cfg(all(debug_assertions, unix))]
    match hash_file_bin(file_path).await {
        Ok(hash) => return Ok(hash),
        Err(err) => log::warn!(
            "Failed to compute checksum with sha256sum, falling back to built-in hasher: {err}",
        ),
    };

    hash_file_builtin(file_path).await
}

async fn hash_file_builtin(file_path: &Path) -> io::Result<String> {
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

/// Compute sha256 hash for the given file using the `sha256sum` binary
///
/// May fail if the binary does not exist on the system.
#[cfg(all(debug_assertions, unix))]
async fn hash_file_bin(file_path: &Path) -> io::Result<String> {
    use tokio::process::Command;

    let output = match Command::new("sha256sum")
        .arg("-b")
        .arg(file_path.as_os_str())
        .output()
        .await
    {
        // Binary must have success exit code
        Ok(command) if !command.status.success() => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to compute checksum with sha256sum: {}",
                    String::from_utf8(command.stderr).unwrap(),
                ),
            ));
        }
        // Binary must have been called successfully
        Err(err) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to compute checksum with sha256sum: {err}"),
            ));
        }
        Ok(command) => command,
    };

    let stdout = String::from_utf8(output.stdout).unwrap();
    let hash = stdout.split_whitespace().next().unwrap();

    if hash.len() != SHA256_HASH_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Failed to compute checksum with sha256sum, malformed output: {stdout}"),
        ));
    }

    Ok(hash.to_string())
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
