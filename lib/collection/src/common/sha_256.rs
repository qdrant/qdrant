use std::io;
use std::path::Path;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

const SHA256_HASH_LENGTH: usize = 64;

/// Compute sha256 hash for the given file
pub async fn hash_file(file_path: &Path) -> io::Result<String> {
    log::debug!("Computing checksum for file: {file_path:?}");

    // On Linux in debug builds, get hash through sha256sum because it's significantly faster
    #[cfg(all(debug_assertions, target_os = "linux"))]
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
#[cfg(all(debug_assertions, target_os = "linux"))]
async fn hash_file_bin(file_path: &Path) -> io::Result<String> {
    use tokio::process::Command;

    let output = match Command::new("sha256sum")
        .arg("-b")
        .arg(file_path)
        .output()
        .await
    {
        Ok(command) if command.status.success() => command,
        Ok(command) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to compute checksum with sha256sum: {}",
                    String::from_utf8(command.stderr).unwrap(),
                ),
            ));
        }
        Err(err) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to compute checksum with sha256sum: {err}"),
            ));
        }
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

    /// Test consistency between built-in sha256 hasher and sha256sum binary
    #[tokio::test]
    #[cfg(all(debug_assertions, target_os = "linux"))]
    async fn test_hash_file_bin() {
        use std::io::Write;

        use rand::{thread_rng, Rng};

        // Create a temporary file with random data
        let mut temp_file = tempfile::Builder::new()
            .prefix("test-sha256sum")
            .tempfile()
            .unwrap();
        let mut buf = [0u8; 128];
        thread_rng().fill(&mut buf[..]);
        temp_file.write_all(&buf).unwrap();

        let hash_builtin = hash_file_builtin(temp_file.path()).await.unwrap();
        let hash_bin = match hash_file_bin(temp_file.path()).await {
            Ok(hash) => hash,
            // May error if the sha256bin binary is not available on this system
            Err(err) => {
                log::warn!("Failed to compute checksum with sha256sum, skipping test: {err}");
                return;
            }
        };

        assert_eq!(
            hash_builtin, hash_bin,
            "built-in hasher and sha256sum binary must produce the same hash",
        );
    }
}
