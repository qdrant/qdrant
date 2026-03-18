use std::io::Write;

use tempfile::NamedTempFile;

use crate::common::sha_256::hash_file;

#[tokio::test]
async fn test_sha_256_digest() -> std::io::Result<()> {
    let mut file = NamedTempFile::new()?;
    write!(file, "This tests if the hashing a file works correctly.")?;
    let result_hash = hash_file(file.path()).await?;
    assert_eq!(
        result_hash,
        "735e3ec1b05d901d07e84b1504518442aba2395fe3f945a1c962e81a8e152b2d",
    );
    Ok(())
}
