use std::io;
use std::path::Path;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

pub async fn hash_file(file_path: &Path) -> io::Result<String> {
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
