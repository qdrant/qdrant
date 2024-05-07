use std::path::PathBuf;
use std::{fs::File, path::Path};

use std::io::{BufReader, Read};

use futures::StreamExt;
use object_store::WriteMultipart;
use tokio::io::AsyncWriteExt;

use super::{
    snapshot_ops::SnapshotDescription,
    types::{CollectionError, CollectionResult},
};

fn trim_dot_slash(path: &Path) -> CollectionResult<object_store::path::Path> {
    // Get file name by trimming the path.
    // if the path is ./path/to/file.txt, the key should be path/to/file.txt
    let key = path.to_str().ok_or_else(|| {
        CollectionError::s3_error(format!(
            "Failed to get key for snapshot: {}",
            path.display()
        ))
    })?;
    Ok(object_store::path::Path::from(key.trim_start_matches("./")))
}

fn get_filename(path: &str) -> CollectionResult<String> {
    let path = PathBuf::from(path);
    path.file_name()
        .ok_or_else(|| CollectionError::s3_error("Failed to get file name".to_string()))
        .and_then(|name| {
            name.to_str()
                .ok_or_else(|| CollectionError::s3_error("Failed to get file name".to_string()))
                .map(|name| name.to_string())
        })
}

pub async fn get_snapshot_description(
    client: &dyn object_store::ObjectStore,
    path: &Path,
) -> CollectionResult<SnapshotDescription> {
    let file_meta: object_store::ObjectMeta = client
        .head(&trim_dot_slash(path)?)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to get head: {}", e)))?;

    let name = get_filename(path.to_str().ok_or_else(|| {
        CollectionError::s3_error(format!(
            "Failed to get key for snapshot: {}",
            path.display()
        ))
    })?)?;
    let size = file_meta.size as u64;
    let last_modified = file_meta.last_modified.naive_local();
    let checksum = None;

    Ok(SnapshotDescription {
        name,
        creation_time: Some(last_modified),
        size,
        checksum,
    })
}

/// This function adjusts the chunk size based on service limits and the total size of the data to be uploaded.
/// Note:
///
/// * Amazon S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
///     partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Google Cloud Storage: https://cloud.google.com/storage/quotas?hl=ja#objects
///     partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Azure Storage: https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob?tabs=microsoft-entra-id#remarks
///     <TODO> It looks like Azure Storage has different limits for different service versions.
pub async fn get_appropriate_chunk_size(local_source_path: &Path) -> CollectionResult<usize> {
    const DEFAULT_CHUNK_SIZE: usize = 50 * 1024 * 1024;
    const MAX_PART_NUMBER: usize = 10000;
    const MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024;

    let file_meta = tokio::fs::metadata(local_source_path).await?;
    let file_size = file_meta.len() as usize;

    // check if the file size exceeds the maximum upload size
    if file_size > MAX_UPLOAD_SIZE {
        return Err(CollectionError::service_error(format!(
            "File size exceeds the maximum upload size: {}",
            MAX_UPLOAD_SIZE
        )));
    }

    // check if the file size exceeds the maximum part number
    // if so, adjust the chunk size to fit the maximum part number
    if file_size > DEFAULT_CHUNK_SIZE * MAX_PART_NUMBER {
        let chunk_size = (file_size - 1 / MAX_PART_NUMBER) + 1; // ceil(file_size / MAX_PART_NUMBER)
        return Ok(chunk_size);
    }
    Ok(DEFAULT_CHUNK_SIZE)
}

pub async fn multipart_upload(
    client: &dyn object_store::ObjectStore,
    source_path: &Path,
    target_path: &Path,
) -> CollectionResult<()> {
    let s3_path = trim_dot_slash(target_path)?;
    let upload = client
        .put_multipart(&s3_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to put multipart: {}", e)))?;

    let chunk_size: usize = get_appropriate_chunk_size(source_path).await?;
    let mut write = WriteMultipart::new_with_chunk_size(upload, chunk_size);
    let file = File::open(source_path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0u8; chunk_size];
    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }
        let buffer = &buffer[..bytes_read];
        write.write(buffer);
    }
    write
        .finish()
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to finish upload: {}", e)))?;
    Ok(())
}

pub async fn list_snapshot_descriptions(
    client: &dyn object_store::ObjectStore,
    directory: &Path,
) -> CollectionResult<Vec<SnapshotDescription>> {
    let prefix = trim_dot_slash(directory)?;
    let mut list_stream = client.list(Some(&prefix));

    let mut snapshots = Vec::new();
    while let Some(meta) =
        list_stream.next().await.transpose().map_err(|e| {
            CollectionError::service_error(format!("Failed to list snapshots: {}", e))
        })?
    {
        snapshots.push(SnapshotDescription {
            name: get_filename(meta.location.as_ref())?,
            creation_time: Some(meta.last_modified.naive_local()),
            size: meta.size as u64,
            checksum: None,
        });
    }
    Ok(snapshots)
}

pub async fn delete_snapshot(
    client: &dyn object_store::ObjectStore,
    path: &Path,
) -> CollectionResult<bool> {
    client
        .delete(&trim_dot_slash(path)?)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to delete snapshot: {}", e)))?;
    Ok(true)
}

pub async fn download_snapshot(
    client: &dyn object_store::ObjectStore,
    path: &Path,
    target_path: &Path,
) -> CollectionResult<()> {
    let s3_path = trim_dot_slash(path)?;
    let download = client
        .get(&s3_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to get: {}", e)))?;

    let mut stream = download.into_stream();
    let mut file = tokio::fs::File::create(target_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to create file: {}", e)))?;

    while let Some(data) = stream.next().await {
        let data = data.map_err(|e| {
            CollectionError::service_error(format!("Failed to get data from stream: {}", e))
        })?;
        file.write_all(&data).await.map_err(|e| {
            CollectionError::service_error(format!("Failed to write to file: {}", e))
        })?;
    }
    Ok(())
}
