use std::fs::File;
use std::io::Write;
use std::path::Path;

use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::byte_stream::{ByteStream, Length};
use aws_smithy_types_convert::date_time::DateTimeExt;

use super::snapshot_ops::SnapshotDescription;
use super::types::{CollectionError, CollectionResult};

pub fn get_key(path: &Path) -> CollectionResult<String> {
    // Get file name by trimming the path.
    // if the path is ./path/to/file.txt, the key should be path/to/file.txt
    let key = path.to_str().ok_or_else(|| {
        CollectionError::s3_error(format!(
            "Failed to get key for snapshot: {}",
            path.display()
        ))
    });
    key.map(|k| k.trim_start_matches("./").to_string())
}

pub async fn multi_part_upload(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    source_path: &str,
) -> CollectionResult<CompleteMultipartUploadOutput> {
    const CHUNK_SIZE: u64 = 1024 * 1024 * 50; // 50MB
    const MAX_CHUNKS: u64 = 10000;
    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            CollectionError::s3_error(format!("Failed to create multipart upload. Error: {}", e))
        })?;

    let upload_id = multipart_upload_res.upload_id().ok_or_else(|| {
        CollectionError::s3_error(format!("Failed to get upload id for key: {}", key))
    })?;

    let file_size = tokio::fs::metadata(source_path)
        .await
        .map_err(|_| CollectionError::not_found(format!("source_path:{}", source_path)))?
        .len();

    let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
    let mut size_of_last_chunk = file_size % CHUNK_SIZE;
    if size_of_last_chunk == 0 {
        size_of_last_chunk = CHUNK_SIZE;
        chunk_count -= 1;
    }

    if file_size == 0 {
        panic!("Bad file size.");
    }
    if chunk_count > MAX_CHUNKS {
        panic!("Too many chunks! Try increasing your chunk size.")
    }

    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    for chunk_index in 0..chunk_count {
        let this_chunk = if chunk_count - 1 == chunk_index {
            size_of_last_chunk
        } else {
            CHUNK_SIZE
        };
        let stream = ByteStream::read_from()
            .path(source_path)
            .offset(chunk_index * CHUNK_SIZE)
            .length(Length::Exact(this_chunk))
            .build()
            .await
            .map_err(|e| CollectionError::s3_error(format!("Failed to read file. Error: {}", e)))?;
        //Chunk index needs to start at 0, but part numbers start at 1.
        let part_number = (chunk_index as i32) + 1;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await
            .map_err(|e| {
                CollectionError::s3_error(format!("Failed to upload part. Error: {}", e))
            })?;
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }
    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .map_err(|e| {
            CollectionError::s3_error(format!("Failed to complete multipart upload. Error: {}", e))
        })
}

pub async fn get_snapshot_description(
    client: &aws_sdk_s3::Client,
    bucket_name: String,
    key: String,
) -> CollectionResult<SnapshotDescription> {
    // if key is "path/to/example.snapshot", the name should be "file.txt"
    let name = key.split('/').last().map(String::from).ok_or_else(|| {
        "Failed to extract filename from key: Key is empty or malformed".to_string()
    })?;
    let file_meta = client
        .head_object()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(|e| {
            CollectionError::s3_error(format!("Failed to create multipart upload. Error: {}", e))
        })?;
    let creation_time = file_meta
        .last_modified
        .map(|t| {
            t.to_chrono_utc()
                .map_err(|_| CollectionError::s3_error("Failed to convert time".to_string()))
                .map(|datetime| datetime.naive_utc())
        })
        .ok_or_else(|| CollectionError::s3_error("Failed to get last modified time".to_string()))?;

    let checksum = file_meta.checksum_sha256.clone();
    let size = file_meta.content_length().unwrap_or(0) as u64;

    Ok(SnapshotDescription {
        name,
        creation_time: creation_time.ok(),
        size,
        checksum,
    })
}

pub async fn list_snapshots_in_bucket_with_key(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> CollectionResult<Vec<SnapshotDescription>> {
    let entries = client
        .list_objects_v2()
        .bucket(bucket_name)
        .prefix(key)
        .send()
        .await
        .map_err(|_| {
            CollectionError::not_found(format!("bucket_name:{}, key:{}", bucket_name, key))
        })?
        .contents
        .ok_or_else(|| {
            CollectionError::s3_error(format!(
                "Failed to list objects in bucket: {} with key {}",
                bucket_name, key
            ))
        })?;

    let snapshot_futures: Vec<_> = entries
        .into_iter()
        .filter_map(|entry| {
            entry
                .key
                .map(|key| get_snapshot_description(client, bucket_name.to_string(), key.clone()))
        })
        .collect();

    let snapshots = futures::future::join_all(snapshot_futures).await;
    // Ok(snapshots.into_iter().collect::<Result<_, _>>().unwrap())
    snapshots.into_iter().collect::<Result<_, _>>()
}

pub async fn delete_snapshot(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> CollectionResult<bool> {
    client
        .delete_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .map_err(|_| {
            CollectionError::not_found(format!("bucket_name:{}, key:{}", bucket_name, key))
        })?;

    Ok(true)
}

pub async fn download_snapshot(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
    destination_path: &Path,
) -> CollectionResult<()> {
    let mut file = File::create(destination_path)?;

    let mut object = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .map_err(|_| {
            CollectionError::not_found(format!("bucket_name:{}, key:{}", bucket_name, key))
        })?;

    let expected_total_bytes = object.content_length().unwrap_or(0);
    let mut byte_count = 0_usize;
    while let Some(bytes) = object.body.try_next().await.unwrap() {
        let bytes_len = bytes.len();
        file.write_all(&bytes)?;
        byte_count += bytes_len;
        log::debug!("Downloaded {}/{} bytes", byte_count, expected_total_bytes);
    }

    Ok(())
}
