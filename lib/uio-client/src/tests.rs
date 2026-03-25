use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::generic_consts::Random;
use common::universal_io::file_ops::UniversalReadFileOps;
use common::universal_io::{FileIndex, OpenOptions, ReadRange, UniversalRead};
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

use crate::generated::qdrant::storage_read_server::{
    StorageRead as StorageReadTrait, StorageReadServer,
};
use crate::generated::qdrant::*;
use crate::remote_read::{RemoteClient, RemoteUniversalRead, bytes_to_elements};

#[test]
fn bytes_to_elements_u8_identity() {
    let bytes = vec![1u8, 2, 3, 4, 5];
    let result: Vec<u8> = bytes_to_elements(&bytes);
    assert_eq!(result, vec![1, 2, 3, 4, 5]);
}

#[test]
fn bytes_to_elements_u32_roundtrip() {
    let values: Vec<u32> = vec![42, 0, u32::MAX, 12345];
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_ne_bytes()).collect();
    let result: Vec<u32> = bytes_to_elements(&bytes);
    assert_eq!(result, values);
}

#[test]
fn bytes_to_elements_f32_roundtrip() {
    let values: Vec<f32> = vec![1.0, -2.5, std::f32::consts::PI];
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_ne_bytes()).collect();
    let result: Vec<f32> = bytes_to_elements(&bytes);
    assert_eq!(result, values);
}

#[test]
fn bytes_to_elements_empty() {
    let result: Vec<u32> = bytes_to_elements(&[]);
    assert!(result.is_empty());
}

#[test]
fn bytes_to_elements_zst() {
    let result: Vec<()> = bytes_to_elements(&[1, 2, 3]);
    assert!(result.is_empty());
}

#[test]
fn open_returns_error() {
    let result = RemoteUniversalRead::<u8>::open("test", OpenOptions::default());
    assert!(result.is_err());
}

#[test]
fn file_ops_return_errors() {
    assert!(RemoteUniversalRead::<u8>::list_files(Path::new("/tmp")).is_err());
    assert!(RemoteUniversalRead::<u8>::exists(Path::new("/tmp/foo")).is_err());
}

struct MockServer {
    files: Arc<HashMap<(String, String), Vec<u8>>>,
}

impl MockServer {
    fn new(files: HashMap<(String, String), Vec<u8>>) -> Self {
        Self {
            files: Arc::new(files),
        }
    }
}

type BoxStream<T> = std::pin::Pin<
    Box<dyn tonic::codegen::tokio_stream::Stream<Item = std::result::Result<T, Status>> + Send>,
>;

#[tonic::async_trait]
impl StorageReadTrait for MockServer {
    async fn list_files(
        &self,
        request: Request<ListFilesRequest>,
    ) -> std::result::Result<Response<ListFilesResponse>, Status> {
        let req = request.into_inner();
        let mut paths: Vec<String> = self
            .files
            .keys()
            .filter(|(col, path)| col == &req.collection_name && path.starts_with(&req.prefix_path))
            .map(|(_, path)| path.clone())
            .collect();
        paths.sort();
        Ok(Response::new(ListFilesResponse { paths }))
    }

    async fn file_exists(
        &self,
        request: Request<FileExistsRequest>,
    ) -> std::result::Result<Response<FileExistsResponse>, Status> {
        let req = request.into_inner();
        let exists = self.files.contains_key(&(req.collection_name, req.path));
        Ok(Response::new(FileExistsResponse { exists }))
    }

    async fn file_length(
        &self,
        request: Request<FileLengthRequest>,
    ) -> std::result::Result<Response<FileLengthResponse>, Status> {
        let req = request.into_inner();
        let data = self
            .files
            .get(&(req.collection_name, req.path))
            .ok_or_else(|| Status::not_found("file not found"))?;
        Ok(Response::new(FileLengthResponse {
            length: data.len() as u64,
        }))
    }

    async fn read_bytes(
        &self,
        request: Request<ReadBytesRequest>,
    ) -> std::result::Result<Response<ReadBytesResponse>, Status> {
        let req = request.into_inner();
        let data = self
            .files
            .get(&(req.collection_name, req.path))
            .ok_or_else(|| Status::not_found("file not found"))?;
        let start = req.offset as usize;
        let end = start + req.length as usize;
        Ok(Response::new(ReadBytesResponse {
            data: data[start..end].to_vec(),
        }))
    }

    type ReadBytesStreamStream = BoxStream<ReadBytesStreamResponse>;

    async fn read_bytes_stream(
        &self,
        _request: Request<ReadBytesStreamRequest>,
    ) -> std::result::Result<Response<Self::ReadBytesStreamStream>, Status> {
        Err(Status::unimplemented("not used in client tests"))
    }

    async fn read_whole(
        &self,
        request: Request<ReadWholeRequest>,
    ) -> std::result::Result<Response<ReadWholeResponse>, Status> {
        let req = request.into_inner();
        let data = self
            .files
            .get(&(req.collection_name, req.path))
            .ok_or_else(|| Status::not_found("file not found"))?;
        Ok(Response::new(ReadWholeResponse { data: data.clone() }))
    }

    async fn read_batch(
        &self,
        request: Request<ReadBatchRequest>,
    ) -> std::result::Result<Response<ReadBatchResponse>, Status> {
        let req = request.into_inner();
        let file_data = self
            .files
            .get(&(req.collection_name, req.path))
            .ok_or_else(|| Status::not_found("file not found"))?;
        let data: Vec<Vec<u8>> = req
            .ranges
            .iter()
            .map(|r| {
                let start = r.offset as usize;
                let end = start + r.length as usize;
                file_data[start..end].to_vec()
            })
            .collect();
        Ok(Response::new(ReadBatchResponse { data }))
    }

    async fn read_multi(
        &self,
        request: Request<ReadMultiRequest>,
    ) -> std::result::Result<Response<ReadMultiResponse>, Status> {
        let req = request.into_inner();
        let data: std::result::Result<Vec<Vec<u8>>, Status> = req
            .reads
            .iter()
            .map(|entry| {
                let file_data = self
                    .files
                    .get(&(req.collection_name.clone(), entry.path.clone()))
                    .ok_or_else(|| Status::not_found(format!("file not found: {}", entry.path)))?;
                let start = entry.offset as usize;
                let end = start + entry.length as usize;
                Ok(file_data[start..end].to_vec())
            })
            .collect();
        Ok(Response::new(ReadMultiResponse { data: data? }))
    }
}

async fn start_mock(files: HashMap<(String, String), Vec<u8>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mock = MockServer::new(files);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(StorageReadServer::new(mock))
            .serve(addr)
            .await
            .unwrap();
    });

    wait_for_server(addr).await;
    format!("http://{addr}")
}

async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("mock server did not start");
}

/// Run sync `UniversalRead` methods on a blocking thread so that
/// `Handle::current().block_on()` inside the impl doesn't conflict with
/// the test's async runtime.
async fn blocking<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

fn test_files() -> HashMap<(String, String), Vec<u8>> {
    let mut files = HashMap::new();
    files.insert(
        ("test-col".into(), "data/a.bin".into()),
        b"abcdefghij".to_vec(),
    );
    files.insert(
        ("test-col".into(), "data/b.bin".into()),
        b"klmnopqrst".to_vec(),
    );
    files.insert(
        ("test-col".into(), "index/chunk_0.bin".into()),
        vec![0u8; 100],
    );
    files
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_client_connect_and_open_file() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();
    assert_eq!(file.len().unwrap(), 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_client_list_files() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let mut paths = client.list_files("test-col", "data/").await.unwrap();
    paths.sort();
    assert_eq!(
        paths,
        vec![PathBuf::from("data/a.bin"), PathBuf::from("data/b.bin")]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_client_file_exists() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    assert!(client.file_exists("test-col", "data/a.bin").await.unwrap());
    assert!(!client.file_exists("test-col", "nope.bin").await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_returns_requested_range() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();

    let data = blocking(move || {
        file.read::<Random>(ReadRange {
            byte_offset: 3,
            length: 4,
        })
        .unwrap()
        .into_owned()
    })
    .await;
    assert_eq!(&data, b"defg");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_whole_returns_entire_file() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();

    let data = blocking(move || file.read_whole().unwrap().into_owned()).await;
    assert_eq!(&data, b"abcdefghij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_batch_returns_each_range() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();

    let results = blocking(move || {
        let mut results: Vec<(usize, Vec<u8>)> = Vec::new();
        file.read_batch::<Random>(
            vec![
                ReadRange {
                    byte_offset: 0,
                    length: 3,
                },
                ReadRange {
                    byte_offset: 7,
                    length: 3,
                },
            ],
            |idx, data| {
                results.push((idx, data.to_vec()));
                Ok(())
            },
        )
        .unwrap();
        results
    })
    .await;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (0, b"abc".to_vec()));
    assert_eq!(results[1], (1, b"hij".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_multi_across_files() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file_a: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();
    let file_b: RemoteUniversalRead<u8> = client.open_file("test-col", "data/b.bin").await.unwrap();

    let results = blocking(move || {
        let files = [file_a, file_b];
        let mut results: Vec<(usize, FileIndex, Vec<u8>)> = Vec::new();
        RemoteUniversalRead::read_multi::<Random>(
            &files,
            vec![
                (
                    0,
                    ReadRange {
                        byte_offset: 1,
                        length: 3,
                    },
                ),
                (
                    1,
                    ReadRange {
                        byte_offset: 0,
                        length: 4,
                    },
                ),
                (
                    0,
                    ReadRange {
                        byte_offset: 8,
                        length: 2,
                    },
                ),
            ],
            |op_idx, file_idx, data| {
                results.push((op_idx, file_idx, data.to_vec()));
                Ok(())
            },
        )
        .unwrap();
        results
    })
    .await;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], (0, 0, b"bcd".to_vec()));
    assert_eq!(results[1], (1, 1, b"klmn".to_vec()));
    assert_eq!(results[2], (2, 0, b"ij".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_multi_empty_files_returns_error() {
    let files: &[RemoteUniversalRead<u8>] = &[];
    let result = RemoteUniversalRead::read_multi::<Random>(
        files,
        vec![(
            0,
            ReadRange {
                byte_offset: 0,
                length: 1,
            },
        )],
        |_, _, _| Ok(()),
    );
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn populate_and_clear_cache_are_noop() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u8> = client.open_file("test-col", "data/a.bin").await.unwrap();

    assert!(file.populate().is_ok());
    assert!(file.clear_ram_cache().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_and_open_convenience() {
    let url = start_mock(test_files()).await;
    let file = RemoteUniversalRead::<u8>::connect_and_open(url, "test-col", "data/a.bin")
        .await
        .unwrap();
    assert_eq!(file.len().unwrap(), 10);

    let data = blocking(move || file.read_whole().unwrap().into_owned()).await;
    assert_eq!(&data, b"abcdefghij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn from_parts_preserves_metadata() {
    let url = start_mock(test_files()).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file =
        RemoteUniversalRead::<u8>::from_parts(client, "test-col".into(), "data/a.bin".into(), 10);
    assert_eq!(file.len().unwrap(), 10);

    let data = blocking(move || file.read_whole().unwrap().into_owned()).await;
    assert_eq!(&data, b"abcdefghij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_u32_elements() {
    let values: Vec<u32> = vec![42, 100, 9999];
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_ne_bytes()).collect();
    let mut files = HashMap::new();
    files.insert(("col".into(), "ints.bin".into()), bytes);

    let url = start_mock(files).await;
    let client = RemoteClient::connect(url).await.unwrap();
    let file: RemoteUniversalRead<u32> = client.open_file("col", "ints.bin").await.unwrap();

    let data = blocking(move || file.read_whole().unwrap().into_owned()).await;
    assert_eq!(&data, &values);
}
