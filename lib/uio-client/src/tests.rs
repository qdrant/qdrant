use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use common::universal_io::ReadRange;
use tokio::net::TcpListener;
use tonic::{Request, Response, Status};

use crate::generated::qdrant::storage_read_server::{
    StorageRead as StorageReadTrait, StorageReadServer,
};
use crate::generated::qdrant::*;
use crate::read::Client;

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
        let start = req.byte_offset as usize;
        let end = start + req.length as usize;
        Ok(Response::new(ReadBytesResponse {
            data: data[start..end].to_vec(),
        }))
    }

    type ReadBytesStreamStream = BoxStream<ReadBytesStreamResponse>;

    async fn read_bytes_stream(
        &self,
        request: Request<ReadBytesStreamRequest>,
    ) -> std::result::Result<Response<Self::ReadBytesStreamStream>, Status> {
        let req = request.into_inner();
        let data = self
            .files
            .get(&(req.collection_name, req.path))
            .ok_or_else(|| Status::not_found("file not found"))?;
        let start = req.byte_offset as usize;
        let end = start + req.length as usize;
        let full = data[start..end].to_vec();

        const CHUNK_SIZE: usize = 4;
        let chunks: Vec<std::result::Result<ReadBytesStreamResponse, Status>> = full
            .chunks(CHUNK_SIZE)
            .map(|c| Ok(ReadBytesStreamResponse { data: c.to_vec() }))
            .collect();

        Ok(Response::new(Box::pin(futures::stream::iter(chunks))))
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
                let start = r.byte_offset as usize;
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
                let start = entry.byte_offset as usize;
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
async fn list_files() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let mut paths = client.list_files("test-col", "data/").await.unwrap();
    paths.sort();
    assert_eq!(
        paths,
        vec![PathBuf::from("data/a.bin"), PathBuf::from("data/b.bin")]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_exists() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert!(client.file_exists("test-col", "data/a.bin").await.unwrap());
    assert!(!client.file_exists("test-col", "nope.bin").await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_length() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert_eq!(
        client.file_length("test-col", "data/a.bin").await.unwrap(),
        10
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let data = client
        .read_bytes("test-col", "data/a.bin", 3, 4)
        .await
        .unwrap();
    assert_eq!(&data, b"defg");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes_stream() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let data = client
        .read_bytes_stream("test-col", "data/a.bin", 0, 10)
        .await
        .unwrap();
    assert_eq!(&data, b"abcdefghij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes_stream_reassembles_chunks() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let data = client
        .read_bytes_stream("test-col", "index/chunk_0.bin", 0, 100)
        .await
        .unwrap();
    assert_eq!(data.len(), 100);
    assert!(data.iter().all(|&b| b == 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_whole() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let data = client.read_whole("test-col", "data/a.bin").await.unwrap();
    assert_eq!(&data, b"abcdefghij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_batch() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let ranges = [
        ReadRange {
            byte_offset: 0,
            length: 3,
        },
        ReadRange {
            byte_offset: 7,
            length: 3,
        },
    ];
    let data = client
        .read_batch("test-col", "data/a.bin", &ranges)
        .await
        .unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(&data[0], b"abc");
    assert_eq!(&data[1], b"hij");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_multi() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let reads = vec![
        (
            "data/a.bin",
            ReadRange {
                byte_offset: 1,
                length: 3,
            },
        ),
        (
            "data/b.bin",
            ReadRange {
                byte_offset: 0,
                length: 4,
            },
        ),
    ];
    let data = client.read_multi("test-col", &reads).await.unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(&data[0], b"bcd");
    assert_eq!(&data[1], b"klmn");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_length_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert!(client.file_length("test-col", "nope.bin").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert!(
        client
            .read_bytes("test-col", "nope.bin", 0, 1)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes_stream_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert!(
        client
            .read_bytes_stream("test-col", "nope.bin", 0, 1)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_whole_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    assert!(client.read_whole("test-col", "nope.bin").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_batch_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let ranges = [ReadRange {
        byte_offset: 0,
        length: 1,
    }];
    assert!(
        client
            .read_batch("test-col", "nope.bin", &ranges)
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_multi_not_found() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let reads = vec![(
        "nope.bin",
        ReadRange {
            byte_offset: 0,
            length: 1,
        },
    )];
    assert!(client.read_multi("test-col", &reads).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_files_empty_result() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let paths = client.list_files("test-col", "nonexistent/").await.unwrap();
    assert!(paths.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_bytes_stream_partial_range() {
    let url = start_mock(test_files()).await;
    let client = Client::connect(url).await.unwrap();
    let data = client
        .read_bytes_stream("test-col", "data/a.bin", 3, 4)
        .await
        .unwrap();
    assert_eq!(&data, b"defg");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_invalid_endpoint() {
    let result = Client::connect("http://127.0.0.1:1").await;
    assert!(result.is_err());
}
