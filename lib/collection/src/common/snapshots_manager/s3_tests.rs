use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::TcpListener;

use http::HeaderMap;

use super::*;

#[tokio::test]
async fn snapshot_upload_uses_crc64nvme() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(&mut stream);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            headers += &line;
        }
        let mut body = [0; 4];
        reader.read_exact(&mut body).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nETag: \"snapshot\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        assert_eq!(body, *b"data");
        let headers: HeaderMap = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| {
                (
                    http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_str(value.trim()).unwrap(),
                )
            })
            .collect();
        assert_eq!(headers["x-amz-checksum-crc64nvme"], "7a5hQ478J4A=");
        assert!(!headers.contains_key("x-amz-checksum-sha256"));
    });
    let store = build_s3_client(Some(&S3Config {
        bucket: "snapshots".into(),
        region: Some("us-east-1".into()),
        access_key: Some("id".into()),
        secret_key: Some("secret".into()),
        endpoint_url: Some(endpoint),
        ..Default::default()
    }))
    .unwrap();
    store
        .put(
            &object_store::path::Path::from("collection.snapshot"),
            bytes::Bytes::from_static(b"data").into(),
        )
        .await
        .unwrap();
    server.join().unwrap();
}
