//! Test-only local HTTP stub for exercising the hand-signed S3 requests of
//! [`native`](super::native) and [`rewrite`](super::rewrite) hermetically.

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use object_store::aws::{AmazonS3, AmazonS3Builder};
use url::Url;

use super::signed::SignedRequestContext;

/// Canned response served by [`stub_server`].
pub(in crate::append) struct StubResponse {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: &'static str,
}

impl StubResponse {
    pub(in crate::append) fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: "",
        }
    }

    pub(in crate::append) fn header(mut self, name: &'static str, value: impl ToString) -> Self {
        self.headers.push((name, value.to_string()));
        self
    }

    pub(in crate::append) fn body(mut self, body: &'static str) -> Self {
        self.body = body;
        self
    }
}

/// One request as observed by [`stub_server`].
pub(in crate::append) struct SeenRequest {
    pub(in crate::append) method: String,
    pub(in crate::append) path: String,
    pub(in crate::append) write_offset: Option<String>,
    pub(in crate::append) copy_source: Option<String>,
    pub(in crate::append) copy_range: Option<String>,
    pub(in crate::append) signed: bool,
    pub(in crate::append) body: Vec<u8>,
}

/// Minimal local HTTP/1.1 server: serves the canned responses in order,
/// one connection per response (every response carries
/// `connection: close`, so retries and the `head()` reconciliation
/// arrive as fresh connections), recording each request. The listener
/// stops after the last response, so an unexpected extra request fails
/// to connect instead of hanging the test.
pub(in crate::append) fn stub_server(
    responses: Vec<StubResponse>,
) -> (String, Arc<Mutex<Vec<SeenRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));

    let seen_in_server = Arc::clone(&seen);
    std::thread::spawn(move || {
        for response in responses {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let Some(request) = read_request(&mut stream) else {
                return;
            };
            seen_in_server.lock().unwrap().push(request);

            let mut payload = format!("HTTP/1.1 {} Stub\r\n", response.status);
            for (name, value) in &response.headers {
                payload += &format!("{name}: {value}\r\n");
            }
            // A HEAD response declares a length without carrying a body.
            let has_length = response
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
            if !has_length {
                payload += &format!("content-length: {}\r\n", response.body.len());
            }
            payload += "connection: close\r\n\r\n";
            payload += response.body;
            let _ = stream.write_all(payload.as_bytes());
        }
    });

    (endpoint, seen)
}

fn read_request(stream: &mut TcpStream) -> Option<SeenRequest> {
    let mut reader = BufReader::new(stream);

    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut content_length = 0;
    let mut write_offset = None;
    let mut copy_source = None;
    let mut copy_range = None;
    let mut signed = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        let value = value.trim().to_string();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().ok()?;
        } else if name.eq_ignore_ascii_case(super::native::WRITE_OFFSET_HEADER) {
            write_offset = Some(value);
        } else if name.eq_ignore_ascii_case("x-amz-copy-source") {
            copy_source = Some(value);
        } else if name.eq_ignore_ascii_case("x-amz-copy-source-range") {
            copy_range = Some(value);
        } else if name.eq_ignore_ascii_case("authorization") {
            signed = true;
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).ok()?;

    Some(SeenRequest {
        method,
        path,
        write_offset,
        copy_source,
        copy_range,
        signed,
        body,
    })
}

/// A store + signed-request context wired to the stub `endpoint`, for the
/// bucket `bucket`.
pub(in crate::append) fn stub_store_and_context(
    endpoint: &str,
) -> (Arc<AmazonS3>, SignedRequestContext) {
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("id")
            .with_secret_access_key("secret")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .build()
            .unwrap(),
    );
    let context = SignedRequestContext::new(
        true,
        "bucket".to_string(),
        Url::parse(&format!("{endpoint}/bucket")).unwrap(),
        "us-east-1".to_string(),
        "s3",
    );
    (store, context)
}
