//! Process-wide counts of the HTTP requests actually sent to S3, by S3 price class.
//!
//! [`RemoteIoStats`](crate::RemoteIoStats) counts logical operations. Here every attempt is
//! counted: retries, list pages, coalesced range GETs, multipart parts and hand-signed appends.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use http::Method;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
};
use strum::{EnumCount, EnumIter, IntoEnumIterator as _, IntoStaticStr};

/// S3 request price class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumCount, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RequestClass {
    /// GET and HEAD of an object.
    Get,
    /// `ListObjectsV2` pages and `ListMultipartUploads`.
    List,
    /// PUT, COPY and POST: uploads, parts, multipart create and complete.
    Put,
    /// DELETE and `DeleteObjects` (free on S3).
    Delete,
    /// Any other method.
    Other,
}

/// How a request ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumCount, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RequestStatus {
    /// 1xx, 2xx, 3xx.
    Ok,
    /// 4xx.
    ClientError,
    /// 5xx.
    ServerError,
    /// No response: connect, TLS or timeout error.
    TransportError,
}

const CLASSES: usize = RequestClass::COUNT;
const STATUSES: usize = RequestStatus::COUNT;

struct HttpRequestStats {
    requests: [[AtomicU64; STATUSES]; CLASSES],
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
}

static STATS: HttpRequestStats = HttpRequestStats {
    requests: [const { [const { AtomicU64::new(0) }; STATUSES] }; CLASSES],
    bytes_sent: AtomicU64::new(0),
    bytes_received: AtomicU64::new(0),
};

/// Cumulative counts since process start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpRequestStatsSnapshot {
    requests: [[u64; STATUSES]; CLASSES],
    /// Request body bytes of every attempt.
    pub bytes_sent: u64,
    /// Response `Content-Length` of every response, which a dropped download may not have read.
    pub bytes_received: u64,
}

impl HttpRequestStatsSnapshot {
    pub fn requests(&self, class: RequestClass, status: RequestStatus) -> u64 {
        self.requests[class as usize][status as usize]
    }

    /// Every (class, status) pair with its count, zero counts included.
    pub fn iter(&self) -> impl Iterator<Item = (RequestClass, RequestStatus, u64)> + '_ {
        RequestClass::iter().flat_map(move |class| {
            RequestStatus::iter().map(move |status| (class, status, self.requests(class, status)))
        })
    }
}

pub fn http_request_stats() -> HttpRequestStatsSnapshot {
    HttpRequestStatsSnapshot {
        requests: std::array::from_fn(|class| {
            std::array::from_fn(|status| STATS.requests[class][status].load(Ordering::Relaxed))
        }),
        bytes_sent: STATS.bytes_sent.load(Ordering::Relaxed),
        bytes_received: STATS.bytes_received.load(Ordering::Relaxed),
    }
}

fn record(class: RequestClass, status: RequestStatus) {
    STATS.requests[class as usize][status as usize].fetch_add(1, Ordering::Relaxed);
}

/// S3 price class of a request, from its method and query.
pub fn classify(method: &Method, query: Option<&str>) -> RequestClass {
    let has_key = |key: &str| {
        query.is_some_and(|query| {
            query
                .split('&')
                .any(|pair| pair.split('=').next() == Some(key))
        })
    };
    match *method {
        Method::GET if has_key("list-type") || has_key("uploads") => RequestClass::List,
        Method::GET | Method::HEAD => RequestClass::Get,
        Method::POST if has_key("delete") => RequestClass::Delete,
        Method::PUT | Method::POST => RequestClass::Put,
        Method::DELETE => RequestClass::Delete,
        _ => RequestClass::Other,
    }
}

fn status_of(code: http::StatusCode) -> RequestStatus {
    if code.is_server_error() {
        RequestStatus::ServerError
    } else if code.is_client_error() {
        RequestStatus::ClientError
    } else {
        RequestStatus::Ok
    }
}

/// Wraps an [`HttpConnector`] so every request its clients send is counted.
#[derive(Debug, Default)]
pub struct CountingConnector<C>(pub C);

impl<C: HttpConnector> HttpConnector for CountingConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.0.connect(options)?;
        Ok(HttpClient::new(CountingService(inner)))
    }
}

#[derive(Debug)]
struct CountingService(HttpClient);

#[async_trait]
impl HttpService for CountingService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let class = classify(request.method(), request.uri().query());
        let sent = request.body().content_length() as u64;
        let result = self.0.execute(request).await;
        match &result {
            Ok(response) => {
                record(class, status_of(response.status()));
                STATS.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                let received = response
                    .headers()
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                STATS.bytes_received.fetch_add(received, Ordering::Relaxed);
            }
            Err(_) => record(class, RequestStatus::TransportError),
        }
        result
    }
}

#[cfg(test)]
mod tests;
