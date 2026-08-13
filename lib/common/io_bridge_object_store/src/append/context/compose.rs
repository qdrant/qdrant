//! The GCS `compose` append strategy: the appended data is uploaded as a
//! temporary object, then the destination is atomically rewritten as the
//! server-side concatenation `compose([existing, temporary])` — nothing is
//! downloaded. Like the S3 strategies, the compose call itself is
//! hand-issued: `object_store` does not expose `compose`.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
use object_store::client::{HttpClient, HttpRequestBody};
use object_store::gcp::GoogleCloudStorage;
use object_store::{ObjectStoreExt as _, PutPayload};
use url::Url;

use super::signed::build_http_client;

/// JSON API base in production.
const DEFAULT_API_BASE: &str = "https://storage.googleapis.com";

/// The GCS `compose` append strategy.
///
/// Unlike the S3 rewrites, the compose is a real compare-and-swap: it is
/// conditional (`ifGenerationMatch`) on the destination generation observed
/// before the upload, so a concurrent rewrite surfaces as an offset
/// conflict instead of silently losing data.
///
/// Each append grows the destination's component count by one; GCS does not
/// cap the count, but readers of heavily-composed objects may benefit from
/// an occasional flattening rewrite.
#[derive(Debug, Clone)]
pub struct ComposeAppend {
    /// Reqwest-backed HTTP client for the compose request, built on first
    /// use and shared across clones.
    client: Arc<OnceLock<HttpClient>>,
    /// Whether to allow a plain-http API base (test endpoints).
    allow_http: bool,
    /// GCS bucket name.
    bucket: String,
    /// JSON API base; [`DEFAULT_API_BASE`] in production.
    api_base: Url,
}

impl ComposeAppend {
    pub fn new(bucket: String) -> Self {
        let api_base = Url::parse(DEFAULT_API_BASE).expect("default API base is a valid URL");
        Self::with_api_base(bucket, api_base, false)
    }

    /// Point the JSON API at a custom (possibly plain-http) endpoint.
    pub fn with_api_base(bucket: String, api_base: Url, allow_http: bool) -> Self {
        Self {
            client: Arc::new(OnceLock::new()),
            allow_http,
            bucket,
            api_base,
        }
    }

    /// Append `data` at `offset` (== the current object size) by composing
    /// the existing object with the uploaded data. Returns the new total
    /// object size.
    pub(in crate::append) async fn append(
        &self,
        store: &Arc<GoogleCloudStorage>,
        key: &object_store::path::Path,
        offset: u64,
        data: Bytes,
    ) -> UioResult<u64> {
        let final_size = offset + data.len() as u64;
        let conflict = || UniversalIoError::AppendOffsetConflict {
            path: std::path::PathBuf::from(key.to_string()),
            offset,
        };

        // An empty prefix means a plain put produces the right object.
        if offset == 0 {
            store
                .put(key, PutPayload::from(data))
                .await
                .map_err(UniversalIoError::s3)?;
            return Ok(final_size);
        }

        // Observe the destination: its size must match the caller's view of
        // the object, and its generation anchors the compose CAS below.
        let meta = match store.head(key).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Err(conflict()),
            Err(other) => return Err(UniversalIoError::s3(other)),
        };
        if meta.size != offset {
            return Err(conflict());
        }

        // The appended bytes as a temporary neighbor object. The name is
        // deterministic: under the single-writer contract a collision is a
        // leftover of our own failed attempt, and overwriting it is exactly
        // right.
        let temp_key = object_store::path::Path::from(format!("{key}.compose-append"));
        store
            .put(&temp_key, PutPayload::from(data))
            .await
            .map_err(UniversalIoError::s3)?;

        let bearer = store
            .credentials()
            .get_credential()
            .await
            .map_err(UniversalIoError::s3)?
            .bearer
            .clone();
        let result = self
            .compose_request(&bearer, key, &temp_key, meta.version.as_deref(), offset)
            .await;

        // Best effort: an orphaned temporary object only wastes its own
        // bytes and is overwritten by the next append anyway.
        let _ = store.delete(&temp_key).await;

        result?;
        Ok(final_size)
    }

    /// `POST /storage/v1/b/{bucket}/o/{dest}/compose` — atomically replace
    /// `dest` with the concatenation of `dest` and `temp`, conditional on
    /// `dest` still being at `generation`.
    async fn compose_request(
        &self,
        bearer: &str,
        dest: &object_store::path::Path,
        temp: &object_store::path::Path,
        generation: Option<&str>,
        offset: u64,
    ) -> UioResult<()> {
        let client = self.client()?;

        let mut url = self.api_base.clone();
        url.path_segments_mut()
            .map_err(|()| UniversalIoError::S3Config {
                description: "compose API base cannot be a base URL".to_string(),
            })?
            .pop_if_empty()
            .extend(["storage", "v1", "b", self.bucket.as_str(), "o"])
            // One segment: the `/`s of the object name are percent-encoded.
            .push(dest.as_ref())
            .push("compose");
        if let Some(generation) = generation {
            url.query_pairs_mut()
                .append_pair("ifGenerationMatch", generation);
        }

        // Hand-built JSON: object names in our storage layouts are plain
        // path characters, never JSON metacharacters.
        let body = format!(
            r#"{{"sourceObjects":[{{"name":"{dest}"}},{{"name":"{temp}"}}],"destination":{{}}}}"#,
        );

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri(url.as_str())
            .header(http::header::AUTHORIZATION, format!("Bearer {bearer}"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(HttpRequestBody::from(Bytes::from(body)))
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("compose request for {dest}: {err}"),
            })?;

        let response = client
            .execute(request)
            .await
            .map_err(UniversalIoError::s3)?;
        let status = response.status();

        // The generation moved: the object was rewritten since we observed
        // it — the compose CAS caught a lost race.
        if status == http::StatusCode::PRECONDITION_FAILED {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: std::path::PathBuf::from(dest.to_string()),
                offset,
            });
        }

        if !status.is_success() {
            let body = response.into_body().bytes().await.unwrap_or_default();
            let excerpt: String = String::from_utf8_lossy(&body).chars().take(512).collect();
            return Err(UniversalIoError::s3(std::io::Error::other(format!(
                "compose for {dest} failed with status {status}: {excerpt}",
            ))));
        }
        Ok(())
    }

    /// The shared HTTP client, built on first call. Concurrent first calls
    /// may build a transient extra client; exactly one is kept.
    fn client(&self) -> UioResult<HttpClient> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let client = build_http_client(self.allow_http)?;
        Ok(self.client.get_or_init(|| client).clone())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::super::stub::{StubResponse, stub_server};
    use super::*;

    fn compose_at(endpoint: &str, generation: Option<&str>) -> UioResult<()> {
        let compose =
            ComposeAppend::with_api_base("bucket".to_string(), Url::parse(endpoint).unwrap(), true);
        let dest = object_store::path::Path::from("dir/append.dat");
        let temp = object_store::path::Path::from("dir/append.dat.compose-append");

        io_bridge::BridgeRuntime::global().block_on(compose.compose_request(
            "stub-token",
            &dest,
            &temp,
            generation,
            5,
        ))
    }

    #[test]
    fn compose_posts_the_conditional_source_list() {
        let (endpoint, seen) = stub_server(vec![StubResponse::new(200).body("{}")]);

        compose_at(&endpoint, Some("42")).unwrap();

        let seen = seen.lock().unwrap();
        let [request] = &seen[..] else {
            panic!("expected exactly one request");
        };
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.path,
            "/storage/v1/b/bucket/o/dir%2Fappend.dat/compose?ifGenerationMatch=42"
        );
        assert!(request.signed);
        assert_eq!(
            String::from_utf8_lossy(&request.body),
            r#"{"sourceObjects":[{"name":"dir/append.dat"},{"name":"dir/append.dat.compose-append"}],"destination":{}}"#,
        );
    }

    /// Without an observed generation there is no precondition to send.
    #[test]
    fn compose_without_a_generation_sends_no_precondition() {
        let (endpoint, seen) = stub_server(vec![StubResponse::new(200).body("{}")]);

        compose_at(&endpoint, None).unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[0].path,
            "/storage/v1/b/bucket/o/dir%2Fappend.dat/compose"
        );
    }

    /// A failed generation precondition means the object was rewritten
    /// behind our back — an offset conflict, not an opaque error.
    #[test]
    fn compose_precondition_failure_is_an_offset_conflict() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(412)]);

        let err = compose_at(&endpoint, Some("42")).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );
    }

    #[test]
    fn compose_failure_surfaces_the_body_excerpt() {
        let (endpoint, _seen) =
            stub_server(vec![StubResponse::new(403).body("compose denied by stub")]);

        let err = compose_at(&endpoint, Some("42")).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        let message = err.to_string();
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("compose denied by stub"), "{message}");
    }
}
