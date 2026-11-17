//! AWS S3 backend.

use common::universal_io::{UioResult, UniversalIoError, UniversalKind};
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, Checksum};
use url::Url;

use crate::append::{
    AppendContext, NativeAppend, PartCopyAppend, SignedRequestContext, XmlComposeAppend,
};
use crate::backend::BlobBackend;

/// SigV4 signing needs a region even for endpoints that ignore it.
const DEFAULT_REGION: &str = "us-east-1";

/// Host of GCS's S3-compatible XML API. Buckets are addressed either by
/// path under it (`storage.googleapis.com/{bucket}`) or as its subdomain
/// (`{bucket}.storage.googleapis.com`).
const GCS_XML_API_HOST: &str = "storage.googleapis.com";

/// How an endpoint addresses GCS's S3-compatible XML API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GcsXmlApi {
    /// `storage.googleapis.com`: objects at `/{bucket}/{key}`.
    PathStyle,
    /// `{bucket}.storage.googleapis.com`: the bucket is in the host,
    /// objects at `/{key}`.
    VirtualHosted,
}

/// Connection parameters for [`AmazonS3`]. Fed into [`AmazonS3Builder`] by
/// the [`BlobBackend`] impl below.
#[derive(Clone, Debug)]
pub struct AwsConfig {
    /// S3 bucket name (without `s3://` prefix).
    pub bucket: String,
    /// AWS region (e.g. `us-east-1`). Required for real AWS; optional for
    /// S3-compatible endpoints that ignore it.
    pub region: Option<String>,
    /// Custom endpoint URL — set this for MinIO / RustFS / LocalStack.
    /// `with_allow_http(true)` is enabled when this is set. GCS's XML API
    /// is recognized as `https://storage.googleapis.com` (path-style) or
    /// `https://{bucket}.storage.googleapis.com` (virtual-hosted; the
    /// subdomain must be `bucket`).
    pub endpoint: Option<String>,
    /// Enable S3 Express One Zone (`*--x-s3` directory buckets).
    pub s3_express: bool,
    /// The store honors native write-offset appends (`PutObject` +
    /// `x-amz-write-offset-bytes`). Not detectable from the endpoint — the
    /// operator declares it for stores like MinIO AiStor or RustFS;
    /// `s3_express` implies it. Left off (plain S3 Standard), appends go
    /// through multipart rewrites with server-side prefix copies — or, on
    /// GCS's XML API endpoint (detected from `endpoint`), through `compose`.
    pub native_append: bool,
    /// How to authenticate to S3.
    pub credentials: AwsCredentials,
}

/// Authentication mode for AWS S3.
#[derive(Clone, Debug)]
pub enum AwsCredentials {
    /// AWS default credential chain, resolved from the environment by `object_store`.
    Default,
    /// Hard-coded access key + secret + optional session token. Useful for
    /// tests and for short-lived credentials provided out-of-band.
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

impl BlobBackend for AmazonS3 {
    type Config = AwsConfig;

    fn build_store(config: &Self::Config) -> UioResult<Self> {
        store_builder(config, |name| std::env::var(name).ok())?
            .build()
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("AmazonS3Builder: {err}"),
            })
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }

    fn append_context(config: &Self::Config) -> UioResult<Option<AppendContext>> {
        let s3_config_error = |description: String| UniversalIoError::S3Config { description };

        let (endpoint, region) =
            resolve_endpoint_and_region(config, |name| std::env::var(name).ok());

        let gcs = match &endpoint {
            Some(endpoint) => gcs_xml_api(endpoint, &config.bucket)?,
            None => None,
        };

        let (object_url_base, service) = match &endpoint {
            // Path-style addressing, as object_store uses for custom
            // endpoints. An explicit endpoint stays the operator override
            // (VPC endpoints, FIPS, MinIO & co) and keeps plain "s3"
            // signing.
            Some(endpoint) => (
                endpoint_object_url_base(endpoint, &config.bucket, gcs)?,
                "s3",
            ),
            // S3 Express directory buckets only exist on their zonal
            // endpoint and reject signatures scoped to "s3".
            None if config.s3_express => {
                let az = directory_bucket_az(&config.bucket).ok_or_else(|| {
                    s3_config_error(format!(
                        "s3_express appends need a directory bucket named \
                         `<name>--<az>--x-s3`, got `{}`",
                        config.bucket,
                    ))
                })?;
                let url = format!(
                    "https://{}.s3express-{az}.{region}.amazonaws.com",
                    config.bucket,
                );
                let url = Url::parse(&url)
                    .map_err(|err| s3_config_error(format!("append object url: {err}")))?;
                (url, "s3express")
            }
            // Virtual-hosted-style addressing, as object_store uses for real
            // AWS.
            None => {
                let url = format!("https://{}.s3.{region}.amazonaws.com", config.bucket);
                let url = Url::parse(&url)
                    .map_err(|err| s3_config_error(format!("append object url: {err}")))?;
                (url, "s3")
            }
        };

        // Allowing plain http mirrors `build_store`: custom endpoints
        // (MinIO & co) are commonly plain http. The HTTP client itself is
        // built lazily on first append.
        let allow_http = endpoint.is_some();

        let context = SignedRequestContext::new(
            allow_http,
            config.bucket.clone(),
            object_url_base,
            region,
            service,
        );

        Ok(Some(if config.s3_express || config.native_append {
            AppendContext::Native(NativeAppend::new(context))
        } else if gcs.is_some() {
            // GCS implements no `UploadPartCopy`, the part-copy rewrite
            // cannot run there.
            AppendContext::XmlCompose(XmlComposeAppend::new(context))
        } else {
            AppendContext::PartCopy(PartCopyAppend::new(context))
        }))
    }
}

/// The [`AmazonS3Builder`] for `config`, with `env` standing in for the
/// process environment when resolving the endpoint.
fn store_builder(
    config: &AwsConfig,
    env: impl Fn(&str) -> Option<String>,
) -> UioResult<AmazonS3Builder> {
    let mut builder = match &config.credentials {
        AwsCredentials::Default => AmazonS3Builder::from_env(),
        AwsCredentials::Static {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            let mut b = AmazonS3Builder::new()
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
            if let Some(t) = session_token {
                b = b.with_token(t);
            }
            b
        }
    };
    builder = builder
        .with_bucket_name(&config.bucket)
        .with_s3_express(config.s3_express);
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }
    let (endpoint, _region) = resolve_endpoint_and_region(config, env);
    let gcs = match &endpoint {
        Some(endpoint) => gcs_xml_api(endpoint, &config.bucket)?,
        None => None,
    };
    if let Some(gcs) = gcs {
        // GCS's XML API has no bulk `DeleteObjects`: delete one object at a
        // time there, as for the other S3-compatible stores lacking it.
        builder = builder.with_disable_bulk_delete(true);
        // Address the bucket the way the endpoint names it, set explicitly
        // so the store and the append context (which builds object URLs
        // itself) agree whatever the environment says.
        builder = match gcs {
            GcsXmlApi::PathStyle => builder.with_virtual_hosted_style_request(false),
            GcsXmlApi::VirtualHosted => {
                // `object_store` uses a virtual-hosted endpoint verbatim as
                // the bucket URL: strip any path (a trailing `/` would
                // double the separator before keys). Set as the
                // S3-specific endpoint, which wins over the generic one.
                let url = Url::parse(endpoint.as_deref().unwrap_or_default()).map_err(|err| {
                    UniversalIoError::S3Config {
                        description: format!("endpoint url: {err}"),
                    }
                })?;
                builder
                    .with_virtual_hosted_style_request(true)
                    .with_config(AmazonS3ConfigKey::S3Endpoint, virtual_hosted_endpoint(&url))
            }
        };
    }
    Ok(builder.with_checksum_algorithm(Checksum::CRC64NVME))
}

/// The object-URL base for appends through an explicit `endpoint`,
/// matching where `store_builder` makes the store put objects: path-style
/// `{endpoint}/{bucket}`, or the bare origin of a virtual-hosted GCS
/// endpoint, whose host already names the bucket.
fn endpoint_object_url_base(
    endpoint: &str,
    bucket: &str,
    gcs: Option<GcsXmlApi>,
) -> UioResult<Url> {
    let s3_config_error = |description: String| UniversalIoError::S3Config { description };
    let mut url = Url::parse(endpoint)
        .map_err(|err| s3_config_error(format!("append endpoint url: {err}")))?;
    match gcs {
        Some(GcsXmlApi::VirtualHosted) => {
            url = Url::parse(&virtual_hosted_endpoint(&url))
                .map_err(|err| s3_config_error(format!("append endpoint url: {err}")))?;
        }
        Some(GcsXmlApi::PathStyle) | None => {
            url.path_segments_mut()
                .map_err(|()| s3_config_error("append endpoint url cannot be a base".into()))?
                .pop_if_empty()
                .push(bucket);
        }
    }
    Ok(url)
}

/// Whether, and how, `endpoint` addresses GCS's S3-compatible XML API for
/// `bucket`. A `{name}.storage.googleapis.com` endpoint naming a bucket
/// other than `bucket` is a configuration error: requests would silently
/// go to the wrong bucket.
fn gcs_xml_api(endpoint: &str, bucket: &str) -> UioResult<Option<GcsXmlApi>> {
    let Some(host) = Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
    else {
        return Ok(None);
    };
    if host == GCS_XML_API_HOST {
        return Ok(Some(GcsXmlApi::PathStyle));
    }
    let Some(host_bucket) = host
        .strip_suffix(GCS_XML_API_HOST)
        .and_then(|prefix| prefix.strip_suffix('.'))
        .filter(|prefix| !prefix.is_empty())
    else {
        return Ok(None);
    };
    if !host_bucket.eq_ignore_ascii_case(bucket) {
        return Err(UniversalIoError::S3Config {
            description: format!(
                "endpoint {endpoint} addresses GCS bucket `{host_bucket}`, \
                 but the configured bucket is `{bucket}`",
            ),
        });
    }
    Ok(Some(GcsXmlApi::VirtualHosted))
}

/// The bucket URL of a virtual-hosted endpoint: its origin, without path.
fn virtual_hosted_endpoint(url: &Url) -> String {
    url.origin().ascii_serialization()
}

/// The availability-zone segment of a directory-bucket name
/// (`<name>--<az>--x-s3`), from which the zonal S3 Express endpoint is
/// derived. `object_store`'s builder performs the same derivation
/// internally (object_store 0.14.1 `aws/builder.rs`) but does not expose
/// it, hence the duplication.
fn directory_bucket_az(bucket: &str) -> Option<&str> {
    let (_, az) = bucket.strip_suffix("--x-s3")?.rsplit_once("--")?;
    (!az.is_empty()).then_some(az)
}

/// Resolve the endpoint and signing region for appends the same way
/// [`BlobBackend::build_store`] resolves them for every other request:
/// explicit config fields first, and — only with the default credential
/// chain, where the store is built via [`AmazonS3Builder::from_env`] — the
/// same environment variables `from_env` honors. Without this, reads and
/// appends could target different hosts or signing regions.
fn resolve_endpoint_and_region(
    config: &AwsConfig,
    env: impl Fn(&str) -> Option<String>,
) -> (Option<String>, String) {
    let from_env = |names: &[&str]| {
        matches!(config.credentials, AwsCredentials::Default)
            .then(|| names.iter().find_map(|name| env(name)))
            .flatten()
    };

    let endpoint = config.endpoint.clone().or_else(|| {
        // `AWS_ENDPOINT_URL_S3` wins over the generic names, as in
        // `AmazonS3Builder::from_env`.
        from_env(&["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"])
    });

    let region = config
        .region
        .clone()
        .or_else(|| from_env(&["AWS_REGION", "AWS_DEFAULT_REGION"]))
        .unwrap_or_else(|| DEFAULT_REGION.to_string());

    (endpoint, region)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(credentials: AwsCredentials) -> AwsConfig {
        AwsConfig {
            bucket: "bucket".to_string(),
            region: None,
            endpoint: None,
            s3_express: false,
            native_append: false,
            credentials,
        }
    }

    #[test]
    fn append_endpoint_and_region_resolve_like_build_store() {
        let env = |name: &str| match name {
            "AWS_ENDPOINT" => Some("http://minio:9000".to_string()),
            "AWS_REGION" => Some("eu-west-1".to_string()),
            _ => None,
        };

        // Default credential chain: the environment applies.
        let (endpoint, region) = resolve_endpoint_and_region(&config(AwsCredentials::Default), env);
        assert_eq!(endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(region, "eu-west-1");

        // Explicit config fields win over the environment.
        let mut explicit = config(AwsCredentials::Default);
        explicit.endpoint = Some("http://other:9000".to_string());
        explicit.region = Some("us-west-2".to_string());
        let (endpoint, region) = resolve_endpoint_and_region(&explicit, env);
        assert_eq!(endpoint.as_deref(), Some("http://other:9000"));
        assert_eq!(region, "us-west-2");

        // Static credentials never consult the environment, mirroring
        // `build_store` (which only uses `from_env` for `Default`).
        let static_credentials = AwsCredentials::Static {
            access_key_id: "id".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        };
        let (endpoint, region) = resolve_endpoint_and_region(&config(static_credentials), env);
        assert_eq!(endpoint, None);
        assert_eq!(region, DEFAULT_REGION);

        // `AWS_ENDPOINT_URL_S3` wins over the generic endpoint names.
        let env_s3 = |name: &str| match name {
            "AWS_ENDPOINT_URL_S3" => Some("http://s3:9000".to_string()),
            "AWS_ENDPOINT" => Some("http://generic:9000".to_string()),
            _ => None,
        };
        let (endpoint, _) = resolve_endpoint_and_region(&config(AwsCredentials::Default), env_s3);
        assert_eq!(endpoint.as_deref(), Some("http://s3:9000"));
    }

    #[test]
    fn directory_bucket_az_parses_the_mandatory_suffix() {
        assert_eq!(
            directory_bucket_az("my-bucket--use1-az4--x-s3"),
            Some("use1-az4")
        );
        // Extra `--` in the name: the az is the last segment before the
        // suffix.
        assert_eq!(
            directory_bucket_az("a--b--use1-az4--x-s3"),
            Some("use1-az4")
        );
        assert_eq!(directory_bucket_az("plain-bucket"), None);
        assert_eq!(directory_bucket_az("no-az--x-s3"), None);
        assert_eq!(directory_bucket_az("empty-az----x-s3"), None);
    }

    /// `s3_express` without a parseable directory-bucket suffix cannot
    /// derive the zonal endpoint and must fail loudly at construction.
    #[test]
    fn s3_express_append_context_requires_a_directory_bucket_name() {
        let mut express = config(AwsCredentials::Static {
            access_key_id: "id".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        });
        express.s3_express = true;
        express.region = Some("us-east-1".to_string());

        let err = <AmazonS3 as BlobBackend>::append_context(&express).unwrap_err();
        assert!(matches!(err, UniversalIoError::S3Config { .. }), "{err}");

        express.bucket = "bucket--use1-az4--x-s3".to_string();
        let context = <AmazonS3 as BlobBackend>::append_context(&express).unwrap();
        assert!(matches!(context, Some(AppendContext::Native(_))));
    }

    /// The GCS XML API endpoint gets the compose strategy: part-copy cannot
    /// run there. Other endpoints keep part-copy, and declared native
    /// appends still win.
    #[test]
    fn gcs_xml_api_endpoint_appends_by_compose() {
        let with_endpoint = |endpoint: &str| {
            let mut config = config(AwsCredentials::Static {
                access_key_id: "id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            });
            config.endpoint = Some(endpoint.to_string());
            config
        };

        for endpoint in [
            "https://storage.googleapis.com",
            "https://storage.googleapis.com/",
            "https://STORAGE.googleapis.com",
            "https://bucket.storage.googleapis.com",
            "https://BUCKET.storage.googleapis.com/",
        ] {
            let context = <AmazonS3 as BlobBackend>::append_context(&with_endpoint(endpoint));
            assert!(
                matches!(context, Ok(Some(AppendContext::XmlCompose(_)))),
                "{endpoint}",
            );
        }

        for endpoint in [
            "http://minio:9000",
            "https://storage.example.com",
            "https://bucketstorage.googleapis.com",
            "https://.storage.googleapis.com",
            "https://bucket.storage.googleapis.com.example.com",
        ] {
            let context = <AmazonS3 as BlobBackend>::append_context(&with_endpoint(endpoint));
            assert!(
                matches!(context, Ok(Some(AppendContext::PartCopy(_)))),
                "{endpoint}",
            );
        }

        let mut native = with_endpoint("https://storage.googleapis.com");
        native.native_append = true;
        let context = <AmazonS3 as BlobBackend>::append_context(&native);
        assert!(matches!(context, Ok(Some(AppendContext::Native(_)))));

        // A subdomain naming another bucket would silently address it.
        let other = with_endpoint("https://other.storage.googleapis.com");
        assert!(matches!(
            <AmazonS3 as BlobBackend>::append_context(&other),
            Err(UniversalIoError::S3Config { .. }),
        ));
        assert!(matches!(
            <AmazonS3 as BlobBackend>::build_store(&other),
            Err(UniversalIoError::S3Config { .. }),
        ));
    }

    /// Appends address objects where the store puts them: under
    /// `/{bucket}` on the path-style endpoint, at the root of the
    /// virtual-hosted one (whose host already names the bucket).
    #[test]
    fn gcs_xml_api_object_urls_agree_with_the_store() {
        use object_store::signer::Signer as _;

        let key = object_store::path::Path::from("dir/append.dat");
        for (endpoint, expected) in [
            (
                "https://storage.googleapis.com",
                "https://storage.googleapis.com/bucket/dir/append.dat",
            ),
            (
                "https://storage.googleapis.com/",
                "https://storage.googleapis.com/bucket/dir/append.dat",
            ),
            (
                "https://bucket.storage.googleapis.com",
                "https://bucket.storage.googleapis.com/dir/append.dat",
            ),
            (
                "https://bucket.storage.googleapis.com/",
                "https://bucket.storage.googleapis.com/dir/append.dat",
            ),
        ] {
            let mut config = config(AwsCredentials::Static {
                access_key_id: "id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            });
            config.endpoint = Some(endpoint.to_string());

            let gcs = gcs_xml_api(endpoint, &config.bucket).unwrap();
            let mut append_url = endpoint_object_url_base(endpoint, &config.bucket, gcs).unwrap();
            append_url
                .path_segments_mut()
                .unwrap()
                .pop_if_empty()
                .extend(key.parts().map(|part| part.as_ref().to_string()));
            assert_eq!(append_url.as_str(), expected, "append, {endpoint}");

            let store = <AmazonS3 as BlobBackend>::build_store(&config).unwrap();
            let mut store_url = io_bridge::BridgeRuntime::global()
                .block_on(store.signed_url(
                    http::Method::GET,
                    &key,
                    std::time::Duration::from_secs(60),
                ))
                .unwrap();
            store_url.set_query(None);
            assert_eq!(store_url.as_str(), expected, "store, {endpoint}");
        }
    }

    /// GCS's XML API has no bulk `DeleteObjects`, so the store deletes one
    /// object at a time there, whether the endpoint is configured or comes
    /// from the environment.
    #[test]
    fn gcs_xml_api_endpoint_disables_bulk_delete() {
        let bulk_delete_disabled = |config: &AwsConfig, env: &dyn Fn(&str) -> Option<String>| {
            store_builder(config, env)
                .unwrap()
                .get_config_value(&AmazonS3ConfigKey::DisableBulkDelete)
                == Some("true".to_string())
        };
        let no_env = |_: &str| None;

        let mut gcs = config(AwsCredentials::Default);
        gcs.endpoint = Some("https://storage.googleapis.com".to_string());
        assert!(bulk_delete_disabled(&gcs, &no_env));

        let mut gcs_subdomain = config(AwsCredentials::Default);
        gcs_subdomain.endpoint = Some("https://bucket.storage.googleapis.com".to_string());
        assert!(bulk_delete_disabled(&gcs_subdomain, &no_env));

        let gcs_env = |name: &str| {
            (name == "AWS_ENDPOINT").then(|| "https://storage.googleapis.com".to_string())
        };
        assert!(bulk_delete_disabled(
            &config(AwsCredentials::Default),
            &gcs_env
        ));

        let mut minio = config(AwsCredentials::Default);
        minio.endpoint = Some("http://minio:9000".to_string());
        assert!(!bulk_delete_disabled(&minio, &no_env));
    }
}
