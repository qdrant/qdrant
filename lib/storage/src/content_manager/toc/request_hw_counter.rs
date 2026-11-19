use std::sync::Arc;

use common::ambient::AmbientContext;
use common::ambient::hw::{HwMetric, HwSharedDrain};
use common::ambient::profile::{ProfileAudience, ProfileMode, Profiler, QueryProfile};
use common::flags::feature_flags;

use super::TableOfContent;
use crate::content_manager::errors::StorageError;

impl TableOfContent {
    pub fn get_collection_hw_metrics(&self, collection_id: String) -> Arc<HwSharedDrain> {
        self.collection_hw_metrics
            .entry(collection_id)
            .or_default()
            .clone()
    }
}

#[derive(Clone)]
pub struct RequestHwCounter {
    counter: AmbientContext,
    /// If this flag is set, RequestHwCounter will be converted into non-None API representation.
    /// Otherwise, it will be ignored.
    report_to_api: bool,
    /// Present when the request asked for a query profile.
    profiler: Option<Profiler>,
}

/// Check that a requested query profile is allowed: the feature is in beta.
pub fn check_profile_allowed(profile: Option<ProfileMode>) -> Result<(), StorageError> {
    if profile.is_some() && !feature_flags().query_profiling {
        return Err(StorageError::bad_request(
            "Query profiling is in beta: enable it with the `feature_flags.query_profiling` \
             config option (`QDRANT__FEATURE_FLAGS__QUERY_PROFILING=true`)",
        ));
    }
    Ok(())
}

/// Explain mode is not meaningful for facet requests, see the "Query profiling" docs.
pub fn check_facet_profile(profile: Option<ProfileMode>) -> Result<(), StorageError> {
    if profile == Some(ProfileMode::Explain) {
        return Err(StorageError::bad_request(
            "`profile=explain` is not supported for facet: a facet has no strategy to choose. \
             It reads the values of one payload index (required for faceting) and counts \
             the matching points per value. Use `profile=analyze` to see where time goes.",
        ));
    }
    Ok(())
}

impl RequestHwCounter {
    pub fn new(counter: AmbientContext, report_to_api: bool) -> Self {
        Self {
            counter,
            report_to_api,
            profiler: None,
        }
    }

    /// A request counter whose context is also profiled, see [`common::ambient::profile`].
    pub fn new_profiled(
        name: &str,
        collection: Arc<HwSharedDrain>,
        report_to_api: bool,
        profile: Option<ProfileMode>,
        audience: ProfileAudience,
    ) -> Self {
        if profile.is_some() {
            // Usually set at startup with the feature flag. A peer with the flag disabled can
            // still serve the shards of a profiled request (the coordinator checked the flag):
            // count from now on, so this profile has distance counts too.
            common::ambient::hw::enable_score_counting();
        }
        let profiler = profile.map(|mode| Profiler::new(mode, audience));
        Self {
            counter: AmbientContext::request_profiled(name, collection, profiler.as_ref()),
            report_to_api,
            profiler,
        }
    }

    /// The profile collected so far, if the request is profiled.
    pub fn profile(&self) -> Option<QueryProfile> {
        self.profiler.as_ref().map(Profiler::finish)
    }

    /// The profile in its gRPC representation.
    pub fn profile_grpc(&self) -> Option<api::grpc::qdrant::QueryProfile> {
        self.profile().map(api::grpc::qdrant::QueryProfile::from)
    }

    pub fn get_counter(&self) -> AmbientContext {
        AmbientContext::clone(&self.counter)
    }

    pub fn to_rest_api(self) -> Option<api::rest::models::HardwareUsage> {
        if self.report_to_api {
            let data = self.counter.hw_data();
            let m = |metric: HwMetric| Some(data[metric]);
            Some(api::rest::models::HardwareUsage {
                cpu: m(HwMetric::Cpu),
                payload_io_read: m(HwMetric::PayloadIoRead),
                payload_io_write: m(HwMetric::PayloadIoWrite),
                payload_index_io_read: m(HwMetric::PayloadIndexIoRead),
                payload_index_io_write: m(HwMetric::PayloadIndexIoWrite),
                vector_io_read: m(HwMetric::VectorIoRead),
                vector_io_write: m(HwMetric::VectorIoWrite),
                remote_io_cached_read: m(HwMetric::RemoteIoCachedRead),
                remote_io_non_cached_read: m(HwMetric::RemoteIoNonCachedRead),
                updated_bytes: None,
                updated_indexed_bytes: None,
            })
        } else {
            None
        }
    }

    pub fn to_grpc_api(self) -> Option<api::grpc::qdrant::HardwareUsage> {
        self.report_to_api
            .then(|| api::grpc::qdrant::HardwareUsage::from(self.counter.hw_data()))
    }
}
