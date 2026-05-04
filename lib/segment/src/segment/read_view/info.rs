use std::collections::HashMap;

use common::types::TelemetryDetail;
use uuid::Uuid;

use crate::id_tracker::IdTrackerRead;
use crate::index::{PayloadIndexRead, VectorIndexRead};
use crate::payload_storage::PayloadStorageRead;
use crate::segment::read_view::SegmentReadView;
use crate::segment::vector_data_read::VectorDataRead;
use crate::telemetry::SegmentTelemetry;
use crate::types::{PayloadIndexInfo, SegmentConfig, SegmentInfo, SegmentType, VectorDataInfo};
use crate::vector_storage::VectorStorageRead;

impl<'s, TIdT, TPI, TPS, TVD> SegmentReadView<'s, TIdT, TPI, TPS, TVD>
where
    TIdT: IdTrackerRead,
    TPI: PayloadIndexRead,
    TPS: PayloadStorageRead,
    TVD: VectorDataRead,
{
    /// Build a `SegmentInfo` describing this segment's storage. The trivial
    /// segment-level fields (`uuid`, `segment_type`, `is_appendable`) come from
    /// the caller — typically the segment-type-specific direct getters.
    ///
    /// `index_schema` is left empty; use [`build_info`] to also populate it.
    pub fn build_size_info(
        &self,
        uuid: Uuid,
        segment_type: SegmentType,
        is_appendable: bool,
    ) -> SegmentInfo {
        let mut total_average_vectors_size_bytes: usize = 0;
        let mut num_vectors_total: usize = 0;
        let mut num_indexed_vectors_total: usize = 0;
        let segment_is_indexed = segment_type == SegmentType::Indexed;

        let vector_data_info: HashMap<_, _> = self
            .vector_data
            .iter()
            .map(|(key, vector_data)| {
                let vector_storage = vector_data.vector_storage();
                let vector_index = vector_data.vector_index();
                let num_vectors = vector_storage.available_vector_count();
                num_vectors_total += num_vectors;

                let average_vector_size_bytes = vector_index
                    .size_of_searchable_vectors_in_bytes()
                    .checked_div(num_vectors)
                    .unwrap_or(0);
                total_average_vectors_size_bytes += average_vector_size_bytes;

                let indexed_vector_count = vector_index.indexed_vector_count();
                let num_indexed_vectors = if vector_index.is_index() {
                    indexed_vector_count
                } else {
                    0
                };
                if segment_is_indexed {
                    num_indexed_vectors_total += indexed_vector_count;
                }

                let info = VectorDataInfo {
                    num_vectors,
                    num_indexed_vectors,
                    num_deleted_vectors: vector_storage.deleted_vector_count(),
                };
                (key.clone(), info)
            })
            .collect();

        let vectors_size_bytes =
            total_average_vectors_size_bytes * self.id_tracker.available_point_count();

        // Unwrap and default to 0 here because the RocksDB storage is the only fallible one,
        // and we will remove it eventually.
        let payloads_size_bytes = self.payload_storage.get_storage_size_bytes().unwrap_or(0);

        SegmentInfo {
            uuid,
            segment_type,
            num_vectors: num_vectors_total,
            num_indexed_vectors: num_indexed_vectors_total,
            num_points: self.id_tracker.available_point_count(),
            num_deferred_points: Some(self.deferred_point_count()),
            num_deleted_deferred_points: Some(self.deferred_deleted_count().unwrap_or_default()),
            num_deleted_vectors: self.id_tracker.deleted_point_count(),
            vectors_size_bytes,  // Considers vector storage, but not indices.
            payloads_size_bytes, // Considers payload storage, but not indices.
            ram_usage_bytes: 0,  // ToDo: Implement.
            disk_usage_bytes: 0, // ToDo: Implement.
            is_appendable,
            index_schema: HashMap::new(),
            vector_data: vector_data_info,
            deferred_internal_id: self.deferred_internal_id(),
        }
    }

    /// Like [`build_size_info`] but additionally populates `index_schema`.
    pub fn build_info(
        &self,
        uuid: Uuid,
        segment_type: SegmentType,
        is_appendable: bool,
    ) -> SegmentInfo {
        let mut info = self.build_size_info(uuid, segment_type, is_appendable);
        info.index_schema = self
            .payload_index
            .indexed_fields()
            .into_iter()
            .map(|(key, index_schema)| {
                let points_count = self.payload_index.indexed_points(&key);
                let index_info = PayloadIndexInfo::new(index_schema, points_count);
                (key, index_info)
            })
            .collect();
        info
    }

    /// Build the segment-level telemetry payload.
    pub fn build_telemetry(
        &self,
        uuid: Uuid,
        segment_type: SegmentType,
        is_appendable: bool,
        config: &SegmentConfig,
        detail: TelemetryDetail,
    ) -> SegmentTelemetry {
        let vector_index_searches = self
            .vector_data
            .iter()
            .map(|(name, vector_data)| {
                let mut telemetry = vector_data.vector_index().get_telemetry_data(detail);
                telemetry.index_name = Some(name.clone());
                telemetry
            })
            .collect();

        SegmentTelemetry {
            info: self.build_info(uuid, segment_type, is_appendable),
            config: config.clone(),
            vector_index_searches,
            payload_field_indices: self.payload_index.get_telemetry_data(),
        }
    }
}
