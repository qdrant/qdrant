use crate::collection_manager::holders::segment_holder::SegmentHolder;
use crate::operations::types::{CollectionError, CollectionResult};
use segment::types::{
    PayloadInterface, PayloadKeyType, PayloadKeyTypeRef, PointIdType, SeqNumberType,
};
use std::collections::{HashMap, HashSet};

/// A collection of functions for updating points and payloads stored in segments
pub struct SegmentsUpdater {}

impl SegmentsUpdater {
    fn check_unprocessed_points(
        points: &[PointIdType],
        processed: &HashSet<PointIdType>,
    ) -> CollectionResult<usize> {
        let missed_point = points.iter().cloned().find(|p| !processed.contains(p));
        match missed_point {
            None => Ok(processed.len()),
            Some(missed_point) => Err(CollectionError::NotFound {
                missed_point_id: missed_point,
            }),
        }
    }

    /// Tries to delete points from all segments, returns number of actually deleted points
    pub fn delete_points(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        ids: &[PointIdType],
    ) -> CollectionResult<usize> {
        let res = segments.apply_points(op_num, ids, |id, write_segment| {
            write_segment.delete_point(op_num, id)
        })?;
        Ok(res)
    }

    pub fn set_payload(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        payload: &HashMap<PayloadKeyType, PayloadInterface>,
        points: &[PointIdType],
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();

        let res = segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            let mut res = true;
            for (key, payload) in payload {
                res = write_segment.set_payload(op_num, id, key, payload.into())? && res;
            }
            Ok(res)
        })?;

        SegmentsUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    pub fn delete_payload(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        points: &[PointIdType],
        keys: &[PayloadKeyType],
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();

        let res = segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            let mut res = true;
            for key in keys {
                res = write_segment.delete_payload(op_num, id, key)? && res;
            }
            Ok(res)
        })?;

        SegmentsUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    pub fn clear_payload(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        points: &[PointIdType],
    ) -> CollectionResult<usize> {
        let mut updated_points: HashSet<PointIdType> = Default::default();
        let res = segments.apply_points_to_appendable(op_num, points, |id, write_segment| {
            updated_points.insert(id);
            write_segment.clear_payload(op_num, id)
        })?;

        SegmentsUpdater::check_unprocessed_points(points, &updated_points)?;
        Ok(res)
    }

    pub fn create_field_index(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        field_name: PayloadKeyTypeRef,
    ) -> CollectionResult<usize> {
        let res = segments.apply_segments(op_num, |write_segment| {
            write_segment.create_field_index(op_num, field_name)
        })?;
        Ok(res)
    }

    pub fn delete_field_index(
        segments: &SegmentHolder,
        op_num: SeqNumberType,
        field_name: PayloadKeyTypeRef,
    ) -> CollectionResult<usize> {
        let res = segments.apply_segments(op_num, |write_segment| {
            write_segment.delete_field_index(op_num, field_name)
        })?;
        Ok(res)
    }
}
