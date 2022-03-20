use tonic::{Request, Response, Status};

use crate::common::points::{
    do_clear_payload, do_create_index, do_delete_index, do_delete_payload, do_delete_points,
    do_get_points, do_scroll_points, do_search_points, do_set_payload, do_update_points,
    CreateFieldIndex,
};
use crate::tonic::api::common::error_to_status;
use crate::tonic::qdrant::points_server::Points;

use crate::tonic::api::conversions::proto_to_payloads;
use crate::tonic::qdrant::{
    ClearPayloadPoints, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePoints, FieldType, GetPoints, GetResponse, PointsOperationResponse,
    RecommendPoints, RecommendResponse, ScrollPoints, ScrollResponse, SearchPoints, SearchResponse,
    SetPayloadPoints, UpsertPoints,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsList};
use collection::operations::types::{PointRequest, ScrollRequest, SearchRequest};
use collection::operations::CollectionUpdateOperations;
use segment::types::PayloadSchemaType;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Instant;
use storage::content_manager::toc::TableOfContent;

pub struct PointsService {
    toc: Arc<TableOfContent>,
}

impl PointsService {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self { toc }
    }
}

#[tonic::async_trait]
impl Points for PointsService {
    async fn upsert(
        &self,
        request: Request<UpsertPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let UpsertPoints {
            collection_name,
            wait,
            points,
        } = request.into_inner();

        let points = points
            .into_iter()
            .map(|point| point.try_into())
            .collect::<Result<_, _>>()?;

        let operation = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperations::PointsList(PointsList { points }),
        ));

        let timing = Instant::now();
        let result = do_update_points(
            self.toc.as_ref(),
            &collection_name,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeletePoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePoints {
            collection_name,
            wait,
            points,
        } = request.into_inner();

        let points_selector = match points {
            None => return Err(Status::invalid_argument("PointSelector is missing")),
            Some(p) => p.try_into()?,
        };

        let timing = Instant::now();
        let result = do_delete_points(
            self.toc.as_ref(),
            &collection_name,
            points_selector,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn get(&self, request: Request<GetPoints>) -> Result<Response<GetResponse>, Status> {
        let GetPoints {
            collection_name,
            ids,
            with_vector,
            with_payload,
        } = request.into_inner();

        let point_request = PointRequest {
            ids: ids
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
            with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: with_vector.unwrap_or(false),
        };

        let timing = Instant::now();

        let records = do_get_points(self.toc.as_ref(), &collection_name, point_request)
            .await
            .map_err(error_to_status)?;

        let response = GetResponse {
            result: records.into_iter().map(|point| point.into()).collect(),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let SetPayloadPoints {
            collection_name,
            wait,
            payload,
            points,
        } = request.into_inner();

        let operation = collection::operations::payload_ops::SetPayload {
            payload: proto_to_payloads(payload)?,
            points: points
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
        };

        let timing = Instant::now();
        let result = do_set_payload(
            self.toc.as_ref(),
            &collection_name,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete_payload(
        &self,
        request: Request<DeletePayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeletePayloadPoints {
            collection_name,
            wait,
            keys,
            points,
        } = request.into_inner();

        let operation = DeletePayload {
            keys,
            points: points
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
        };

        let timing = Instant::now();
        let result = do_delete_payload(
            self.toc.as_ref(),
            &collection_name,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn clear_payload(
        &self,
        request: Request<ClearPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let ClearPayloadPoints {
            collection_name,
            wait,
            points,
        } = request.into_inner();

        let points_selector = match points {
            None => return Err(Status::invalid_argument("PointSelector is missing")),
            Some(p) => p.try_into()?,
        };

        let timing = Instant::now();
        let result = do_clear_payload(
            self.toc.as_ref(),
            &collection_name,
            points_selector,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn create_field_index(
        &self,
        request: Request<CreateFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let CreateFieldIndexCollection {
            collection_name,
            wait,
            field_name,
            field_type,
        } = request.into_inner();

        let field_type = match field_type {
            None => None,
            Some(f) => match FieldType::from_i32(f) {
                None => return Err(Status::invalid_argument("cannot convert field_type")),
                Some(v) => match v {
                    FieldType::Keyword => Some(PayloadSchemaType::Keyword),
                    FieldType::Integer => Some(PayloadSchemaType::Integer),
                    FieldType::Float => Some(PayloadSchemaType::Float),
                    FieldType::Geo => Some(PayloadSchemaType::Geo),
                },
            },
        };

        let operation = CreateFieldIndex {
            field_name,
            field_type,
        };

        let timing = Instant::now();
        let result = do_create_index(
            self.toc.as_ref(),
            &collection_name,
            operation,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn delete_field_index(
        &self,
        request: Request<DeleteFieldIndexCollection>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let DeleteFieldIndexCollection {
            collection_name,
            wait,
            field_name,
        } = request.into_inner();

        let timing = Instant::now();
        let result = do_delete_index(
            self.toc.as_ref(),
            &collection_name,
            field_name,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn search(
        &self,
        request: Request<SearchPoints>,
    ) -> Result<Response<SearchResponse>, Status> {
        let SearchPoints {
            collection_name,
            vector,
            filter,
            top,
            with_vector,
            with_payload,
            params,
        } = request.into_inner();

        let search_request = SearchRequest {
            vector,
            filter: filter.map(|f| f.try_into()).transpose()?,
            params: params.map(|p| p.into()),
            top: top as usize,
            with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: with_vector.unwrap_or(false),
        };

        let timing = Instant::now();
        let scored_points = do_search_points(self.toc.as_ref(), &collection_name, search_request)
            .await
            .map_err(error_to_status)?;

        let response = SearchResponse {
            result: scored_points
                .into_iter()
                .map(|point| point.into())
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }

    async fn scroll(
        &self,
        request: Request<ScrollPoints>,
    ) -> Result<Response<ScrollResponse>, Status> {
        let ScrollPoints {
            collection_name,
            filter,
            offset,
            limit,
            with_vector,
            with_payload,
        } = request.into_inner();

        let scroll_request = ScrollRequest {
            offset: offset.map(|o| o.try_into()).transpose()?,
            limit: limit.map(|l| l as usize),
            filter: filter.map(|f| f.try_into()).transpose()?,
            with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: with_vector.unwrap_or(false),
        };

        let timing = Instant::now();
        let scrolled_points = do_scroll_points(self.toc.as_ref(), &collection_name, scroll_request)
            .await
            .map_err(error_to_status)?;

        let response = ScrollResponse {
            next_page_offset: scrolled_points.next_page_offset.map(|n| n.into()),
            result: scrolled_points
                .points
                .into_iter()
                .map(|point| point.into())
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }

    async fn recommend(
        &self,
        request: Request<RecommendPoints>,
    ) -> Result<Response<RecommendResponse>, Status> {
        let RecommendPoints {
            collection_name,
            positive,
            negative,
            filter,
            top,
            with_vector,
            with_payload,
            params,
        } = request.into_inner();

        let request = collection::operations::types::RecommendRequest {
            positive: positive
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
            negative: negative
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
            filter: filter.map(|f| f.try_into()).transpose()?,
            params: params.map(|p| p.into()),
            top: top as usize,
            with_payload: with_payload.map(|wp| wp.try_into()).transpose()?,
            with_vector: with_vector.unwrap_or(false),
        };

        let timing = Instant::now();
        let recommended_points = self
            .toc
            .as_ref()
            .recommend(&collection_name, request)
            .await
            .map_err(error_to_status)?;

        let response = RecommendResponse {
            result: recommended_points
                .into_iter()
                .map(|point| point.into())
                .collect(),
            time: timing.elapsed().as_secs_f64(),
        };

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_grpc() {
        // For running build from IDE
        eprintln!("hello");
    }
}
