use tonic::{Request, Response, Status};

use crate::common::points::{
    do_clear_payload, do_create_index, do_delete_index, do_delete_payload, do_delete_points,
    do_search_points, do_set_payload, do_update_points, CreateFieldIndex,
};
use crate::tonic::api::common::error_to_status;
use crate::tonic::api::conversions::*;
use crate::tonic::qdrant::points_server::Points;

use crate::tonic::qdrant::{
    ClearPayloadPoints, CreateFieldIndexCollection, DeleteFieldIndexCollection,
    DeletePayloadPoints, DeletePoints, PointsOperationResponse, SearchPoints, SearchResponse,
    SetPayloadPoints, UpsertPoints,
};
use collection::operations::payload_ops::DeletePayload;
use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointsList};
use collection::operations::types::SearchRequest;
use collection::operations::CollectionUpdateOperations;
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
            collection,
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
            &collection,
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
            collection,
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
            &collection,
            points_selector,
            wait.unwrap_or(false),
        )
        .await
        .map_err(error_to_status)?;

        let response = PointsOperationResponse::from((timing, result));
        Ok(Response::new(response))
    }

    async fn set_payload(
        &self,
        request: Request<SetPayloadPoints>,
    ) -> Result<Response<PointsOperationResponse>, Status> {
        let SetPayloadPoints {
            collection,
            wait,
            payload,
            points,
        } = request.into_inner();

        let operation = collection::operations::payload_ops::SetPayload {
            payload: payload_to_interface(payload)?,
            points: points
                .into_iter()
                .map(|p| p.try_into())
                .collect::<Result<_, _>>()?,
        };

        let timing = Instant::now();
        let result = do_set_payload(
            self.toc.as_ref(),
            &collection,
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
            collection,
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
            &collection,
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
            collection,
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
            &collection,
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
            collection,
            wait,
            field_name,
        } = request.into_inner();

        let operation = CreateFieldIndex { field_name };

        let timing = Instant::now();
        let result = do_create_index(
            self.toc.as_ref(),
            &collection,
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
            collection,
            wait,
            field_name,
        } = request.into_inner();

        let timing = Instant::now();
        let result = do_delete_index(
            self.toc.as_ref(),
            &collection,
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
            collection,
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
            with_vector,
        };

        let timing = Instant::now();
        let scored_points = do_search_points(self.toc.as_ref(), &collection, search_request)
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_grpc() {
        // For running build from IDE
        eprintln!("hello");
    }
}
