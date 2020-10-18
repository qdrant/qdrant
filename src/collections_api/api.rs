use storage::content_manager::toc::TableOfContent;
use actix_web::{get, web, Responder};
use crate::collections_api::models::{CollectionDescription, CollectionsResponse};
use itertools::Itertools;
use crate::common::helpers::process_response;
use actix_web::rt::time::Instant;

#[get("/collections")]
pub async fn collections(
    toc: web::Data<TableOfContent>
) -> impl Responder {
    let timing = Instant::now();

    let response = {
        let collections = toc
            .all_collections()
            .into_iter()
            .map(|name| CollectionDescription { name })
            .collect_vec();

        Ok(CollectionsResponse { collections })
    };

    process_response(response, timing)
}


