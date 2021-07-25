pub mod api;
#[allow(dead_code)] // May contain functions used in different binaries. Not actually dead
pub mod helpers;

use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{error, get, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;

use crate::actix::api::collections_api::{get_collection, get_collections, update_collections};
use crate::actix::api::recommend_api::recommend_points;
use crate::actix::api::retrieve_api::{get_point, get_points, scroll_points};
use crate::actix::api::search_api::search_points;
use crate::actix::api::update_api::update_points;
use crate::common::models::VersionInfo;
use crate::settings::Settings;

fn json_error_handler(err: error::JsonPayloadError, _req: &HttpRequest) -> error::Error {
    use actix_web::error::JsonPayloadError;

    let detail = err.to_string();
    let resp = match &err {
        JsonPayloadError::ContentType => HttpResponse::UnsupportedMediaType().body(detail),
        JsonPayloadError::Deserialize(json_err) if json_err.is_data() => {
            HttpResponse::UnprocessableEntity().body(detail)
        }
        _ => HttpResponse::BadRequest().body(detail),
    };
    error::InternalError::from_response(err, resp).into()
}

#[get("/")]
pub async fn index() -> impl Responder {
    HttpResponse::Ok().json(VersionInfo::default())
}

#[allow(dead_code)]
pub fn init(toc: Arc<TableOfContent>, settings: Settings) -> std::io::Result<()> {
    actix_web::rt::System::new().block_on(async {
        let toc_data = web::Data::new(toc);
        HttpServer::new(move || {
            App::new()
                .wrap(Logger::default())
                .app_data(toc_data.clone())
                .app_data(Data::new(
                    web::JsonConfig::default()
                        .limit(32 * 1024 * 1024)
                        .error_handler(json_error_handler),
                )) // 32 Mb
                .service(index)
                .service(get_collections)
                .service(update_collections)
                .service(get_collection)
                .service(update_points)
                .service(get_point)
                .service(get_points)
                .service(scroll_points)
                .service(search_points)
                .service(recommend_points)
        })
        // .workers(4)
        .bind(format!(
            "{}:{}",
            settings.service.host, settings.service.port
        ))?
        .run()
        .await
    })
}
