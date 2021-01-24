#[macro_use]
extern crate log;

mod settings;

mod common;
mod api;

use actix_web::middleware::Logger;

use actix_web::{get, web, App, HttpServer, error, HttpRequest, HttpResponse, Responder};

use env_logger;
use storage::content_manager::toc::TableOfContent;
use crate::api::collections_api::{get_collections, update_collections, get_collection};
use crate::api::update_api::update_points;
use crate::api::retrieve_api::{get_vectors, get_point};
use crate::api::search_api::search_points;
use serde::{Deserialize, Serialize};
use crate::api::recommend_api::recommend_points;

#[derive(Serialize, Deserialize)]
pub struct VersionInfo {
    pub title: String,
    pub version: String
}

fn json_error_handler(err: error::JsonPayloadError, _req: &HttpRequest) -> error::Error {
    use actix_web::error::JsonPayloadError;

    let detail = err.to_string();
    let resp = match &err {
        JsonPayloadError::ContentType => {
            HttpResponse::UnsupportedMediaType().body(detail)
        }
        JsonPayloadError::Deserialize(json_err) if json_err.is_data() => {
            HttpResponse::UnprocessableEntity().body(detail)
        }
        _ => HttpResponse::BadRequest().body(detail),
    };
    error::InternalError::from_response(err, resp).into()
}

#[get("/")]
pub async fn index() -> impl Responder {
    HttpResponse::Ok().json(VersionInfo {
        title: "qdrant - vector search engine".to_string(),
        version: option_env!("CARGO_PKG_VERSION").unwrap().to_string()
    })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG", settings.log_level);
    env_logger::init();

    let toc = TableOfContent::new(&settings.storage);

    for collection in toc.all_collections() {
        info!("loaded collection: {}", collection);
    }

    let toc_data = web::Data::new(toc);

    HttpServer::new(move || {
        let app = App::new()
            .wrap(Logger::default())
            .app_data(toc_data.clone())
            .data(web::JsonConfig::default().limit(33554432).error_handler(json_error_handler)) // 32 Mb
            .service(index)
            .service(get_collections)
            .service(update_collections)
            .service(get_collection)
            .service(update_points)
            .service(get_point)
            .service(get_vectors)
            .service(search_points)
            .service(recommend_points)
            ;

        app
    })
        // .workers(1)
        .bind(format!("{}:{}", settings.service.host, settings.service.port))?
        .run()
        .await
}
