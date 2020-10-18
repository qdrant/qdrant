#[macro_use]
extern crate log;

mod settings;

mod common;
mod collections_api;

use actix_web::middleware::Logger;

use actix_web::{web, App, HttpServer, error, HttpRequest, HttpResponse};

use env_logger;
use storage::content_manager::toc::TableOfContent;
use crate::collections_api::api::{get_collections, get_collection, update_collections};


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
            .service(get_collections)
            .service(update_collections)
            .service(get_collection);

        app
    })
        // .workers(1)
        .bind(format!("{}:{}", settings.service.host, settings.service.port))?
        .run()
        .await
}
