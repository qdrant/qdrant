#[macro_use] extern crate log;

mod settings;

mod common;
mod api_models;

use actix_web::middleware::Logger;

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

use env_logger;
use storage::content_manager::toc::TableOfContent;
use crate::api_models::models::{CollectionDescription, ApiResponse, CollectionsResponse, Status};
use itertools::Itertools;
use actix_web::rt::time::Instant;


#[get("/collections")]
async fn collections(
    toc: web::Data<TableOfContent>
) -> impl Responder {
    let now = Instant::now();

    let collections = toc
        .all_collections()
        .into_iter()
        .map(|name| CollectionDescription { name })
        .collect_vec();

    HttpResponse::Ok().json(ApiResponse {
        result: Some(CollectionsResponse { collections }),
        status: Status::Ok,
        time: now.elapsed().as_secs_f64()
    })
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG",  settings.log_level);
    env_logger::init();

    let toc = TableOfContent::new(&settings.storage);

    for collection in toc.all_collections() {
        info!("loaded collection: {}", collection);
    }

    let toc_data = web::Data::new(toc);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(toc_data.clone())
            .data(web::JsonConfig::default().limit(33554432)) // 32 Mb
            .service(collections)
    })
    // .workers(1)
    .bind(format!("{}:{}", settings.service.host, settings.service.port))?
    .run()
    .await
}
