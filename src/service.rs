mod settings;

mod common;
mod operations;
mod api_models;

use actix_web::middleware::Logger;

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};

use env_logger;
use storage::content_manager::toc::TableOfContent;


#[get("/collections")]
async fn collections() -> impl Responder {
    HttpResponse::Ok()
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let settings = settings::Settings::new().expect("Can't read config.");
    std::env::set_var("RUST_LOG",  settings.log_level);
    env_logger::init();

    let toc = TableOfContent::new();

    
    HttpServer::new(|| {
        App::new()
            .wrap(Logger::default())
            .data(web::JsonConfig::default().limit(33554432)) // 32 Mb
            .service(collections)
    })
    // .workers(1)
    .bind(format!("{}:{}", settings.service.host, settings.service.port))?
    .run()
    .await
}
