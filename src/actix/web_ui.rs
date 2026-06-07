use std::fs;
use std::path::Path;

use actix_web::dev::HttpServiceFactory;
use actix_web::http::header::HeaderValue;
use actix_web::middleware::{self, DefaultHeaders};
use actix_web::{web, HttpResponse, Responder};

use crate::actix::ui_auth::{
    change_credentials_handler, inject_dashboard_logout_bar, login_handler, login_page,
    logout_handler, session_status_handler, ui_auth_middleware,
};
use crate::common::ui_auth::UiAuthState;
use crate::settings::Settings;

const DEFAULT_STATIC_DIR: &str = "./static";
pub const WEB_UI_PATH: &str = "/dashboard";

pub fn web_ui_folder(settings: &Settings) -> Option<String> {
    let web_ui_enabled = settings.service.enable_static_content.unwrap_or(true);

    if web_ui_enabled {
        let static_folder = settings
            .service
            .static_content_dir
            .clone()
            .unwrap_or_else(|| DEFAULT_STATIC_DIR.to_string());
        let static_folder_path = Path::new(&static_folder);
        if !static_folder_path.exists() || !static_folder_path.is_dir() {
            // enabled BUT folder does not exist
            log::warn!(
                "Static content folder for Web UI '{}' does not exist",
                static_folder_path.display(),
            );
            None
        } else {
            // enabled AND folder exists
            Some(static_folder)
        }
    } else {
        // not enabled
        None
    }
}

async fn dashboard_index(
    auth: web::Data<UiAuthState>,
    static_folder: web::Data<String>,
) -> impl Responder {
    let index_path = Path::new(static_folder.as_str()).join("index.html");
    let Ok(content) = fs::read_to_string(index_path) else {
        return HttpResponse::NotFound().finish();
    };

    let body = if auth.enabled {
        inject_dashboard_logout_bar(&content)
    } else {
        content
    };

    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(body)
}

pub fn web_ui_factory(
    static_folder: &str,
    ui_auth_state: web::Data<UiAuthState>,
    secure_ui_cookie: web::Data<bool>,
) -> impl HttpServiceFactory + use<> {
    let static_folder_data = web::Data::new(static_folder.to_string());

    web::scope(WEB_UI_PATH)
        .wrap(DefaultHeaders::new().add(("X-Frame-Options", HeaderValue::from_static("DENY"))))
        .app_data(ui_auth_state.clone())
        .app_data(secure_ui_cookie)
        .app_data(static_folder_data)
        .wrap(middleware::from_fn(ui_auth_middleware))
        .route("/login", web::get().to(login_page))
        .route("/auth/login", web::post().to(login_handler))
        .route("/auth/logout", web::post().to(logout_handler))
        .route("/auth/session", web::get().to(session_status_handler))
        .route(
            "/auth/credentials",
            web::put().to(change_credentials_handler),
        )
        .route("", web::get().to(dashboard_index))
        .route("/", web::get().to(dashboard_index))
        .route("/index.html", web::get().to(dashboard_index))
        .service(actix_files::Files::new("/", static_folder).index_file("index.html"))
}

#[cfg(test)]
mod tests {
    use actix_web::App;
    use actix_web::http::StatusCode;
    use actix_web::http::header::{self, HeaderMap};
    use actix_web::test::{self, TestRequest};

    use super::*;

    fn assert_html_custom_headers(headers: &HeaderMap) {
        let content_type = header::HeaderValue::from_static("text/html; charset=utf-8");
        assert_eq!(headers.get(header::CONTENT_TYPE), Some(&content_type));
        let x_frame_options = header::HeaderValue::from_static("DENY");
        assert_eq!(headers.get(header::X_FRAME_OPTIONS), Some(&x_frame_options),);
    }

    #[actix_web::test]
    async fn test_web_ui() {
        let static_dir = String::from("static");
        let mut settings = Settings::new(None).unwrap();
        settings.service.static_content_dir = Some(static_dir.clone());

        let maybe_static_folder = web_ui_folder(&settings);
        if maybe_static_folder.is_none() {
            println!("Skipping test because the static folder was not found.");
            return;
        }

        let static_folder = maybe_static_folder.unwrap();
        let secure_cookie = web::Data::new(false);
        let auth = web::Data::new(UiAuthState::disabled());
        let srv = test::init_service(
            App::new().service(web_ui_factory(&static_folder, auth, secure_cookie)),
        )
        .await;

        // Index path (no trailing slash)
        let req = TestRequest::with_uri(WEB_UI_PATH).to_request();
        let res = test::call_service(&srv, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        let headers = res.headers();
        assert_html_custom_headers(headers);
        // Index path (trailing slash)
        let req = TestRequest::with_uri(format!("{WEB_UI_PATH}/").as_str()).to_request();
        let res = test::call_service(&srv, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        let headers = res.headers();
        assert_html_custom_headers(headers);
        // Index path (index.html file)
        let req = TestRequest::with_uri(format!("{WEB_UI_PATH}/index.html").as_str()).to_request();
        let res = test::call_service(&srv, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        let headers = res.headers();
        assert_html_custom_headers(headers);
        // Static asset (favicon.ico)
        let req = TestRequest::with_uri(format!("{WEB_UI_PATH}/favicon.ico").as_str()).to_request();
        let res = test::call_service(&srv, req).await;
        assert_eq!(res.status(), StatusCode::OK);
        let headers = res.headers();
        assert_eq!(
            headers.get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("image/x-icon")),
        );
        // Non-existing path (404 Not Found)
        let fake_path = uuid::Uuid::new_v4().to_string();
        let secure_cookie = web::Data::new(false);
        let auth = web::Data::new(UiAuthState::disabled());
        let srv = test::init_service(
            App::new().service(web_ui_factory(&fake_path, auth, secure_cookie)),
        )
        .await;

        let req = TestRequest::with_uri(WEB_UI_PATH).to_request();
        let res = test::call_service(&srv, req).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let headers = res.headers();
        assert_eq!(headers.get(header::CONTENT_TYPE), None);
        assert_eq!(headers.get(header::CONTENT_LENGTH), None);
    }
}
