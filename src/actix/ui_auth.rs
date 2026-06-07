use actix_web::body::BoxBody;
use actix_web::cookie::{Cookie, SameSite};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::header;
use actix_web::middleware::Next;
use actix_web::web;
use actix_web::{Error, HttpResponse, Responder};
use serde::Deserialize;

use crate::common::ui_auth::{
    SESSION_COOKIE, SESSION_DURATION_SECS, UiAuthError, UiAuthState,
};

const LOGIN_PAGE: &str = include_str!("ui_login.html");

const PUBLIC_PATHS: [&str; 4] = [
    "/dashboard/login",
    "/dashboard/auth/login",
    "/dashboard/auth/logout",
    "/dashboard/auth/session",
];

const LOGOUT_BAR_INJECTION: &str = r#"
  <div id="qdrant-ui-auth-bar" style="position:fixed;top:12px;right:12px;z-index:100000;display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:10px;border:1px solid rgba(255,255,255,0.12);background:rgba(15,20,25,0.92);color:#e7ecf3;font:600 0.85rem Inter,system-ui,sans-serif;backdrop-filter:blur(8px);box-shadow:0 8px 24px rgba(0,0,0,0.35);">
    <span id="qdrant-ui-auth-user"></span>
    <button id="qdrant-ui-auth-logout" type="button" style="border:0;border-radius:8px;padding:8px 12px;background:#dc244c;color:#fff;font:inherit;cursor:pointer;">Log out</button>
  </div>
  <script>
  (function () {
    fetch('/dashboard/auth/session')
      .then(function (response) { return response.json(); })
      .then(function (data) {
        if (!data.authenticated) {
          var bar = document.getElementById('qdrant-ui-auth-bar');
          if (bar) bar.remove();
          return;
        }
        document.getElementById('qdrant-ui-auth-user').textContent = data.username;
        document.getElementById('qdrant-ui-auth-logout').addEventListener('click', function () {
          fetch('/dashboard/auth/logout', { method: 'POST' })
            .then(function () { window.location.href = '/dashboard/login'; });
        });
      });
  })();
  </script>
"#;

pub fn inject_dashboard_logout_bar(html: &str) -> String {
    if html.contains("</body>") {
        html.replacen("</body>", &format!("{LOGOUT_BAR_INJECTION}</body>"), 1)
    } else {
        format!("{html}{LOGOUT_BAR_INJECTION}")
    }
}

fn is_public_path(req: &ServiceRequest) -> bool {
    let pattern = req.match_pattern().unwrap_or_else(|| req.path().to_string());
    PUBLIC_PATHS.iter().any(|path| pattern == *path)
}

fn has_valid_session(auth: &UiAuthState, req: &ServiceRequest) -> bool {
    req.cookie(SESSION_COOKIE)
        .map(|cookie| auth.validate_session_token(cookie.value()))
        .unwrap_or(false)
}

pub async fn ui_auth_middleware(
    req: ServiceRequest,
    next: Next<BoxBody>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let auth = req
        .app_data::<web::Data<UiAuthState>>()
        .cloned()
        .expect("UiAuthState must be registered for dashboard scope");

    if !auth.enabled || is_public_path(&req) || has_valid_session(&auth, &req) {
        return next.call(req).await;
    }

    let method = req.method().clone();
    let redirect_target = req.uri().to_string();
    let response = if method == actix_web::http::Method::GET {
        let location = format!(
            "/dashboard/login?redirect={}",
            urlencoding::encode(&redirect_target)
        );
        HttpResponse::Found()
            .append_header((header::LOCATION, location))
            .finish()
    } else {
        HttpResponse::Unauthorized().body("Web UI authentication required")
    };

    let (request, _) = req.into_parts();
    Ok(ServiceResponse::new(request, response.map_into_boxed_body()))
}

fn session_cookie(token: &str, secure: bool, max_age_secs: i64) -> Cookie<'static> {
    Cookie::build(SESSION_COOKIE, token.to_string())
        .path("/dashboard")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(secure)
        .max_age(actix_web::cookie::time::Duration::seconds(max_age_secs))
        .finish()
}

pub async fn login_page(auth: web::Data<UiAuthState>) -> impl Responder {
    if !auth.enabled {
        return HttpResponse::NotFound().body("Web UI authentication is disabled");
    }
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(LOGIN_PAGE)
}

#[derive(Deserialize)]
pub struct LoginRequest {
    username: String,
    password: String,
}

pub async fn login_handler(
    auth: web::Data<UiAuthState>,
    secure_cookie: web::Data<bool>,
    body: web::Json<LoginRequest>,
) -> impl Responder {
    if !auth.enabled {
        return HttpResponse::NotFound().json(serde_json::json!({
            "status": "error",
            "message": "Web UI authentication is disabled",
        }));
    }

    if !auth.verify_credentials(&body.username, &body.password) {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "status": "error",
            "message": "Invalid username or password",
        }));
    }

    let token = auth.create_session_token();
    let cookie = session_cookie(&token, **secure_cookie, SESSION_DURATION_SECS as i64);

    HttpResponse::Ok()
        .cookie(cookie)
        .json(serde_json::json!({
            "status": "ok",
            "username": auth.username(),
        }))
}

pub async fn logout_handler(secure_cookie: web::Data<bool>) -> impl Responder {
    let cookie = session_cookie("", **secure_cookie, 0);

    HttpResponse::Ok()
        .cookie(cookie)
        .json(serde_json::json!({ "status": "ok" }))
}

#[derive(Deserialize)]
pub struct ChangeCredentialsRequest {
    current_password: String,
    new_username: Option<String>,
    new_password: String,
}

pub async fn change_credentials_handler(
    auth: web::Data<UiAuthState>,
    secure_cookie: web::Data<bool>,
    req: actix_web::HttpRequest,
    body: web::Json<ChangeCredentialsRequest>,
) -> impl Responder {
    if !auth.enabled {
        return HttpResponse::NotFound().json(serde_json::json!({
            "status": "error",
            "message": "Web UI authentication is disabled",
        }));
    }

    let Some(cookie) = req.cookie(SESSION_COOKIE) else {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "status": "error",
            "message": "Not authenticated",
        }));
    };

    if !auth.validate_session_token(cookie.value()) {
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "status": "error",
            "message": "Not authenticated",
        }));
    }

    let new_username = body
        .new_username
        .clone()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| auth.username());

    match auth.update_credentials(
        &body.current_password,
        &new_username,
        &body.new_password,
    ) {
        Ok(()) => HttpResponse::Ok()
            .cookie(session_cookie("", **secure_cookie, 0))
            .json(serde_json::json!({
                "status": "ok",
                "username": new_username,
                "message": "Credentials updated. Please sign in again.",
            })),
        Err(UiAuthError::InvalidCredentials) => HttpResponse::Unauthorized().json(serde_json::json!({
            "status": "error",
            "message": "Invalid current password",
        })),
        Err(err) => HttpResponse::InternalServerError().json(serde_json::json!({
            "status": "error",
            "message": err.to_string(),
        })),
    }
}

pub async fn session_status_handler(
    auth: web::Data<UiAuthState>,
    req: actix_web::HttpRequest,
) -> impl Responder {
    if !auth.enabled {
        return HttpResponse::Ok().json(serde_json::json!({
            "authenticated": false,
            "enabled": false,
        }));
    }

    let authenticated = req
        .cookie(SESSION_COOKIE)
        .map(|cookie| auth.validate_session_token(cookie.value()))
        .unwrap_or(false);

    HttpResponse::Ok().json(serde_json::json!({
        "authenticated": authenticated,
        "enabled": true,
        "username": if authenticated { Some(auth.username()) } else { None },
    }))
}
