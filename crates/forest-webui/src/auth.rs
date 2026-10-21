use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq;

const COOKIE: &str = "forest_session";
pub(crate) struct Auth {
    password: [u8; 32],
    session: [u8; 32],
    cookie: String,
}
impl Auth {
    pub fn new(password: &str) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(!password.is_empty(), "WebUI password must not be empty");
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(|e| anyhow::anyhow!("session randomness: {e}"))?;
        let token = random
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        Ok(Arc::new(Self {
            password: digest(password),
            session: digest(&token),
            cookie: format!("{COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/api"),
        }))
    }
    pub fn accepts(&self, headers: &HeaderMap) -> bool {
        headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .filter_map(|v| v.trim().split_once('='))
            .any(|(name, value)| name == COOKIE && bool::from(digest(value).ct_eq(&self.session)))
    }
}
fn digest(input: &str) -> [u8; 32] {
    Sha256::digest(input.as_bytes()).into()
}

pub(crate) async fn protect(
    State(auth): State<Arc<Auth>>,
    request: Request,
    next: Next,
) -> Response {
    if !auth.accepts(request.headers()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"message":"请先登录宿主调试台"})),
        )
            .into_response();
    }
    next.run(request).await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Login {
    password: String,
}

// Session mutation is same-origin; no caller-supplied identity is accepted.
fn same_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else {
        return false;
    };
    let Ok(origin) = origin.to_str().unwrap_or("").parse::<axum::http::Uri>() else {
        return false;
    };
    matches!(origin.scheme_str(), Some("http" | "https"))
        && origin.authority().is_some_and(|a| a.as_str() == host)
}
pub(crate) async fn login(
    State(auth): State<Arc<Auth>>,
    headers: HeaderMap,
    Json(input): Json<Login>,
) -> Response {
    if !same_origin(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !bool::from(digest(&input.password).ct_eq(&auth.password)) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"message":"管理口令不正确"})),
        )
            .into_response();
    }
    (
        [(
            header::SET_COOKIE,
            HeaderValue::from_str(&auth.cookie).expect("hex session cookie"),
        )],
        StatusCode::NO_CONTENT,
    )
        .into_response()
}
pub(crate) async fn logout(headers: HeaderMap) -> Response {
    if !same_origin(&headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    (
        [(
            header::SET_COOKIE,
            format!("{COOKIE}=; HttpOnly; SameSite=Strict; Path=/api; Max-Age=0"),
        )],
        StatusCode::NO_CONTENT,
    )
        .into_response()
}
pub(crate) async fn session(State(auth): State<Arc<Auth>>, headers: HeaderMap) -> StatusCode {
    if auth.accepts(&headers) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::UNAUTHORIZED
    }
}
