use crate::contract::{Blob, Error};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_protocol::service::HttpRequest;
use forest_sdk::host;

pub async fn fetch(source: &str) -> Result<Blob, Error> {
    if source.starts_with("data:image/") {
        let (mime, bytes) = source
            .split_once(";base64,")
            .ok_or_else(|| Error::Fetch("unsupported data URI".into()))?;
        let bytes = STANDARD
            .decode(bytes)
            .map_err(|e| Error::Fetch(e.to_string()))?;
        return Ok(Blob {
            mime: mime.trim_start_matches("data:").into(),
            bytes,
        });
    }
    if let Some(bytes) = source.strip_prefix("base64://") {
        let bytes = STANDARD
            .decode(bytes)
            .map_err(|e| Error::Fetch(e.to_string()))?;
        return Ok(Blob {
            mime: mime(&bytes).into(),
            bytes,
        });
    }
    let mut url = url::Url::parse(source)
        .map_err(|_| Error::Fetch("image has no retrievable URL or inline data".into()))?;
    for _ in 0..6 {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::Fetch("unsupported media scheme".into()));
        }
        let response = host::http_request(HttpRequest {
            method: "GET".into(),
            url: url.to_string(),
            headers: vec![],
            body: vec![],
        })
        .await??;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            let next = response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("location"))
                .ok_or_else(|| Error::Fetch("redirect missing location".into()))?;
            url = url.join(&next.1).map_err(|e| Error::Fetch(e.to_string()))?;
            continue;
        }
        if response.status != 200 {
            return Err(Error::Fetch(format!("HTTP {}", response.status)));
        }
        let mime = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, mime)| mime.split(';').next().unwrap().trim().to_owned())
            .unwrap_or_else(|| mime(&response.body).into());
        return Ok(Blob {
            mime,
            bytes: response.body,
        });
    }
    Err(Error::Fetch("too many image redirects".into()))
}
fn mime(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG") {
        "image/png"
    } else if bytes.starts_with(b"GIF8") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") {
        "image/webp"
    } else {
        "image/jpeg"
    }
}
