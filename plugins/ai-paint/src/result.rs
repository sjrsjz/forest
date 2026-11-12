use crate::{contract::Error, media::Blob};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_protocol::service::HttpRequest;
use forest_sdk::host;

fn image(bytes: Vec<u8>) -> Result<Blob, Error> {
    let mime = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else {
        return Err(Error::Execution(
            "Image provider returned unsupported or invalid image data".into(),
        ));
    };
    Ok(Blob {
        mime: mime.into(),
        bytes,
    })
}
pub fn base64(value: &str) -> Result<Blob, Error> {
    image(
        STANDARD
            .decode(value)
            .map_err(|e| Error::Execution(format!("Image base64: {e}")))?,
    )
}
pub fn data_uri(value: &str) -> Result<Blob, Error> {
    let (_, bytes) = value
        .split_once(";base64,")
        .ok_or_else(|| Error::Execution("Image data URI must use base64".into()))?;
    base64(bytes)
}
pub async fn download(address: &str) -> Result<Blob, Error> {
    let mut url = url::Url::parse(address).map_err(|e| Error::Execution(e.to_string()))?;
    for _ in 0..6 {
        if !matches!(url.scheme(), "https" | "http")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(Error::Execution(
                "Generated image URL must use HTTP(S) without embedded credentials".into(),
            ));
        }
        // Generated URLs may point to a CDN; never forward the API credential.
        let response = host::http_request(HttpRequest {
            timeout_ms: None,
            method: "GET".into(),
            url: url.to_string(),
            headers: vec![],
            body: vec![],
        })
        .await
        .map_err(|e| Error::Execution(e.to_string()))?
        .map_err(|e| Error::Execution(e.to_string()))?;
        if matches!(response.status, 301 | 302 | 303 | 307 | 308) {
            let location = response
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("location"))
                .ok_or_else(|| {
                    Error::Execution("Generated image redirect missing location".into())
                })?;
            url = url
                .join(&location.1)
                .map_err(|e| Error::Execution(e.to_string()))?;
            continue;
        }
        if response.status != 200 {
            return Err(Error::Execution(format!(
                "Generated image download HTTP {}",
                response.status
            )));
        }
        return image(response.body);
    }
    Err(Error::Execution(
        "Too many generated image redirects".into(),
    ))
}
