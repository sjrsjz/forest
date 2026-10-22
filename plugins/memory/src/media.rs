use crate::{Memory, openai::ImageUrl, store::*, types::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_protocol::service::{HttpRequest, LogLevel};
use forest_sdk::host;
use std::{collections::BTreeSet, rc::Rc};

struct Lease<'a> {
    owner: &'a Memory,
    reference: String,
    lock: Rc<futures::lock::Mutex<()>>,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if Rc::strong_count(&self.lock) == 2 {
            self.owner.media_locks.borrow_mut().remove(&self.reference);
        }
    }
}

impl Memory {
    pub async fn image(&self, reference: &str) -> Result<String, Error> {
        let lock = self
            .media_locks
            .borrow_mut()
            .entry(reference.to_owned())
            .or_insert_with(|| Rc::new(futures::lock::Mutex::new(())))
            .clone();
        let lease = Lease {
            owner: self,
            reference: reference.into(),
            lock,
        };
        let _guard = lease.lock.lock().await;
        let data = execute(vec![stmt(
            "SELECT source,state,error,mime FROM media WHERE reference=?",
            vec![text(reference)],
        )])
        .await?;
        let row = data[0].rows.first().ok_or(Error::NotFound)?;
        let path = media_path(reference)?;
        match string(&row[1])? {
            "ready" => Ok(data_url(string(&row[3])?, &host::file_read(path).await??)),
            "failed" => Err(Error::Media(string(&row[2])?.into())),
            _ => {
                let result = fetch(string(&row[0])?).await;
                match result {
                    Ok((mime, data)) => {
                        let url = data_url(&mime, &data);
                        host::file_write(path, data).await??;
                        execute(vec![stmt(
                            "UPDATE media SET state='ready',error=NULL,mime=? WHERE reference=?",
                            vec![text(mime), text(reference)],
                        )])
                        .await?;
                        Ok(url)
                    }
                    Err(error) => {
                        execute(vec![stmt(
                            "UPDATE media SET state='failed',error=? WHERE reference=?",
                            vec![text(format!("{error:?}")), text(reference)],
                        )])
                        .await?;
                        Err(error)
                    }
                }
            }
        }
    }
    pub async fn project(
        self: Rc<Self>,
        _: forest_protocol::CallContext,
        input: Project,
    ) -> Result<Projection, Error> {
        let units = read(&input.ids, false).await?;
        if units.len() != input.ids.len() {
            return Err(Error::NotFound);
        }
        let mut images = Vec::new();
        if input.images {
            let mut refs = BTreeSet::new();
            for unit in &units {
                refs.extend(crate::projection::image_references(unit)?);
            }
            for reference in refs {
                match self.image(&reference).await {
                    Ok(url) => images.push(ImageInput {
                        reference,
                        image: ImageUrl { url, detail: None },
                    }),
                    Err(error) => {
                        let _ = host::log(
                            LogLevel::Warn,
                            format!("image unavailable: {reference}: {error:?}"),
                        )
                        .await;
                    }
                }
            }
        }
        Ok(Projection {
            units: units
                .iter()
                .map(crate::projection::projected)
                .collect::<Result<_, _>>()?,
            images,
        })
    }
}
fn media_path(reference: &str) -> Result<String, Error> {
    let hash = reference
        .strip_prefix("media:")
        .ok_or_else(|| Error::Media("expected managed reference".into()))?;
    if hash.len() != 64 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Media("invalid managed reference".into()));
    }
    Ok(format!("media/{hash}.bin"))
}
fn data_url(mime: &str, data: &[u8]) -> String {
    format!("data:{mime};base64,{}", STANDARD.encode(data))
}
async fn fetch(source: &str) -> Result<(String, Vec<u8>), Error> {
    if source.starts_with("data:image/") {
        let (mime, bytes) = source
            .split_once(";base64,")
            .ok_or_else(|| Error::Media("unsupported data URI".into()))?;
        let bytes = STANDARD
            .decode(bytes)
            .map_err(|e| Error::Media(e.to_string()))?;
        return Ok((mime.trim_start_matches("data:").into(), bytes));
    }
    if let Some(bytes) = source.strip_prefix("base64://") {
        let bytes = STANDARD
            .decode(bytes)
            .map_err(|e| Error::Media(e.to_string()))?;
        return Ok((mime(&bytes).into(), bytes));
    }
    let mut url = url::Url::parse(source)
        .map_err(|_| Error::Media("image has no retrievable URL or inline data".into()))?;
    for _ in 0..6 {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::Media("unsupported media scheme".into()));
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
                .ok_or_else(|| Error::Media("redirect missing location".into()))?;
            url = url.join(&next.1).map_err(|e| Error::Media(e.to_string()))?;
            continue;
        }
        if response.status != 200 {
            return Err(Error::Media(format!("HTTP {}", response.status)));
        }
        let mime = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
            .map(|(_, mime)| mime.split(';').next().unwrap().trim().to_owned())
            .unwrap_or_else(|| mime(&response.body).into());
        return Ok((mime, response.body));
    }
    Err(Error::Media("too many image redirects".into()))
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
