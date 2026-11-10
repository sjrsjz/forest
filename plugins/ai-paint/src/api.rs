use crate::{Request, Style, config, contract::Error, media::Blob, result};
use forest_protocol::service::HttpRequest;
use forest_sdk::host;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub enum Client {
    OpenAiImages(config::OpenAiImages),
    SiliconFlow(config::SiliconFlow),
    SwarmUi(config::SwarmUi),
}

#[derive(Clone, Debug, forest_serde::DeriveSchema, forest_serde::DeriveValue)]
pub enum ConfigurationError {
    InvalidEndpoint,
    InvalidSteps {
        backend: config::Backend,
        steps: i64,
    },
    InvalidDimensions {
        size: String,
    },
}

fn endpoint(value: &str) -> Result<url::Url, ConfigurationError> {
    let url = url::Url::parse(value).map_err(|_| ConfigurationError::InvalidEndpoint)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigurationError::InvalidEndpoint);
    }
    Ok(url)
}

impl Client {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        let address = match self {
            Self::OpenAiImages(c) => &c.endpoint,
            Self::SiliconFlow(c) => &c.endpoint,
            Self::SwarmUi(c) => &c.endpoint,
        };
        endpoint(address)?;
        match self {
            Self::SiliconFlow(c) if !(1..=100).contains(&c.steps) => {
                return Err(ConfigurationError::InvalidSteps {
                    backend: config::Backend::SiliconFlow,
                    steps: c.steps.into(),
                });
            }
            Self::SwarmUi(c) => {
                if !(1..=100).contains(&c.steps) {
                    return Err(ConfigurationError::InvalidSteps {
                        backend: config::Backend::SwarmUi,
                        steps: c.steps.into(),
                    });
                }
                for size in [&c.sizes.portrait, &c.sizes.landscape, &c.sizes.square] {
                    dimensions(size)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn generate(&self, args: Request) -> Result<Blob, Error> {
        match self {
            Self::OpenAiImages(c) => {
                if c.api_key.is_empty() {
                    return Err(Error::Execution(
                        "Image generation is not configured".into(),
                    ));
                }
                if args.seed.is_some() {
                    return Err(Error::Arguments(
                        "The configured OpenAI images backend does not accept seed".into(),
                    ));
                }
                #[derive(Deserialize)]
                struct Response {
                    data: Vec<Image>,
                }
                #[derive(Deserialize)]
                #[serde(untagged)]
                enum Image {
                    Url { url: String },
                    Base64 { b64_json: String },
                }
                let payload = json!({
                    "model":c.model,"prompt":args.prompt(),"n":1,
                    "size":args.size.value(&c.sizes),"quality":c.quality,
                });
                let response: Response = post(&c.endpoint, bearer(&c.api_key), payload).await?;
                match response.data.into_iter().next().ok_or_else(empty)? {
                    Image::Url { url } => result::download(&url).await,
                    Image::Base64 { b64_json } => result::base64(&b64_json),
                }
            }
            Self::SiliconFlow(c) => {
                if c.api_key.is_empty() {
                    return Err(Error::Execution(
                        "Image generation is not configured".into(),
                    ));
                }
                #[derive(Deserialize)]
                struct Response {
                    images: Vec<Image>,
                }
                #[derive(Deserialize)]
                struct Image {
                    url: String,
                }
                let mut payload = json!({"model":c.model,"prompt":args.prompt(),"image_size":args.size.value(&c.sizes),"num_inference_steps":c.steps});
                if let Some(seed) = args.seed {
                    payload["seed"] = json!(seed);
                }
                let response: Response = post(&c.endpoint, bearer(&c.api_key), payload).await?;
                result::download(&response.images.into_iter().next().ok_or_else(empty)?.url).await
            }
            Self::SwarmUi(c) => {
                let headers = if c.auth_token.is_empty() {
                    vec![]
                } else {
                    vec![("Cookie".into(), format!("swarm_token={}", c.auth_token))]
                };
                #[derive(Deserialize)]
                struct Session {
                    session_id: String,
                }
                let session: Session = post(
                    &format!("{}/API/GetNewSession", c.endpoint.trim_end_matches('/')),
                    headers.clone(),
                    json!({}),
                )
                .await?;
                let (model, negative) = match args.style {
                    Style::Anime => (&c.anime_model, c.anime_negative_prompt.as_str()),
                    Style::Photo => (&c.photo_model, c.photo_negative_prompt.as_str()),
                    Style::None => (&c.default_model, ""),
                };
                let (width, height) = dimensions(args.size.value(&c.sizes))
                    .map_err(|error| Error::Execution(format!("{error:?}")))?;
                #[derive(Deserialize)]
                struct Response {
                    images: Vec<String>,
                }
                let payload = json!({
                    "session_id":session.session_id,"images":1,"donotsave":true,"prompt":args.prompt(),
                    "negativeprompt":negative,"model":model,"width":width,"height":height,
                    "cfgscale":7.5,"steps":c.steps,"seed":args.seed.unwrap_or(-1),
                });
                let address = format!(
                    "{}/API/GenerateText2Image",
                    c.endpoint.trim_end_matches('/')
                );
                let response: Response = post(&address, headers, payload).await?;
                let image = response.images.into_iter().next().ok_or_else(empty)?;
                if image.starts_with("data:image/") {
                    result::data_uri(&image)
                } else {
                    let base = endpoint(&format!("{}/", c.endpoint.trim_end_matches('/')))
                        .map_err(|error| Error::Execution(format!("{error:?}")))?;
                    let url = base
                        .join(&image)
                        .map_err(|e| Error::Execution(e.to_string()))?;
                    result::download(url.as_str()).await
                }
            }
        }
    }
}

fn dimensions(size: &str) -> Result<(u32, u32), ConfigurationError> {
    let invalid = || ConfigurationError::InvalidDimensions { size: size.into() };
    let (width, height) = size.split_once('x').ok_or_else(invalid)?;
    let width = width.parse().map_err(|_| invalid())?;
    let height = height.parse().map_err(|_| invalid())?;
    if width == 0 || height == 0 {
        return Err(invalid());
    }
    Ok((width, height))
}
fn empty() -> Error {
    Error::Execution("Image provider returned no image".into())
}
fn bearer(key: &str) -> Vec<(String, String)> {
    vec![("Authorization".into(), format!("Bearer {key}"))]
}

async fn post<T: DeserializeOwned>(
    url: &str,
    mut headers: Vec<(String, String)>,
    payload: Value,
) -> Result<T, Error> {
    headers.push(("Content-Type".into(), "application/json".into()));
    let response = host::http_request(HttpRequest {
        method: "POST".into(),
        url: url.into(),
        headers,
        body: serde_json::to_vec(&payload).expect("serializable generation request"),
    })
    .await
    .map_err(|e| Error::Execution(e.to_string()))?
    .map_err(|e| Error::Execution(e.to_string()))?;
    if !(200..300).contains(&response.status) {
        return Err(Error::Execution(format!(
            "Image generation HTTP {}",
            response.status
        )));
    }
    serde_json::from_slice(&response.body)
        .map_err(|e| Error::Execution(format!("Image provider response: {e}")))
}
