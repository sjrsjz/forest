//! Native asynchronous HTTP(S), enabled explicitly with the `http` feature.

use reqwest::{
    Client, StatusCode, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
};

use crate::Transport;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("endpoint must be an HTTP(S) base URL without userinfo, query, or fragment")]
    InvalidEndpoint,
    #[error("invalid Bearer token header: {0}")]
    InvalidAuthorization(#[from] reqwest::header::InvalidHeaderValue),
    #[error("HTTP client creation failed: {0}")]
    Client(#[from] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("HTTP exchange failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("HTTP status {status}")]
    Status { status: StatusCode, body: Vec<u8> },
}

/// A reusable connection pool, with no hidden task or serialized request queue.
/// Dropping it does not establish whether a previously sent action was executed.
#[derive(Clone)]
pub struct HttpTransport {
    endpoint: Url,
    authorization: Option<HeaderValue>,
    client: Client,
}

impl HttpTransport {
    pub fn new(endpoint: Url, access_token: Option<&str>) -> Result<Self, BuildError> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Self::with_client(endpoint, access_token, client)
    }

    /// Uses an explicitly configured pool (proxy, TLS roots, etc.). Its redirect,
    /// retry and deadline policies are the caller's responsibility.
    pub fn with_client(
        endpoint: Url,
        access_token: Option<&str>,
        client: Client,
    ) -> Result<Self, BuildError> {
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(BuildError::InvalidEndpoint);
        }
        let authorization = access_token
            .map(|token| {
                let mut value = HeaderValue::from_str(&format!("Bearer {token}"))?;
                value.set_sensitive(true);
                Ok::<_, reqwest::header::InvalidHeaderValue>(value)
            })
            .transpose()?;
        Ok(Self {
            endpoint,
            authorization,
            client,
        })
    }
}

impl Transport for HttpTransport {
    type Error = HttpError;

    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let mut url = self.endpoint.clone();
        // Constructor guarantees a hierarchical HTTP URL. Appending a segment
        // preserves a configured path prefix and never interprets action as a URL.
        url.path_segments_mut()
            .expect("HTTP base URL")
            .pop_if_empty()
            .push(action);
        let mut request = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(params);
        if let Some(authorization) = &self.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        let response = request.send().await?;
        let status = response.status();
        let body = response.bytes().await?.to_vec();
        if status != StatusCode::OK {
            return Err(HttpError::Status { status, body });
        }
        Ok(body)
    }
}
