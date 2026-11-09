mod cache;
pub mod contract;
pub mod media;
mod project;

use contract::{Error, OBJECT, PREPARE, THUMBNAIL, Thumbnail};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use std::rc::Rc;

#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub max_side: i32,
    pub decode_memory_mib: i32,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            max_side: 4096,
            decode_memory_mib: 64,
        }
    }
}

pub struct Projector {
    settings: Settings,
}

impl Module for Projector {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings: Settings = host::config_get(context.object, "/settings".parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if settings.max_side <= 0 || settings.decode_memory_mib <= 0 {
            return Err("image dimensions and decode memory must be positive".into());
        }
        cache::initialize().await.map_err(|e| format!("{e:?}"))?;
        Ok(Self { settings })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new()
            .with(
                PREPARE.parse().unwrap(),
                false,
                callers(vec![
                    SubjectPattern::Path("/module/memory".parse().unwrap()),
                    SubjectPattern::Path("/user/admin".parse().unwrap()),
                ]),
                Self::prepare,
            )
            .with(
                THUMBNAIL.parse().unwrap(),
                false,
                callers(vec![
                    SubjectPattern::Path("/module/memory".parse().unwrap()),
                    SubjectPattern::Path("/user/admin".parse().unwrap()),
                ]),
                Self::thumbnail,
            )
    }
}

impl Projector {
    async fn thumbnail(
        self: Rc<Self>,
        _: CallContext,
        input: Thumbnail,
    ) -> Result<media::Blob, Error> {
        if input.max_side <= 0 {
            return Err(Error::Image("thumbnail max_side must be positive".into()));
        }
        let Thumbnail {
            reference,
            max_side,
        } = input;
        let cache_key = format!("thumbnail:{reference}");
        if let Some(blob) = cache::get(&cache_key, max_side).await? {
            return Ok(blob);
        }
        let source: Result<media::Blob, media::Error> = host::call(
            media::OBJECT.parse().unwrap(),
            "/get".parse().unwrap(),
            &reference,
        )
        .await
        .map_err(|e| Error::Host(e.to_string()))?;
        let settings = Settings {
            max_side,
            ..self.settings.clone()
        };
        let blob = project::thumbnail(&source.map_err(Error::Media)?, &settings)?;
        cache::put(cache_key, max_side, &blob).await?;
        Ok(blob)
    }
    async fn prepare(
        self: Rc<Self>,
        _: CallContext,
        reference: String,
    ) -> Result<media::Blob, Error> {
        if let Some(blob) = cache::get(&reference, self.settings.max_side).await? {
            return Ok(blob);
        }
        let source: Result<media::Blob, media::Error> = host::call(
            media::OBJECT.parse().unwrap(),
            "/get".parse().unwrap(),
            &reference,
        )
        .await
        .map_err(|e| Error::Host(e.to_string()))?;
        let mut source = source.map_err(Error::Media)?;
        match project::project(&source, &self.settings)? {
            project::Projection::Original { mime } => {
                source.mime = mime;
                Ok(source)
            }
            project::Projection::Scaled(blob) => {
                cache::put(reference, self.settings.max_side, &blob).await?;
                Ok(blob)
            }
        }
    }
}

forest_sdk::export_module!(super::Projector);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Image projection".into()),
            description: Some(
                "Configurable model-input image dimensions; persistent derived images".into(),
            ),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: "/settings".parse().unwrap(),
            schema: Settings::as_schema(),
            default: Settings::default().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Projector::handlers().declarations(),
        uses: vec![InterfaceUse {
            object: media::OBJECT.parse().unwrap(),
            interface: "/get".parse().unwrap(),
            input: String::as_schema(),
            output: Result::<media::Blob, media::Error>::as_schema(),
        }],
        subscriptions: vec![],
    }
}
