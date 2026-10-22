//! Typst rendering in an independent Wasm module.
pub mod contract;
pub mod media;
mod world;

use contract::{Error, Invocation};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, rc::Rc};

pub const OBJECT: &str = "/module/typst";

#[derive(Clone, DeriveSchema, DeriveValue)]
pub struct Settings {
    pub page_width_pt: i32,
    pub font_size_pt: i32,
    pub pixels_per_point: i32,
    pub max_pages: i32,
    pub max_page_pixels: i64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            page_width_pt: 480,
            font_size_pt: 12,
            pixels_per_point: 2,
            max_pages: 8,
            max_page_pixels: 8_000_000,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    source: String,
    /// Explicit virtual file names mapped to managed image references.
    #[serde(default)]
    images: BTreeMap<String, String>,
}
#[derive(Serialize)]
struct Page {
    page: usize,
    reference: String,
    width: u32,
    height: u32,
    cq: String,
}
#[derive(Serialize)]
struct Answer {
    pages: Vec<Page>,
    warnings: Vec<world::Diagnostic>,
}

pub struct Renderer {
    fonts: world::Fonts,
    settings: Settings,
}
impl Module for Renderer {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings: Settings = host::config_get(context.object, "/settings".parse().unwrap())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if !(120..=1440).contains(&settings.page_width_pt)
            || !(6..=48).contains(&settings.font_size_pt)
            || !(1..=3).contains(&settings.pixels_per_point)
            || !(1..=32).contains(&settings.max_pages)
            || !(1..=16_000_000).contains(&settings.max_page_pixels)
        {
            return Err("invalid Typst output settings".into());
        }
        Ok(Self {
            fonts: world::Fonts::new(),
            settings,
        })
    }
    fn handlers() -> Handlers<Self> {
        Handlers::new().with(
            "/render".parse().unwrap(),
            false,
            callers(vec![
                SubjectPattern::Path("/module/agent-tools".parse().unwrap()),
                SubjectPattern::Path("/user/admin".parse().unwrap()),
            ]),
            Self::render,
        )
    }
}
impl Renderer {
    async fn render(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let request: Request = serde_json::from_str(&input.arguments_json)
            .map_err(|e| Error::Arguments(e.to_string()))?;
        if request.source.trim().is_empty() || request.source.len() > 100_000 {
            return Err(Error::Arguments(
                "source must contain 1–100000 UTF-8 bytes".into(),
            ));
        }
        let mut images = BTreeMap::new();
        for (name, reference) in request.images {
            if name.is_empty()
                || name.starts_with('/')
                || name
                    .split('/')
                    .any(|s| s.is_empty() || s == "." || s == "..")
                || name.contains('\\')
                || name == "main.typ"
            {
                return Err(Error::Arguments(
                    "image names must be relative virtual file paths other than main.typ".into(),
                ));
            }
            typst::syntax::VirtualPath::new(&name).map_err(|e| Error::Arguments(e.to_string()))?;
            let blob: Result<media::Blob, media::Error> = host::call(
                media::OBJECT.parse().unwrap(),
                "/get".parse().unwrap(),
                &reference,
            )
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
            images.insert(
                name,
                blob.map_err(|e| Error::Execution(format!("{e:?}")))?.bytes,
            );
        }
        // No host I/O occurs inside Typst's synchronous World callbacks.
        let rendered = world::render(&self.fonts, &self.settings, request.source, images);
        // Retain no compilation graph across unrelated document requests.
        typst::comemo::evict(0);
        let (pages, warnings) = rendered?;
        let mut answer = Answer {
            pages: Vec::new(),
            warnings,
        };
        for (index, page) in pages.into_iter().enumerate() {
            let result: Result<String, media::Error> = host::call(
                media::OBJECT.parse().unwrap(),
                "/put".parse().unwrap(),
                &media::Blob {
                    mime: "image/png".into(),
                    bytes: page.bytes,
                },
            )
            .await
            .map_err(|e| Error::Execution(e.to_string()))?;
            let reference = result.map_err(|e| Error::Execution(format!("{e:?}")))?;
            answer.pages.push(Page {
                page: index + 1,
                cq: format!("[CQ:image,file={reference}]"),
                reference,
                width: page.width,
                height: page.height,
            });
        }
        serde_json::to_string(&answer).map_err(|e| Error::Execution(e.to_string()))
    }
}
forest_sdk::export_module!(super::Renderer);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Typst".into()),
            description: Some("Typeset documents and equations into managed PNG references".into()),
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
        provides: Renderer::handlers().declarations(),
        uses: vec![
            InterfaceUse {
                object: media::OBJECT.parse().unwrap(),
                interface: "/put".parse().unwrap(),
                input: media::Blob::as_schema(),
                output: Result::<String, media::Error>::as_schema(),
            },
            InterfaceUse {
                object: media::OBJECT.parse().unwrap(),
                interface: "/get".parse().unwrap(),
                input: String::as_schema(),
                output: Result::<media::Blob, media::Error>::as_schema(),
            },
        ],
        subscriptions: vec![],
    }
}
