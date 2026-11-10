pub mod contract;
mod encode;
mod parse;
pub mod typst;

use contract::*;
use forest_onebot::message::{Image, Segment};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use std::rc::Rc;

pub struct Markdown;
impl Module for Markdown {
    type LifecycleError = std::convert::Infallible;

    async fn create(_: CreateContext) -> Result<Self, Self::LifecycleError> {
        Ok(Self)
    }
    fn handlers() -> Handlers<Self> {
        let access = callers(vec![
            SubjectPattern::Path("/module/**".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with(
                "/format".parse().unwrap(),
                true,
                access.clone(),
                Self::format,
            )
            .with("/encode".parse().unwrap(), true, access, Self::encode)
    }
}
impl Markdown {
    async fn encode(
        self: Rc<Self>,
        _: CallContext,
        inputs: Vec<Encode>,
    ) -> Result<Vec<Vec<Fragment>>, Error> {
        inputs.into_iter().map(encode::encode).collect()
    }
    async fn format(self: Rc<Self>, _: CallContext, input: Format) -> Result<Vec<Part>, Error> {
        // Parse the entire document before rendering or delivering any part.
        let drafts = parse::parse(&input)?;
        let mut parts = Vec::new();
        for draft in drafts {
            let mut segments = Vec::new();
            for atom in draft.atoms {
                match atom {
                    parse::Atom::Segment(segment) => segments.push(segment),
                    parse::Atom::Typeset { source, original } => {
                        let rendered = host::call::<_, Result<Vec<String>, typst::Error>>(
                            typst::OBJECT.parse().unwrap(),
                            "/typeset".parse().unwrap(),
                            &source,
                        )
                        .await;
                        match rendered {
                            Ok(Ok(images)) if !images.is_empty() => {
                                segments.extend(images.into_iter().map(|file| {
                                    Segment::Image(Image {
                                        file,
                                        kind: None,
                                        options: Default::default(),
                                    })
                                }))
                            }
                            result => {
                                let _ = host::log(
                                    service::LogLevel::Warn,
                                    format!("Markdown node kept as source: {result:?}"),
                                )
                                .await;
                                segments.push(Segment::text(original));
                            }
                        }
                    }
                }
            }
            parts.push(Part {
                segments,
                source_characters: draft.characters,
                source_lines: draft.lines,
            });
        }
        Ok(parts)
    }
}
forest_sdk::export_module!(super::Markdown);

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Markdown messages".into()),
            description: Some(
                "Source-preserving Markdown, typed message links and rich blocks".into(),
            ),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Markdown as Module>::LifecycleError::as_schema(),
        configs: vec![],
        provides: Markdown::handlers().declarations(),
        uses: vec![InterfaceUse {
            object: typst::OBJECT.parse().unwrap(),
            interface: "/typeset".parse().unwrap(),
            input: String::as_schema(),
            output: Result::<Vec<String>, typst::Error>::as_schema(),
        }],
        subscriptions: vec![],
    }
}
