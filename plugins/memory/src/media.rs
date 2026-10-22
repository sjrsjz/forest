use crate::{Memory, managed, openai::ImageUrl, store::*, types::*};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_protocol::service::LogLevel;
use forest_sdk::host;
use std::{collections::BTreeSet, rc::Rc};

pub async fn register(source: crate::projection::Source) -> Result<(), Error> {
    let result: Result<(), managed::Error> = host::call(
        managed::OBJECT.parse().unwrap(),
        "/register".parse().unwrap(),
        &managed::Source {
            reference: source.reference,
            source: source.source,
        },
    )
    .await?;
    result.map_err(|e| Error::Media(format!("{e:?}")))
}
impl Memory {
    pub async fn image(&self, reference: &str) -> Result<String, Error> {
        // Raw message associations remain Memory's authority; bytes belong to Media.
        let rows = execute(vec![stmt(
            "SELECT source FROM media WHERE reference=?",
            vec![text(reference)],
        )])
        .await?;
        if let Some(row) = rows[0].rows.first() {
            let result: Result<(), managed::Error> = host::call(
                managed::OBJECT.parse().unwrap(),
                "/register".parse().unwrap(),
                &managed::Source {
                    reference: reference.into(),
                    source: string(&row[0])?.into(),
                },
            )
            .await?;
            result.map_err(|e| Error::Media(format!("{e:?}")))?;
        }
        let result: Result<managed::Blob, managed::Error> = host::call(
            managed::OBJECT.parse().unwrap(),
            "/get".parse().unwrap(),
            &reference.to_owned(),
        )
        .await?;
        let blob = result.map_err(|e| Error::Media(format!("{e:?}")))?;
        Ok(format!(
            "data:{};base64,{}",
            blob.mime,
            STANDARD.encode(blob.bytes)
        ))
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
        // Publish source identities even for text-only model projections.
        for unit in &units {
            if let crate::content::MemoryContent::GroupMessage(message) = &unit.content {
                let key = format!("qq:{}:{}", message.self_id, message.message_id);
                for source in crate::projection::sources(unit.id.group, &key, &unit.content)? {
                    register(source).await?;
                }
            }
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
