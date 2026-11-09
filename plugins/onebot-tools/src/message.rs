use crate::{contract::Error, managed, markdown};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_onebot::message::{Message, Segment};
use forest_sdk::host;

pub async fn resolve(body: &str) -> Result<Vec<Message>, Error> {
    let parts: Result<Vec<markdown::Part>, markdown::Error> = host::call(
        markdown::OBJECT.parse().unwrap(),
        "/format".parse().unwrap(),
        &markdown::Format {
            group_id: None,
            source: body.into(),
        },
    )
    .await
    .map_err(|e| Error::Execution(e.to_string()))?;
    let parts = parts.map_err(|e| Error::Arguments(format!("{e:?}")))?;
    if parts.is_empty() {
        return Err(Error::Arguments(
            "message must contain a nonempty part".into(),
        ));
    }
    let mut messages = Vec::new();
    for part in parts {
        let mut segments = part.segments;
        for segment in &mut segments {
            if let Segment::Image(image) = segment {
                let data: Result<managed::Blob, managed::Error> = host::call(
                    managed::OBJECT.parse().unwrap(),
                    "/get".parse().unwrap(),
                    &image.file,
                )
                .await
                .map_err(|e| Error::Execution(e.to_string()))?;
                let data = data.map_err(|e| Error::Execution(format!("{e:?}")))?;
                image.file = format!("base64://{}", STANDARD.encode(data.bytes));
                image.options = Default::default();
            }
        }
        messages.push(Message::Segments(segments));
    }
    Ok(messages)
}
