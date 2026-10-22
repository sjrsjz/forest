use crate::{contract::Error, managed};
use base64::{Engine, engine::general_purpose::STANDARD};
use forest_onebot::{
    cq,
    message::{Message, Segment},
};
use forest_sdk::host;

pub async fn resolve(body: &str) -> Result<Message, Error> {
    if body.trim().is_empty() {
        return Err(Error::Arguments("message must not be empty".into()));
    }
    let mut segments = cq::decode(body).map_err(|e| Error::Arguments(e.to_string()))?;
    for segment in &segments {
        if !matches!(
            segment,
            Segment::Text { .. }
                | Segment::At { .. }
                | Segment::Reply { .. }
                | Segment::Face { .. }
                | Segment::Image(_)
        ) {
            return Err(Error::Arguments(
                "supported CQ segments: text, at, reply, face, image".into(),
            ));
        }
    }
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
    Ok(Message::Segments(segments))
}
