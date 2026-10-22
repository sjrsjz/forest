//! Derived CQ and embedding views. Never rewrites the stored source object.
use crate::{content::*, types::Error};
use forest_onebot::{
    cq,
    message::{MediaOptions, Message, Node, Segment},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub struct Source {
    pub reference: String,
    pub path: String,
    pub source: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Representation {
    Text(String),
    Image(String),
}
pub fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}
pub fn point_id(text: &str) -> String {
    let id = digest(text);
    format!(
        "{}-{}-{}-{}-{}",
        &id[..8],
        &id[8..12],
        &id[12..16],
        &id[16..20],
        &id[20..32]
    )
}
pub fn reference(group: i64, key: &str, path: &str) -> String {
    format!("media:{}", digest(&format!("{group}:{key}:{path}")))
}
pub fn sources(group: i64, key: &str, content: &MemoryContent) -> Result<Vec<Source>, Error> {
    let mut sources = Vec::new();
    if let MemoryContent::GroupMessage(message) = content {
        visit(&message.message, "", &mut |path, segment| {
            if let Some((file, options)) = media(segment) {
                sources.push(Source {
                    reference: reference(group, key, path),
                    path: path.into(),
                    source: options.url.clone().unwrap_or_else(|| file.clone()),
                });
            }
        })?;
    }
    Ok(sources)
}
pub fn projected(unit: &MemoryUnit) -> Result<MemoryUnit, Error> {
    let mut output = unit.clone();
    if let MemoryContent::GroupMessage(message) = &mut output.content {
        let key = format!("qq:{}:{}", message.self_id, message.message_id);
        message.message = map(&message.message, "", &mut |path, segment| match segment {
            Segment::Image(v) => {
                v.file = reference(unit.id.group, &key, path);
                v.options = MediaOptions::default();
            }
            Segment::Record(v) => {
                v.file = reference(unit.id.group, &key, path);
                v.options = MediaOptions::default();
            }
            Segment::Video(v) => {
                v.file = reference(unit.id.group, &key, path);
                v.options = MediaOptions::default();
            }
            _ => {}
        })?;
        // Context reads message; keeping a second unprojected body would expose inline bytes.
        message.raw_message = cq::encode(&message.message).map_err(cq_error)?;
    }
    Ok(output)
}
fn media(segment: &Segment) -> Option<(&String, &MediaOptions)> {
    match segment {
        Segment::Image(v) => Some((&v.file, &v.options)),
        Segment::Record(v) => Some((&v.file, &v.options)),
        Segment::Video(v) => Some((&v.file, &v.options)),
        _ => None,
    }
}
pub fn image_references(unit: &MemoryUnit) -> Result<Vec<String>, Error> {
    let mut result = Vec::new();
    if let MemoryContent::GroupMessage(message) = &projected(unit)?.content {
        visit(&message.message, "", &mut |_, segment| {
            if let Segment::Image(v) = segment {
                result.push(v.file.clone());
            }
        })?;
    }
    Ok(result)
}
pub fn representations(unit: &MemoryUnit) -> Result<Vec<Representation>, Error> {
    let projected = projected(unit)?;
    let body = match &projected.content {
        MemoryContent::GroupMessage(message) => cq::encode(&message.message).map_err(cq_error)?,
        MemoryContent::ModelTurn(turn) => {
            let mut body = turn.assistant.content.clone().unwrap_or_default();
            if let Some(refusal) = &turn.assistant.refusal {
                body.push_str(refusal);
            }
            if let Some(calls) = &turn.assistant.tool_calls {
                for call in calls {
                    let crate::openai::ToolCall::Function { function, .. } = call;
                    body.push_str(&format!("\n{}({})", function.name, function.arguments));
                }
            }
            body
        }
        MemoryContent::ToolResult(tool) => tool.content.clone(),
    };
    let mut result = chunks(&body, 2048)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .map(Representation::Text)
        .collect::<Vec<_>>();
    result.extend(
        image_references(unit)?
            .into_iter()
            .map(Representation::Image),
    );
    Ok(result)
}
/// CQ controls and escape entities are indivisible; long text keeps every scalar.
pub fn chunks(text: &str, limit: usize) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut count = 0;
    let mut rest = text;
    while !rest.is_empty() {
        let end = if rest.starts_with("[CQ:") {
            rest.find(']').map(|n| n + 1)
        } else if rest.starts_with('&') {
            ["&amp;", "&#91;", "&#93;", "&#44;"]
                .into_iter()
                .find(|v| rest.starts_with(v))
                .map(str::len)
        } else {
            None
        };
        let end = end.unwrap_or_else(|| rest.chars().next().unwrap().len_utf8());
        let atom = &rest[..end];
        let size = atom.chars().count();
        if count + size > limit && !current.is_empty() {
            result.push(std::mem::take(&mut current));
            count = 0;
        }
        current.push_str(atom);
        count += size;
        rest = &rest[end..];
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}
fn visit(message: &Message, path: &str, f: &mut impl FnMut(&str, &Segment)) -> Result<(), Error> {
    for (index, segment) in cq::segments(message).map_err(cq_error)?.iter().enumerate() {
        let path = format!("{path}/{index}");
        f(&path, segment);
        if let Segment::Node(Node::Custom { content, .. }) = segment {
            visit(content, &path, f)?;
        }
    }
    Ok(())
}
fn map(
    message: &Message,
    path: &str,
    f: &mut impl FnMut(&str, &mut Segment),
) -> Result<Message, Error> {
    let mut segments = cq::segments(message).map_err(cq_error)?.into_owned();
    for (index, segment) in segments.iter_mut().enumerate() {
        let path = format!("{path}/{index}");
        f(&path, segment);
        if let Segment::Node(Node::Custom { content, .. }) = segment {
            *content = map(content, &path, f)?;
        }
    }
    Ok(Message::Segments(segments))
}
fn cq_error(error: cq::Error) -> Error {
    Error::InvalidInput(error.to_string())
}
