//! OneBot v11 CQ text/parameter escaping and conversion of typed message segments.
use crate::message::*;
use std::{borrow::Cow, collections::BTreeMap};

#[cfg_attr(
    feature = "forest",
    derive(forest_serde::DeriveSchema, forest_serde::DeriveValue)
)]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("unterminated CQ code")]
    Unterminated,
    #[error("invalid CQ parameter")]
    InvalidParameter,
    #[error("unknown CQ type: {0}")]
    UnknownType(String),
    #[error("missing CQ parameter: {0}")]
    MissingParameter(String),
    #[error("invalid CQ parameter value: {0}")]
    InvalidValue(String),
}

pub fn escape_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('[', "&#91;")
        .replace(']', "&#93;")
}

pub fn escape_parameter(text: &str) -> String {
    escape_text(text).replace(',', "&#44;")
}

pub fn unescape_text(text: &str) -> String {
    text.replace("&#91;", "[")
        .replace("&#93;", "]")
        .replace("&amp;", "&")
}

pub fn unescape_parameter(text: &str) -> String {
    unescape_text(&text.replace("&#44;", ","))
}

pub fn segments(message: &Message) -> Result<Cow<'_, [Segment]>, Error> {
    match message {
        Message::Cq(text) => decode(text).map(Cow::Owned),
        Message::Segments(parts) => Ok(Cow::Borrowed(parts)),
        Message::Single(part) => Ok(Cow::Borrowed(std::slice::from_ref(part))),
    }
}

pub fn decode(mut text: &str) -> Result<Vec<Segment>, Error> {
    let mut result = Vec::new();
    while let Some(start) = text.find("[CQ:") {
        if start > 0 {
            result.push(Segment::text(unescape_text(&text[..start])));
        }
        let code = &text[start + 4..];
        let end = code.find(']').ok_or(Error::Unterminated)?;
        result.push(parse(&code[..end])?);
        text = &code[end + 1..];
    }
    if !text.is_empty() {
        result.push(Segment::text(unescape_text(text)));
    }
    Ok(result)
}

fn parse(code: &str) -> Result<Segment, Error> {
    let (kind, tail) = code.split_once(',').unwrap_or((code, ""));
    let mut params = BTreeMap::new();
    if !tail.is_empty() {
        for parameter in tail.split(',') {
            let (key, value) = parameter.split_once('=').ok_or(Error::InvalidParameter)?;
            if params.insert(key, unescape_parameter(value)).is_some() {
                return Err(Error::InvalidParameter);
            }
        }
    }
    let required = |key: &str| {
        params
            .get(key)
            .cloned()
            .ok_or_else(|| Error::MissingParameter(key.into()))
    };
    let optional = |key: &str| params.get(key).cloned();
    let options = || MediaOptions {
        url: optional("url"),
        cache: optional("cache"),
        proxy: optional("proxy"),
        timeout: optional("timeout"),
    };
    Ok(match kind {
        "text" => Segment::text(required("text")?),
        "face" => Segment::Face {
            id: required("id")?,
        },
        "image" => Segment::Image(Image {
            file: required("file")?,
            kind: match params.get("type").map(String::as_str) {
                None => None,
                Some("flash") => Some(ImageKind::Flash),
                Some(_) => return Err(Error::InvalidValue("image.type".into())),
            },
            options: options(),
        }),
        "record" => Segment::Record(Record {
            file: required("file")?,
            magic: optional("magic"),
            options: options(),
        }),
        "video" => Segment::Video(Media {
            file: required("file")?,
            options: options(),
        }),
        "at" => Segment::at(required("qq")?),
        "rps" => Segment::Rps {},
        "dice" => Segment::Dice {},
        "shake" => Segment::Shake {},
        "poke" => Segment::Poke {
            kind: required("type")?,
            id: required("id")?,
            name: optional("name"),
        },
        "anonymous" => Segment::Anonymous {
            ignore: optional("ignore"),
        },
        "share" => Segment::Share {
            url: required("url")?,
            title: required("title")?,
            content: optional("content"),
            image: optional("image"),
        },
        "contact" => Segment::Contact(match required("type")?.as_str() {
            "qq" => Contact::Qq {
                id: required("id")?,
            },
            "group" => Contact::Group {
                id: required("id")?,
            },
            _ => return Err(Error::InvalidValue("contact.type".into())),
        }),
        "location" => Segment::Location {
            lat: required("lat")?,
            lon: required("lon")?,
            title: optional("title"),
            content: optional("content"),
        },
        "music" => Segment::Music(match required("type")?.as_str() {
            "qq" => Music::Qq {
                id: required("id")?,
            },
            "163" => Music::Netease {
                id: required("id")?,
            },
            "xm" => Music::Xiami {
                id: required("id")?,
            },
            "custom" => Music::Custom {
                url: required("url")?,
                audio: required("audio")?,
                title: required("title")?,
                content: optional("content"),
                image: optional("image"),
            },
            _ => return Err(Error::InvalidValue("music.type".into())),
        }),
        "reply" => Segment::Reply {
            id: required("id")?,
        },
        "forward" => Segment::Forward {
            id: required("id")?,
        },
        "node" => Segment::Node(if let Some(id) = optional("id") {
            Node::Reference { id }
        } else {
            Node::Custom {
                user_id: required("user_id")?,
                nickname: required("nickname")?,
                content: Message::Cq(required("content")?),
            }
        }),
        "xml" => Segment::Xml {
            data: required("data")?,
        },
        "json" => Segment::Json {
            data: required("data")?,
        },
        _ => return Err(Error::UnknownType(kind.into())),
    })
}

pub fn encode(message: &Message) -> Result<String, Error> {
    let mut text = String::new();
    for part in segments(message)?.iter() {
        text.push_str(&encode_segment(part)?);
    }
    Ok(text)
}

fn code(kind: &str, required: &[(&str, &str)], optional: &[(&str, Option<&str>)]) -> String {
    let mut result = format!("[CQ:{kind}");
    for &(key, value) in required {
        result.push_str(&format!(",{key}={}", escape_parameter(value)));
    }
    for &(key, value) in optional {
        if let Some(value) = value {
            result.push_str(&format!(",{key}={}", escape_parameter(value)));
        }
    }
    result.push(']');
    result
}

pub fn encode_segment(part: &Segment) -> Result<String, Error> {
    Ok(match part {
        Segment::Text { text } => escape_text(text),
        Segment::Face { id } => code("face", &[("id", id)], &[]),
        Segment::Image(image) => code(
            "image",
            &[("file", &image.file)],
            &[
                ("type", image.kind.as_ref().map(|_| "flash")),
                ("url", image.options.url.as_deref()),
                ("cache", image.options.cache.as_deref()),
                ("proxy", image.options.proxy.as_deref()),
                ("timeout", image.options.timeout.as_deref()),
            ],
        ),
        Segment::Record(record) => code(
            "record",
            &[("file", &record.file)],
            &[
                ("magic", record.magic.as_deref()),
                ("url", record.options.url.as_deref()),
                ("cache", record.options.cache.as_deref()),
                ("proxy", record.options.proxy.as_deref()),
                ("timeout", record.options.timeout.as_deref()),
            ],
        ),
        Segment::Video(media) => code(
            "video",
            &[("file", &media.file)],
            &[
                ("url", media.options.url.as_deref()),
                ("cache", media.options.cache.as_deref()),
                ("proxy", media.options.proxy.as_deref()),
                ("timeout", media.options.timeout.as_deref()),
            ],
        ),
        Segment::At { qq } => code("at", &[("qq", qq)], &[]),
        Segment::Rps {} => code("rps", &[], &[]),
        Segment::Dice {} => code("dice", &[], &[]),
        Segment::Shake {} => code("shake", &[], &[]),
        Segment::Poke { kind, id, name } => code(
            "poke",
            &[("type", kind), ("id", id)],
            &[("name", name.as_deref())],
        ),
        Segment::Anonymous { ignore } => code("anonymous", &[], &[("ignore", ignore.as_deref())]),
        Segment::Share {
            url,
            title,
            content,
            image,
        } => code(
            "share",
            &[("url", url), ("title", title)],
            &[("content", content.as_deref()), ("image", image.as_deref())],
        ),
        Segment::Contact(contact) => match contact {
            Contact::Qq { id } => code("contact", &[("type", "qq"), ("id", id)], &[]),
            Contact::Group { id } => code("contact", &[("type", "group"), ("id", id)], &[]),
        },
        Segment::Location {
            lat,
            lon,
            title,
            content,
        } => code(
            "location",
            &[("lat", lat), ("lon", lon)],
            &[("title", title.as_deref()), ("content", content.as_deref())],
        ),
        Segment::Music(music) => match music {
            Music::Qq { id } => code("music", &[("type", "qq"), ("id", id)], &[]),
            Music::Netease { id } => code("music", &[("type", "163"), ("id", id)], &[]),
            Music::Xiami { id } => code("music", &[("type", "xm"), ("id", id)], &[]),
            Music::Custom {
                url,
                audio,
                title,
                content,
                image,
            } => code(
                "music",
                &[
                    ("type", "custom"),
                    ("url", url),
                    ("audio", audio),
                    ("title", title),
                ],
                &[("content", content.as_deref()), ("image", image.as_deref())],
            ),
        },
        Segment::Reply { id } => code("reply", &[("id", id)], &[]),
        Segment::Forward { id } => code("forward", &[("id", id)], &[]),
        Segment::Node(node) => match node {
            Node::Reference { id } => code("node", &[("id", id)], &[]),
            Node::Custom {
                user_id,
                nickname,
                content,
            } => code(
                "node",
                &[
                    ("user_id", user_id),
                    ("nickname", nickname),
                    ("content", &encode(content)?),
                ],
                &[],
            ),
        },
        Segment::Xml { data } => code("xml", &[("data", data)], &[]),
        Segment::Json { data } => code("json", &[("data", data)], &[]),
    })
}
