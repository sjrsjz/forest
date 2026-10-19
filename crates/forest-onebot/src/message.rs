//! Standard v11 message representations. File references remain references.

use serde::{Deserialize, Serialize};

/// All three representations accepted by the standard. No CQ parsing is implicit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    Cq(String),
    Segments(Vec<Segment>),
    Single(Box<Segment>),
}

/// Segment data uses strings as specified by the v11 message wire format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Segment {
    Text {
        text: String,
    },
    Face {
        id: String,
    },
    Image(Image),
    Record(Record),
    Video(Media),
    At {
        qq: String,
    },
    Rps {},
    Dice {},
    Shake {},
    Poke {
        #[serde(rename = "type")]
        kind: String,
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    Anonymous {
        #[serde(skip_serializing_if = "Option::is_none")]
        ignore: Option<String>,
    },
    Share {
        url: String,
        title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        image: Option<String>,
    },
    Contact(Contact),
    Location {
        lat: String,
        lon: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
    },
    Music(Music),
    Reply {
        id: String,
    },
    Forward {
        id: String,
    },
    Node(Node),
    Xml {
        data: String,
    },
    Json {
        data: String,
    },
}

impl Segment {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn at(qq: impl Into<String>) -> Self {
        Self::At { qq: qq.into() }
    }

    pub fn image(file: impl Into<String>) -> Self {
        Self::Image(Image {
            file: file.into(),
            kind: None,
            options: MediaOptions::default(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Media {
    pub file: String,
    #[serde(flatten)]
    pub options: MediaOptions,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Image {
    pub file: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<ImageKind>,
    #[serde(flatten)]
    pub options: MediaOptions,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageKind {
    Flash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub magic: Option<String>,
    #[serde(flatten)]
    pub options: MediaOptions,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Contact {
    Qq { id: String },
    Group { id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Music {
    #[serde(rename = "qq")]
    Qq { id: String },
    #[serde(rename = "163")]
    Netease { id: String },
    #[serde(rename = "xm")]
    Xiami { id: String },
    #[serde(rename = "custom")]
    Custom {
        url: String,
        audio: String,
        title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        image: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Node {
    Reference {
        id: String,
    },
    Custom {
        user_id: String,
        nickname: String,
        content: Message,
    },
}
