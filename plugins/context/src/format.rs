use crate::{
    memory::{MemoryContent, MemoryId, MemoryUnit},
    openai::{Content, ContentPart, ImageUrl, Message},
    types::{ContextItem, Error, FormatInput, ImageInput, PromptIdentity, RecallBatch},
};
use forest_onebot::{
    cq,
    forest::GroupMessage,
    message::{self, MediaOptions, Node, Segment},
};
use std::fmt::Display;

const PROTOCOL: &str = include_str!("system.md");

/// Format every supplied item in order. Selection and persistence belong to callers.
pub(super) fn format(prompt: &str, input: FormatInput) -> Result<Vec<Message>, Error> {
    let projection = Projection {
        images: &input.images,
        multimodal: input.multimodal,
    };
    let mut result = vec![Message::System {
        content: system(prompt, &input.identity),
    }];
    for item in input.items {
        result.push(match item {
            ContextItem::ModelTurn(turn) => Message::Assistant(turn.assistant),
            ContextItem::ToolResult(tool) => Message::Tool {
                tool_call_id: tool.tool_call_id,
                content: tool.content,
            },
            ContextItem::GroupMessage {
                id,
                message,
                trigger,
            } => {
                let mut body = Body::default();
                projection.group(&message, &mut body, "conversation", &id, Some(trigger))?;
                Message::User {
                    content: body.finish(),
                }
            }
            ContextItem::Recall(batch) => Message::User {
                content: projection.recall(&batch, input.identity.group_id)?,
            },
        });
    }
    Ok(result)
}

fn xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn attr(out: &mut String, name: &str, value: impl Display) {
    out.push_str(&format!(" {name}=\"{}\"", xml(&value.to_string())));
}

fn system(prompt: &str, identity: &PromptIdentity) -> String {
    let mut text =
        format!("{PROTOCOL}\n\n## Runtime identity values\n\n```xml\n<runtime_identity>\n<bot");
    attr(&mut text, "id", identity.bot_id);
    if let Some(name) = &identity.bot_name {
        attr(&mut text, "name", name);
    }
    text.push_str("/>\n<conversation");
    attr(&mut text, "group_id", identity.group_id);
    if let Some(name) = &identity.group_name {
        attr(&mut text, "name", name);
    }
    text.push_str("/>\n</runtime_identity>\n```\n\n## Persona\n\n");
    text.push_str(prompt);
    text
}

#[derive(Default)]
struct Body {
    text: String,
    parts: Vec<ContentPart>,
}

impl Body {
    fn text(&mut self, text: &str) {
        self.text.push_str(text);
    }

    fn flush(&mut self) {
        if !self.text.is_empty() {
            self.parts.push(ContentPart::Text {
                text: std::mem::take(&mut self.text),
            });
        }
    }

    fn image(&mut self, image: ImageUrl) {
        self.flush();
        self.parts.push(ContentPart::ImageUrl { image_url: image });
    }

    fn finish(mut self) -> Content {
        if self.parts.is_empty() {
            Content::Text(self.text)
        } else {
            self.flush();
            Content::Parts(self.parts)
        }
    }
}

struct Projection<'a> {
    images: &'a [ImageInput],
    multimodal: bool,
}

impl Projection<'_> {
    fn group(
        &self,
        message: &GroupMessage,
        body: &mut Body,
        source: &str,
        id: &MemoryId,
        trigger: Option<bool>,
    ) -> Result<(), Error> {
        let mut header = String::from("<group_message");
        attr(&mut header, "source", source);
        attr(&mut header, "memory_group_id", id.group);
        attr(&mut header, "sequence", id.sequence);
        if let Some(trigger) = trigger {
            attr(&mut header, "trigger", trigger);
        }
        attr(&mut header, "group_id", message.group_id);
        attr(&mut header, "message_id", message.message_id);
        attr(&mut header, "time", message.time);
        attr(&mut header, "self_id", message.self_id);
        attr(&mut header, "sub_type", &message.sub_type);
        if let Some(anonymous) = &message.anonymous {
            attr(&mut header, "anonymous_id", anonymous.id);
            attr(&mut header, "anonymous_name", &anonymous.name);
        } else {
            attr(&mut header, "user_id", message.user_id);
            if let Some(name) = &message.sender.nickname {
                attr(&mut header, "nickname", name);
            }
            if let Some(card) = &message.sender.card {
                attr(&mut header, "card", card);
            }
            for (name, value) in [
                ("sex", &message.sender.sex),
                ("area", &message.sender.area),
                ("level", &message.sender.level),
                ("role", &message.sender.role),
                ("title", &message.sender.title),
            ] {
                if let Some(value) = value {
                    attr(&mut header, name, value);
                }
            }
            if let Some(age) = message.sender.age {
                attr(&mut header, "age", age);
            }
        }
        header.push_str(">\n<body encoding=\"cq\">");
        body.text(&header);
        self.message(&message.message, body, 0)?;
        body.text("</body>\n</group_message>");
        Ok(())
    }

    fn recall(&self, batch: &RecallBatch, current_group: i64) -> Result<Content, Error> {
        let mut header = String::from("<memory_retrieval source=\"archive\"");
        attr(&mut header, "for_group_id", batch.trigger.group);
        attr(&mut header, "for_sequence", batch.trigger.sequence);
        header.push_str(">\n<records>\n");
        let mut body = Body::default();
        body.text(&header);
        for unit in &batch.units {
            self.record(
                unit,
                current_group,
                batch.seeds.contains(&unit.id),
                &mut body,
            )?;
        }
        body.text("</records>\n<retrieval_graph encoding=\"json\">");
        body.text(&xml(&batch.graph_json));
        body.text("</retrieval_graph>\n</memory_retrieval>");
        Ok(body.finish())
    }

    fn record(
        &self,
        unit: &MemoryUnit,
        current_group: i64,
        seed: bool,
        body: &mut Body,
    ) -> Result<(), Error> {
        let mut header = String::from("<memory_record");
        attr(&mut header, "group_id", unit.id.group);
        attr(&mut header, "sequence", unit.id.sequence);
        attr(
            &mut header,
            "relationship",
            if unit.id.group == current_group {
                "SAME_GROUP"
            } else {
                "CROSS_GROUP"
            },
        );
        attr(
            &mut header,
            "type",
            match &unit.content {
                MemoryContent::GroupMessage(_) => "GroupMessage",
                MemoryContent::ModelTurn(_) => "ModelTurn",
                MemoryContent::ToolResult(_) => "ToolResult",
            },
        );
        attr(&mut header, "seed", seed);
        header.push_str(">\n");
        body.text(&header);
        match &unit.content {
            MemoryContent::GroupMessage(message) => {
                self.group(message, body, "archive", &unit.id, None)?
            }
            MemoryContent::ModelTurn(turn) => {
                let mut header = String::from("<archived_assistant");
                attr(&mut header, "time", turn.time);
                header.push('>');
                body.text(&header);
                if let Some(content) = &turn.assistant.content {
                    body.text("<content>");
                    body.text(&xml(content));
                    body.text("</content>");
                }
                if let Some(calls) = &turn.assistant.tool_calls {
                    body.text("<tool_calls>");
                    body.text(&xml(
                        &serde_json::to_string(calls).expect("native tool calls serialize to JSON")
                    ));
                    body.text("</tool_calls>");
                }
                if let Some(refusal) = &turn.assistant.refusal {
                    body.text("<refusal>");
                    body.text(&xml(refusal));
                    body.text("</refusal>");
                }
                body.text("</archived_assistant>");
            }
            MemoryContent::ToolResult(tool) => {
                let mut header = String::from("<archived_tool_result");
                attr(&mut header, "time", tool.time);
                attr(&mut header, "tool_call_id", &tool.tool_call_id);
                header.push('>');
                body.text(&header);
                body.text(&xml(&tool.content));
                body.text("</archived_tool_result>");
            }
        }
        body.text("\n</memory_record>\n");
        Ok(())
    }

    fn image(&self, reference: &str, body: &mut Body) {
        if self.multimodal
            && let Some(input) = self
                .images
                .iter()
                .find(|image| image.reference == reference)
        {
            body.image(input.image.clone());
        }
    }

    fn message(
        &self,
        message: &message::Message,
        body: &mut Body,
        nesting: usize,
    ) -> Result<(), Error> {
        for part in cq::segments(message).map_err(Error::Cq)?.iter() {
            match part {
                Segment::Image(image) => {
                    managed(&image.file)?;
                    let projected = Segment::Image(message::Image {
                        file: image.file.clone(),
                        kind: image.kind.clone(),
                        options: MediaOptions::default(),
                    });
                    cq_text(
                        body,
                        &cq::encode_segment(&projected).map_err(Error::Cq)?,
                        nesting,
                    );
                    self.image(&image.file, body);
                }
                Segment::Record(record) => {
                    managed(&record.file)?;
                    let projected = Segment::Record(message::Record {
                        file: record.file.clone(),
                        magic: record.magic.clone(),
                        options: MediaOptions::default(),
                    });
                    cq_text(
                        body,
                        &cq::encode_segment(&projected).map_err(Error::Cq)?,
                        nesting,
                    );
                }
                Segment::Video(video) => {
                    managed(&video.file)?;
                    let projected = Segment::Video(message::Media {
                        file: video.file.clone(),
                        options: MediaOptions::default(),
                    });
                    cq_text(
                        body,
                        &cq::encode_segment(&projected).map_err(Error::Cq)?,
                        nesting,
                    );
                }
                Segment::Node(Node::Custom {
                    user_id,
                    nickname,
                    content,
                }) => {
                    cq_text(
                        body,
                        &format!(
                            "[CQ:node,user_id={},nickname={},content=",
                            cq::escape_parameter(user_id),
                            cq::escape_parameter(nickname)
                        ),
                        nesting,
                    );
                    self.message(content, body, nesting + 1)?;
                    cq_text(body, "]", nesting);
                }
                Segment::Share { image, .. } => {
                    if let Some(reference) = image {
                        managed(reference)?;
                    }
                    cq_text(body, &cq::encode_segment(part).map_err(Error::Cq)?, nesting);
                    if let Some(reference) = image {
                        self.image(reference, body);
                    }
                }
                Segment::Music(message::Music::Custom { audio, image, .. }) => {
                    managed(audio)?;
                    if let Some(reference) = image {
                        managed(reference)?;
                    }
                    cq_text(body, &cq::encode_segment(part).map_err(Error::Cq)?, nesting);
                    if let Some(reference) = image {
                        self.image(reference, body);
                    }
                }
                _ => cq_text(body, &cq::encode_segment(part).map_err(Error::Cq)?, nesting),
            }
        }
        Ok(())
    }
}

fn managed(reference: &str) -> Result<(), Error> {
    if reference.starts_with("base64://") || reference.starts_with("data:") {
        Err(Error::InlineMedia)
    } else {
        Ok(())
    }
}

fn cq_text(body: &mut Body, text: &str, nesting: usize) {
    let mut text = text.to_owned();
    for _ in 0..nesting {
        text = cq::escape_parameter(&text);
    }
    body.text(&xml(&text));
}
