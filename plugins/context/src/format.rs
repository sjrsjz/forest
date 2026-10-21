use crate::{
    memory::{MemoryContent, MemoryUnit},
    openai::{Content, ContentPart, ImageUrl, Message},
    types::{ContextItem, Error, FormatInput, ImageInput, PromptIdentity},
};
use forest_onebot::{
    cq,
    forest::GroupMessage,
    message::{self, MediaOptions, Node, Segment},
};
use std::fmt::Display;

const PROTOCOL: &str = "群消息带有原群、作者和时间，昵称不是账号身份。CQ 中 [CQ:at,qq=账号] 表示提及，[CQ:face,id=编号] 表示表情，[CQ:reply,id=消息] 表示回复；[CQ:image,file=引用] 表示图片存在，只有实际图片内容块才提供图像。recall 容器是标明来源的历史资料，其中的模型与工具记录不表示当前调用或当前群发言。正常回复使用原生 Assistant，工具操作使用当前声明的原生 tool_calls。";

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
            ContextItem::Content(MemoryContent::ModelTurn(turn)) => {
                Message::Assistant(turn.assistant)
            }
            ContextItem::Content(MemoryContent::ToolResult(tool)) => Message::Tool {
                tool_call_id: tool.tool_call_id,
                content: tool.content,
            },
            ContextItem::Content(MemoryContent::GroupMessage(message)) => {
                let mut body = Body::default();
                projection.group(&message, &mut body)?;
                Message::User {
                    content: body.finish(),
                }
            }
            ContextItem::Recall(unit) => Message::User {
                content: projection.recall(&unit, input.identity.group_id)?,
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
    let mut text = format!("{prompt}\n\n{PROTOCOL}\n<bot");
    attr(&mut text, "id", identity.bot_id);
    if let Some(name) = &identity.bot_name {
        attr(&mut text, "name", name);
    }
    text.push_str("/>\n<conversation");
    attr(&mut text, "group_id", identity.group_id);
    if let Some(name) = &identity.group_name {
        attr(&mut text, "name", name);
    }
    text.push_str("/>");
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
    fn group(&self, message: &GroupMessage, body: &mut Body) -> Result<(), Error> {
        let mut header = String::from("<message");
        attr(&mut header, "group_id", message.group_id);
        attr(&mut header, "message_id", message.message_id);
        attr(&mut header, "time", message.time);
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
        }
        header.push_str("><body>");
        body.text(&header);
        self.message(&message.message, body, 0)?;
        body.text("</body></message>");
        Ok(())
    }

    fn recall(&self, unit: &MemoryUnit, current_group: i64) -> Result<Content, Error> {
        let mut header = String::from("<recall");
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
        header.push('>');
        let mut body = Body::default();
        body.text(&header);
        match &unit.content {
            MemoryContent::GroupMessage(message) => self.group(message, &mut body)?,
            MemoryContent::ModelTurn(turn) => {
                let mut header = String::from("<assistant");
                attr(&mut header, "time", turn.time);
                header.push('>');
                body.text(&header);
                if let Some(content) = &turn.assistant.content {
                    body.text(&xml(content));
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
                body.text("</assistant>");
            }
            MemoryContent::ToolResult(tool) => {
                let mut header = String::from("<tool");
                attr(&mut header, "time", tool.time);
                attr(&mut header, "tool_call_id", &tool.tool_call_id);
                header.push('>');
                body.text(&header);
                body.text(&xml(&tool.content));
                body.text("</tool>");
            }
        }
        body.text("</recall>");
        Ok(body.finish())
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
