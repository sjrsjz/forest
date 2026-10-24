use crate::{Settings, contract::GroupOutput};
use forest_onebot::message::{Message, Node, Segment};

/// Transform only the transport representation; preserve every segment in order.
pub fn format(input: &GroupOutput, segments: Vec<Segment>, settings: &Settings) -> Message {
    let enabled = settings
        .groups
        .iter()
        .find(|g| g.group_id == input.group_id)
        .map_or(settings.enabled_by_default, |g| g.enabled);
    let (mut chars, mut lines) = (0usize, 0usize);
    for segment in &segments {
        if let Segment::Text { text } = segment {
            chars += text.chars().count();
            lines += text.chars().filter(|&c| c == '\n').count();
        }
    }
    lines += usize::from(chars != 0);
    if !enabled
        || (chars <= settings.character_threshold as usize
            && lines <= settings.line_threshold as usize)
    {
        return Message::Segments(segments);
    }

    Message::Segments(vec![Segment::Node(Node::Custom {
        user_id: input.bot_id.to_string(),
        nickname: input.bot_name.clone().unwrap_or_else(|| "Bot".into()),
        content: Message::Segments(segments),
    })])
}
