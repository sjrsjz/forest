use crate::contract::{Encode, Error, Fragment};
use forest_onebot::{
    cq,
    message::{Message, Node, Segment},
};

/// Escape literal chat text before embedding it in a Markdown document.
pub fn literal(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    let mut previous: Option<char> = None;
    let mut line_prefix = true;
    while let Some(c) = chars.next() {
        let escape = matches!(
            c,
            '\\' | '[' | ']' | '`' | '*' | '$' | '#' | '!' | '|' | '<' | '>' | '~'
        ) || (c == '_'
            && !(previous.is_some_and(char::is_alphanumeric)
                && chars.peek().is_some_and(|c| c.is_alphanumeric())))
            || (line_prefix && matches!(c, '-' | '+' | '='))
            || (c == '.'
                && previous.is_some_and(|c| c.is_ascii_digit())
                && chars.peek().is_some_and(|c| c.is_whitespace()));
        if escape {
            out.push('\\');
        }
        out.push(c);
        if c == '\n' {
            line_prefix = true;
        } else if !c.is_whitespace() {
            line_prefix = false;
        }
        previous = Some(c);
    }
    out
}

pub fn encode(input: Encode) -> Result<Vec<Fragment>, Error> {
    let mut out = Vec::new();
    message(input.group_id, &input.message, &mut out)?;
    Ok(out)
}

fn text(out: &mut Vec<Fragment>, value: &str) {
    if let Some(Fragment::Text(previous)) = out.last_mut() {
        previous.push_str(value);
    } else {
        out.push(Fragment::Text(value.into()));
    }
}

fn message(group: i64, input: &Message, out: &mut Vec<Fragment>) -> Result<(), Error> {
    for segment in cq::segments(input)
        .map_err(|e| Error::Input(e.to_string()))?
        .iter()
    {
        match segment {
            Segment::Text { text: value } | Segment::Markdown { content: value } => {
                text(out, &literal(value))
            }
            Segment::At { qq } => text(out, &format!("[@{qq}](forest:at/{qq})")),
            Segment::Reply { id } => text(out, &format!("[reply](forest:reply/{group}/{id})")),
            Segment::Face { id } => text(out, &format!("[face {id}](forest:face/{id})")),
            Segment::Image(image) => {
                no_inline(&image.file)?;
                text(out, &format!("![image]({})", image.file));
                out.push(Fragment::Image(image.file.clone()));
            }
            Segment::Node(Node::Custom {
                user_id,
                nickname,
                content,
            }) => {
                text(
                    out,
                    &format!(
                        "\n<forwarded_message user_id={} nickname={}>\n",
                        serde_json::to_string(user_id).unwrap(),
                        serde_json::to_string(nickname).unwrap()
                    ),
                );
                message(group, content, out)?;
                text(out, "\n</forwarded_message>\n");
            }
            _ => {
                // Unsupported native types remain inspectable data; no CQ escaping layers.
                let value =
                    serde_json::to_value(segment).map_err(|e| Error::Input(e.to_string()))?;
                reject_inline(&value)?;
                text(
                    out,
                    &format!("\n<onebot_segment>{value}</onebot_segment>\n"),
                );
            }
        }
    }
    Ok(())
}
fn no_inline(value: &str) -> Result<(), Error> {
    if value.starts_with("base64://") || value.starts_with("data:") {
        Err(Error::Input(
            "media must be managed before projection".into(),
        ))
    } else {
        Ok(())
    }
}
fn reject_inline(value: &serde_json::Value) -> Result<(), Error> {
    match value {
        serde_json::Value::Array(a) => {
            for v in a {
                reject_inline(v)?;
            }
            Ok(())
        }
        serde_json::Value::Object(o) => {
            for (key, v) in o {
                if matches!(key.as_str(), "file" | "url" | "image" | "audio") {
                    if let Some(value) = v.as_str() {
                        no_inline(value)?;
                    }
                }
                reject_inline(v)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
