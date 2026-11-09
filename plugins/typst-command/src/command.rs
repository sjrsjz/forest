use forest_onebot::{cq, forest::GroupMessage, message::Segment};

pub const HELP: &str = "发送 /typst 后接 Typst 源码，例如：\n/typst\n= 公式\n$ integral_0^1 x^2 dif x = 1/3 $\n\n也可以将源码放在 ```typst 代码块中。";

/// Read decoded text segments so CQ escaping cannot change Typst source.
pub fn source(event: &GroupMessage) -> Option<Result<String, &'static str>> {
    let segments = cq::segments(&event.message).ok()?;
    let mut text = String::new();
    for segment in segments.iter() {
        match segment {
            Segment::Text { text: part } => text.push_str(part),
            Segment::At { qq } if text.trim().is_empty() && qq == &event.self_id.to_string() => {}
            Segment::Reply { .. } if text.trim().is_empty() => {}
            _ => return body(&text).map(|_| Err("请在 /typst 后直接发送文本源码。")),
        }
    }
    body(&text).map(|source| {
        let source = source.trim();
        if source.starts_with("```") {
            let (fence, rest) = source
                .split_once('\n')
                .ok_or("代码块缺少源码或结束标记。")?;
            if !matches!(fence.trim_end(), "```" | "```typst" | "```typ") {
                return Err("请使用 typst 代码块。不要发送 Markdown 或 LaTeX 源码。");
            }
            let (source, end) = rest.rsplit_once('\n').ok_or("代码块缺少结束标记。")?;
            if end.trim() != "```" {
                return Err("代码块缺少结束标记。");
            }
            Ok(source.trim().to_owned())
        } else {
            Ok(source.to_owned())
        }
    })
}

fn body(text: &str) -> Option<&str> {
    let rest = text.trim_start().strip_prefix("/typst")?;
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return None;
    }
    Some(rest)
}
