//! A source-preserving plan from one CommonMark parser. No I/O here.
use crate::contract::{Error, Format};
use forest_onebot::message::{Image, Segment};
use pulldown_cmark::{Alignment, CodeBlockKind, Event, Options, Parser, Tag};
use std::ops::Range;

fn math(text: &str) -> Result<String, String> {
    Ok(format!(
        "#forest-equation({})",
        string(&mitex::convert_math(text, None)?)
    ))
}

#[derive(Debug)]
pub enum Atom {
    Segment(Segment),
    Typeset { source: String, original: String },
}
#[derive(Debug, Default)]
pub struct Draft {
    pub atoms: Vec<Atom>,
    pub characters: i64,
    pub lines: i64,
    reply: Option<String>,
}

struct Node<'a> {
    event: Event<'a>,
    span: Range<usize>,
    children: Vec<Node<'a>>,
}

fn nodes<'a>(events: &mut impl Iterator<Item = (Event<'a>, Range<usize>)>) -> Vec<Node<'a>> {
    let mut out = Vec::new();
    while let Some((event, span)) = events.next() {
        match event {
            Event::End(_) => break,
            Event::Start(_) => {
                let children = nodes(events);
                out.push(Node {
                    event,
                    span,
                    children,
                });
            }
            _ => out.push(Node {
                event,
                span,
                children: Vec::new(),
            }),
        }
    }
    out
}

pub fn parse(input: &Format) -> Result<Vec<Draft>, Error> {
    let options = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_MATH;
    let tree = nodes(&mut Parser::new_ext(&input.source, options).into_offset_iter());
    let mut builder = Builder {
        input,
        parts: vec![],
        current: Draft::default(),
    };
    builder.walk(0..input.source.len(), &tree)?;
    builder.finish();
    Ok(builder.parts)
}

struct Builder<'a> {
    input: &'a Format,
    parts: Vec<Draft>,
    current: Draft,
}
impl Builder<'_> {
    fn account(&mut self, source: &str) {
        self.current.characters += source.chars().count() as i64;
        self.current.lines += source.chars().filter(|&c| c == '\n').count() as i64;
    }
    fn raw(&mut self, range: Range<usize>) {
        let source = &self.input.source[range];
        self.account(source);
        self.segment(Segment::text(source));
    }
    fn segment(&mut self, segment: Segment) {
        if let Segment::Text { text } = &segment {
            if text.is_empty() {
                return;
            }
            if let Some(Atom::Segment(Segment::Text { text: previous })) =
                self.current.atoms.last_mut()
            {
                previous.push_str(text);
                return;
            }
        }
        self.current.atoms.push(Atom::Segment(segment));
    }
    fn finish(&mut self) {
        let meaningful =
            self.current.atoms.iter().any(
                |a| !matches!(a, Atom::Segment(Segment::Text {text}) if text.trim().is_empty()),
            );
        if meaningful {
            if let Some(id) = self.current.reply.take() {
                self.current
                    .atoms
                    .insert(0, Atom::Segment(Segment::Reply { id }));
            }
            self.current.lines += 1;
            self.parts.push(std::mem::take(&mut self.current));
        } else {
            self.current = Draft::default();
        }
    }
    fn walk(&mut self, span: Range<usize>, children: &[Node<'_>]) -> Result<(), Error> {
        let mut cursor = span.start;
        for node in children {
            // OffsetIter excludes the escape backslash from a decoded Text event.
            // Keep source accounting, but do not turn that syntax gap into prose.
            let gap = &self.input.source[cursor..node.span.start];
            if matches!(node.event, Event::Text(_)) && gap.ends_with('\\') {
                self.raw(cursor..node.span.start - 1);
                self.account("\\");
            } else {
                self.raw(cursor..node.span.start);
            }
            self.node(node)?;
            cursor = node.span.end;
        }
        self.raw(cursor..span.end);
        Ok(())
    }
    fn node(&mut self, node: &Node<'_>) -> Result<(), Error> {
        let original = &self.input.source[node.span.clone()];
        match &node.event {
            Event::Rule => self.finish(),
            Event::Start(Tag::CodeBlock(kind)) => {
                let code = node
                    .children
                    .iter()
                    .filter_map(|n| match &n.event {
                        Event::Text(t) => Some(t.as_ref()),
                        _ => None,
                    })
                    .collect::<String>();
                let lang = match kind {
                    CodeBlockKind::Fenced(info) => info.split_whitespace().next().unwrap_or(""),
                    _ => "",
                };
                self.typeset(
                    format!(
                        "#raw({}, block: true, lang: {})",
                        string(&code),
                        string(lang)
                    ),
                    original,
                    false,
                );
            }
            Event::Start(Tag::Table(alignments)) if !controls(node) => {
                if let Ok(source) = table(node, alignments) {
                    self.typeset(source, original, has_math(node));
                } else {
                    self.raw(node.span.clone());
                }
            }
            Event::DisplayMath(math) if self.standalone(node) => match self::math(math) {
                Ok(math) => self.typeset(
                    format!("#set page(width: auto, margin: 6pt)\n{math}"),
                    original,
                    true,
                ),
                Err(_) => self.raw(node.span.clone()),
            },
            Event::Start(Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. })
                if dest_url.starts_with("forest:") || dest_url.starts_with("media:") =>
            {
                let image = matches!(node.event, Event::Start(Tag::Image { .. }));
                let target = target(dest_url, image, self.input.group_id)?;
                match target {
                    Segment::Reply { id } => {
                        if self.current.reply.replace(id).is_some() {
                            return Err(Error::Protocol(
                                "one reply target per message is allowed".into(),
                            ));
                        }
                    }
                    other => self.segment(other),
                }
            }
            Event::Start(Tag::HtmlBlock)
            | Event::Start(Tag::Image { .. } | Tag::Link { .. })
            | Event::Html(_)
            | Event::InlineHtml(_) => self.raw(node.span.clone()),
            Event::Code(_) | Event::InlineMath(_) | Event::DisplayMath(_) => {
                self.raw(node.span.clone())
            }
            Event::Text(text) => {
                self.account(original);
                self.segment(Segment::text(text.as_ref()));
            }
            // Literal source is the fallback for all remaining nodes. Walking containers
            // preserves Markdown decoration while still activating their real links.
            Event::Start(_) => self.walk(node.span.clone(), &node.children)?,
            _ => self.raw(node.span.clone()),
        }
        Ok(())
    }
    fn standalone(&self, node: &Node<'_>) -> bool {
        let before = &self.input.source[..node.span.start];
        let after = &self.input.source[node.span.end..];
        before.rsplit('\n').next().unwrap().trim().is_empty()
            && after.split('\n').next().unwrap().trim().is_empty()
    }
    fn typeset(&mut self, source: String, original: &str, needs_math: bool) {
        self.account(original);
        let source = if needs_math {
            format!(
                "{}\n{}\n#let forest-equation(value) = eval(\"$\" + value + \"$\", scope: scope)\n{source}",
                include_str!("../assets/mitex/prelude.typ"),
                include_str!("../assets/mitex/standard.typ")
            )
        } else {
            source
        };
        self.current.atoms.push(Atom::Typeset {
            source,
            original: original.into(),
        });
    }
}

fn string(text: &str) -> String {
    serde_json::to_string(text).unwrap()
}

pub fn target(uri: &str, image: bool, group: Option<i64>) -> Result<Segment, Error> {
    let invalid = || Error::Protocol(format!("invalid message target: {uri}"));
    if let Some(hash) = uri.strip_prefix("media:") {
        if !image || hash.len() != 64 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        return Ok(Segment::Image(Image {
            file: uri.into(),
            kind: None,
            options: Default::default(),
        }));
    }
    if image {
        return Err(invalid());
    }
    let paths: Vec<_> = uri
        .strip_prefix("forest:")
        .ok_or_else(invalid)?
        .split('/')
        .collect();
    match paths.as_slice() {
        ["at", account]
            if group.is_some()
                && (*account == "all" || account.parse::<u64>().is_ok_and(|n| n > 0)) =>
        {
            Ok(Segment::At {
                qq: (*account).into(),
            })
        }
        ["face", face] if face.parse::<u32>().is_ok() => Ok(Segment::Face { id: (*face).into() }),
        ["reply", source, message]
            if source.parse::<i64>().ok().filter(|g| *g > 0) == group
                && group.is_some()
                && message.parse::<i32>().is_ok() =>
        {
            Ok(Segment::Reply {
                id: (*message).into(),
            })
        }
        _ => Err(invalid()),
    }
}

fn controls(node: &Node<'_>) -> bool {
    matches!(node.event, Event::Rule)
        || matches!(&node.event, Event::Start(Tag::Link {dest_url, ..} | Tag::Image {dest_url, ..}) if dest_url.starts_with("forest:") || dest_url.starts_with("media:"))
        || node.children.iter().any(controls)
}
fn has_math(node: &Node<'_>) -> bool {
    matches!(node.event, Event::InlineMath(_) | Event::DisplayMath(_))
        || node.children.iter().any(has_math)
}
fn cell(nodes: &[Node<'_>]) -> Result<String, String> {
    let mut out = String::new();
    for node in nodes {
        out.push_str(&match &node.event {
            Event::Text(t) => format!("#text({})", string(t)),
            Event::Code(t) => format!("#raw({})", string(t)),
            Event::InlineMath(m) | Event::DisplayMath(m) => math(m)?,
            Event::SoftBreak | Event::HardBreak => "#linebreak()".into(),
            Event::Start(Tag::Strong) => format!("#strong[{}]", cell(&node.children)?),
            Event::Start(Tag::Emphasis) => format!("#emph[{}]", cell(&node.children)?),
            Event::Start(Tag::Strikethrough) => format!("#strike[{}]", cell(&node.children)?),
            // A PNG cannot carry a clickable URL. Keep this source node as
            // Markdown instead of silently hiding its destination in pixels.
            _ => return Err("unsupported table cell".into()),
        });
    }
    Ok(out)
}
fn table(node: &Node<'_>, alignments: &[Alignment]) -> Result<String, String> {
    let aligns = alignments
        .iter()
        .map(|a| match a {
            Alignment::Center => "center",
            Alignment::Right => "right",
            _ => "left",
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut out = format!(
        "#table(columns: {}, align: ({aligns},), inset: 8pt, stroke: 0.5pt + luma(210), fill: (x,y) => if y == 0 {{luma(245)}} else {{none}},",
        alignments.len()
    );
    for row in &node.children {
        for item in &row.children {
            if !matches!(item.event, Event::Start(Tag::TableCell)) {
                return Err("unsupported table structure".into());
            }
            out.push_str(&format!("[{}],", cell(&item.children)?));
        }
    }
    out.push(')');
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan(s: &str) -> Result<Vec<Draft>, Error> {
        parse(&Format {
            group_id: Some(100),
            source: s.into(),
        })
    }
    #[test]
    fn protocol_is_ast_scoped_and_signed_replies_keep_group() {
        let p = plan(
            "[reply](forest:reply/100/-42) [x](forest:at/7) `[x](forest:at/8)` \\[x](forest:at/9)",
        )
        .unwrap();
        assert!(matches!(&p[0].atoms[0], Atom::Segment(Segment::Reply{id}) if id=="-42"));
        assert_eq!(
            p[0].atoms
                .iter()
                .filter(|a| matches!(a, Atom::Segment(Segment::At { .. })))
                .count(),
            1
        );
        assert!(plan("[reply](forest:reply/200/-42)").is_err());
        assert!(plan("[reply](forest:reply/100/-42)[reply](forest:reply/100/43)").is_err());
        assert!(plan("[x](forest:nope/7)").is_err());
    }
    #[test]
    fn breaks_are_nodes_not_substrings() {
        let p = plan(
            "---\n\nHeading\n---\n\nfirst\n\n---\n\n```txt\n---\n[x](forest:at/8)\n```\n\n---",
        )
        .unwrap();
        assert_eq!(p.len(), 2);
        assert!(
            p[1].atoms
                .iter()
                .any(|a| matches!(a,Atom::Typeset {source,..} if source.contains("forest:at/8")))
        );
        assert!(
            !p[1]
                .atoms
                .iter()
                .any(|a| matches!(a, Atom::Segment(Segment::At { .. })))
        );
    }
    #[test]
    fn render_plans_and_unsupported_source_survive() {
        let p = plan("$$\\frac{a}{b}$$\n\n|a|b|\n|-|-|\n|1|2|\n\n<div>x</div>\n").unwrap();
        assert_eq!(
            p[0].atoms
                .iter()
                .filter(|a| matches!(a, Atom::Typeset { .. }))
                .count(),
            2
        );
        assert!(p[0].atoms.iter().any(
            |a| matches!(a,Atom::Segment(Segment::Text {text}) if text.contains("<div>x</div>"))
        ));
        let p = plan("|a|b|\n|-|-|\n|[x](forest:at/7)|2|\n").unwrap();
        assert!(
            p[0].atoms
                .iter()
                .any(|a| matches!(a, Atom::Segment(Segment::At { .. })))
        );
        assert!(!p[0].atoms.iter().any(|a| matches!(a, Atom::Typeset { .. })));
    }
    #[test]
    fn prose_inlines_stay_text_and_only_display_blocks_are_typeset() {
        let source = "Text **bold** *emphasis* ~~old~~ `length` $x^2$ and $$y^2$$ in prose.\n\n**[name](forest:at/7)**";
        let p = plan(source).unwrap();
        assert!(!p[0].atoms.iter().any(|a| matches!(a, Atom::Typeset { .. })));
        let text = p[0]
            .atoms
            .iter()
            .filter_map(|a| match a {
                Atom::Segment(Segment::Text { text }) => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert!(
            text.starts_with(
                "Text **bold** *emphasis* ~~old~~ `length` $x^2$ and $$y^2$$ in prose."
            )
        );
        assert!(
            p[0].atoms
                .iter()
                .any(|a| matches!(a,Atom::Segment(Segment::At {qq}) if qq=="7"))
        );
        let p = plan("Before\n\n$$x^2$$\n\nAfter\n\n$$\ny^2\n$$").unwrap();
        assert_eq!(
            p[0].atoms
                .iter()
                .filter(|a| matches!(a, Atom::Typeset { .. }))
                .count(),
            2
        );
    }
}
