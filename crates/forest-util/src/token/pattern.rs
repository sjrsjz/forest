use super::{Segment, Token};

/// A literal segment, `*` (one segment), or `**` (zero or more segments).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PatternSegment<K = Segment> {
    Literal(K),
    Any,
    AnyDepth,
}

/// Matches complete paths. Prefix matching is always explicit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TokenPattern<K = Segment> {
    segments: Vec<PatternSegment<K>>,
}

impl<K> TokenPattern<K> {
    pub fn from_segments(segments: impl IntoIterator<Item = PatternSegment<K>>) -> Self {
        Self {
            segments: segments.into_iter().collect(),
        }
    }

    pub fn segments(&self) -> &[PatternSegment<K>] {
        &self.segments
    }

    pub fn exact(token: Token<K>) -> Self {
        Self::from_segments(
            token
                .into_segments()
                .into_iter()
                .map(PatternSegment::Literal),
        )
    }

    /// Matches the supplied path itself and every descendant (`path/**`).
    pub fn subtree(token: Token<K>) -> Self {
        let mut pattern = Self::exact(token);
        pattern.segments.push(PatternSegment::AnyDepth);
        pattern
    }
}

impl<K: PartialEq> TokenPattern<K> {
    /// Iterative matching with O(1) auxiliary space and O((P + 1) * (T + 1))
    /// worst-case comparisons. No recursive wildcard expansion is performed.
    pub fn matches(&self, token: &Token<K>) -> bool {
        let path = token.segments();
        let (mut pattern_index, mut path_index) = (0, 0);
        let mut last_wildcard = None;

        while path_index < path.len() {
            match self.segments.get(pattern_index) {
                Some(PatternSegment::AnyDepth) => {
                    last_wildcard = Some((pattern_index, path_index));
                    pattern_index += 1;
                    continue;
                }
                Some(PatternSegment::Any) => {
                    pattern_index += 1;
                    path_index += 1;
                    continue;
                }
                Some(PatternSegment::Literal(segment)) if segment == &path[path_index] => {
                    pattern_index += 1;
                    path_index += 1;
                    continue;
                }
                _ => {}
            }

            // A suffix failed: let the most recent ** consume one more segment.
            let Some((wildcard_index, consumed_until)) = last_wildcard else {
                return false;
            };
            path_index = consumed_until + 1;
            pattern_index = wildcard_index + 1;
            last_wildcard = Some((wildcard_index, path_index));
        }

        self.segments[pattern_index..]
            .iter()
            .all(|segment| matches!(segment, PatternSegment::AnyDepth))
    }
}
