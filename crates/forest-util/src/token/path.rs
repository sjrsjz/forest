use super::Segment;

/// A concrete source path. Constructing one does not grant its identity or rights.
///
/// Custom segment types can encode identities without string interpretation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token<K = Segment> {
    segments: Vec<K>,
}

impl<K> Token<K> {
    /// The empty path. It has no implicit authority.
    pub const fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn from_segments(segments: impl IntoIterator<Item = K>) -> Self {
        Self {
            segments: segments.into_iter().collect(),
        }
    }

    pub fn segments(&self) -> &[K] {
        &self.segments
    }

    pub fn into_segments(self) -> Vec<K> {
        self.segments
    }
}

impl<K: Clone> Token<K> {
    /// Constructs a new value without modifying this path or granting rights.
    pub fn child(&self, segment: K) -> Self {
        let mut segments = self.segments.clone();
        segments.push(segment);
        Self { segments }
    }

    /// Returns `None` for the root.
    pub fn parent(&self) -> Option<Self> {
        let (_, prefix) = self.segments.split_last()?;
        Some(Self::from_segments(prefix.iter().cloned()))
    }
}

impl<K: PartialEq> Token<K> {
    /// Includes equality; compares whole segments, not string prefixes.
    pub fn is_prefix_of(&self, other: &Self) -> bool {
        other.segments.starts_with(&self.segments)
    }

    /// Strict ancestry: a path is not its own ancestor.
    pub fn is_ancestor_of(&self, other: &Self) -> bool {
        self.segments.len() < other.segments.len() && self.is_prefix_of(other)
    }
}

impl<K> Default for Token<K> {
    fn default() -> Self {
        Self::root()
    }
}
