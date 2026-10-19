use std::{error::Error, fmt, str::FromStr};

use super::{PatternSegment, Token, TokenPattern};

/// A validated literal segment for the text path format.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Segment(Box<str>);

impl Segment {
    pub fn new(value: impl Into<Box<str>>) -> Result<Self, SegmentError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SegmentError::Empty);
        }
        if value.as_ref() == "." || value.as_ref() == ".." {
            return Err(SegmentError::Relative);
        }
        if let Some(character) = value.chars().find(|character| {
            matches!(character, '/' | '\\' | '*')
                || character.is_whitespace()
                || character.is_control()
        }) {
            return Err(SegmentError::InvalidCharacter(character));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SegmentError {
    Empty,
    Relative,
    InvalidCharacter(char),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("empty path segment"),
            Self::Relative => formatter.write_str("relative path segment is not allowed"),
            Self::InvalidCharacter(character) => {
                write!(formatter, "invalid path character {character:?}")
            }
        }
    }
}

impl Error for SegmentError {}

/// Text input errors. Segment indexes are zero-based after the leading slash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathParseError {
    MissingRoot,
    InvalidSegment { index: usize, reason: SegmentError },
}

impl fmt::Display for PathParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRoot => formatter.write_str("path must start with /"),
            Self::InvalidSegment { index, reason } => {
                write!(formatter, "segment {index}: {reason}")
            }
        }
    }
}

impl Error for PathParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MissingRoot => None,
            Self::InvalidSegment { reason, .. } => Some(reason),
        }
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Segment {
    type Err = SegmentError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

// Validates the common path envelope; literal validation stays at Segment's boundary.
fn parse_segments<T>(
    value: &str,
    parse: impl Fn(&str) -> Result<T, SegmentError>,
) -> Result<Vec<T>, PathParseError> {
    let suffix = value.strip_prefix('/').ok_or(PathParseError::MissingRoot)?;
    if suffix.is_empty() {
        return Ok(Vec::new());
    }
    suffix
        .split('/')
        .enumerate()
        .map(|(index, segment)| {
            parse(segment).map_err(|reason| PathParseError::InvalidSegment { index, reason })
        })
        .collect()
}

impl FromStr for Token {
    type Err = PathParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_segments(parse_segments(value, |segment| {
            Segment::new(segment)
        })?))
    }
}

impl FromStr for TokenPattern {
    type Err = PathParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_segments(parse_segments(
            value,
            |segment| match segment {
                "*" => Ok(PatternSegment::Any),
                "**" => Ok(PatternSegment::AnyDepth),
                _ => Segment::new(segment).map(PatternSegment::Literal),
            },
        )?))
    }
}

fn format_segments<T: fmt::Display>(
    formatter: &mut fmt::Formatter<'_>,
    segments: &[T],
) -> fmt::Result {
    if segments.is_empty() {
        return formatter.write_str("/");
    }
    for segment in segments {
        write!(formatter, "/{segment}")?;
    }
    Ok(())
}

impl fmt::Display for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_segments(formatter, self.segments())
    }
}

impl fmt::Display for PatternSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(segment) => segment.fmt(formatter),
            Self::Any => formatter.write_str("*"),
            Self::AnyDepth => formatter.write_str("**"),
        }
    }
}

impl fmt::Display for TokenPattern {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_segments(formatter, self.segments())
    }
}
