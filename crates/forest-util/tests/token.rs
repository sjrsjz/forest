use forest_util::token::{PathParseError, PatternSegment, SegmentError, Token, TokenPattern};

#[test]
fn wildcard_matching_uses_whole_segments_and_whole_paths() {
    for (pattern, path, expected) in [
        ("/", "/", true),
        ("/", "/module", false),
        ("/module/a", "/module/a", true),
        ("/module/a", "/module/ab", false),
        ("/module/a", "/module/a/x", false),
        ("/module/*", "/module/a", true),
        ("/module/*", "/module", false),
        ("/module/*", "/module/a/x", false),
        ("/*", "/", false),
        ("/**", "/", true),
        ("/module/**", "/module", true),
        ("/module/**", "/module/a/x", true),
        ("/module/**", "/modules/a", false),
        ("/a/**/b", "/a/b", true),
        ("/a/**/b", "/a/x/b/x/b", true),
        ("/a/**/b", "/a/x/b/x", false),
        ("/**/a/**/b", "/x/a/y/a/z/b", true),
        ("/**/a/**/b", "/x/a/y/a/z", false),
        ("/**/**", "/", true),
        ("/**/**", "/a/b", true),
        ("/bot/*/module/**", "/bot/a/module", true),
        ("/module/A", "/module/a", false),
    ] {
        let pattern: TokenPattern = pattern.parse().unwrap();
        let path: Token = path.parse().unwrap();
        assert_eq!(pattern.matches(&path), expected, "{pattern} against {path}");
    }
}

#[test]
fn typed_segments_need_no_text_conversion_or_default_implementation() {
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Identity {
        Module,
        Instance(u64),
    }

    let root = Token::<Identity>::default();
    let modules = root.child(Identity::Module);
    let instance = modules.child(Identity::Instance(7));
    assert_eq!(root.parent(), None);
    assert_eq!(instance.parent(), Some(modules.clone()));
    assert!(modules.is_ancestor_of(&instance));
    assert!(!modules.is_ancestor_of(&modules));
    assert!(modules.is_prefix_of(&modules));
    assert!(!instance.is_prefix_of(&modules));
    assert!(TokenPattern::subtree(modules.clone()).matches(&instance));
    assert!(!TokenPattern::exact(modules.clone()).matches(&instance));
    assert!(TokenPattern::exact(modules).matches(&instance.parent().unwrap()));
}

#[test]
fn ambiguous_or_relative_text_is_rejected_without_normalization() {
    for path in ["", "module/a", " /module/a"] {
        assert_eq!(path.parse::<Token>(), Err(PathParseError::MissingRoot));
    }
    for path in [
        "//a",
        "/a/",
        "/a//b",
        "/a/.",
        "/a/..",
        "/a\\b",
        "/a b",
        "/a\n",
        "/a\0",
        "/a\u{2003}b",
    ] {
        assert!(path.parse::<Token>().is_err(), "{path:?}");
        assert!(path.parse::<TokenPattern>().is_err(), "{path:?}");
    }
    assert_eq!(
        "/module/*".parse::<Token>(),
        Err(PathParseError::InvalidSegment {
            index: 1,
            reason: SegmentError::InvalidCharacter('*'),
        })
    );
    for pattern in ["/a*", "/***", "/a/**b"] {
        assert!(pattern.parse::<TokenPattern>().is_err());
    }
}

#[test]
fn text_round_trips_without_decoding_or_case_folding() {
    for text in ["/", "/module/a", "/user/本地/甲", "/literal/%2F/?", "/A"] {
        let token: Token = text.parse().unwrap();
        assert_eq!(token.to_string(), text);
        assert_eq!(token.to_string().parse::<Token>().unwrap(), token);
    }
    for text in ["/", "/**", "/a/*/**/b", "/**/**", "/user/本地/*"] {
        let pattern: TokenPattern = text.parse().unwrap();
        assert_eq!(pattern.to_string(), text);
        assert_eq!(
            pattern.to_string().parse::<TokenPattern>().unwrap(),
            pattern
        );
    }
    let encoded: Token = "/literal/%2F".parse().unwrap();
    assert_eq!(encoded.segments().len(), 2);
    let parent: Token = "/module/a".parse().unwrap();
    assert!(!parent.is_prefix_of(&"/module/ab".parse().unwrap()));
}

#[test]
fn wildcard_backtracking_agrees_with_a_small_exhaustive_reference() {
    // Enumerate short paths/patterns, including multiple ** and literal suffixes.
    // The independent reference explores both meanings of ** recursively.
    fn reference(pattern: &[PatternSegment<u8>], path: &[u8]) -> bool {
        match pattern.split_first() {
            None => path.is_empty(),
            Some((PatternSegment::AnyDepth, rest)) => {
                reference(rest, path) || (!path.is_empty() && reference(pattern, &path[1..]))
            }
            Some((segment, rest)) => path.split_first().is_some_and(|(head, tail)| {
                (matches!(segment, PatternSegment::Any)
                    || matches!(segment, PatternSegment::Literal(value) if value == head))
                    && reference(rest, tail)
            }),
        }
    }

    fn words<T: Clone>(alphabet: &[T], depth: usize) -> Vec<Vec<T>> {
        let mut result = vec![vec![]];
        let mut level = vec![vec![]];
        for _ in 0..depth {
            let mut next = Vec::new();
            for prefix in &level {
                for item in alphabet {
                    let mut word = prefix.clone();
                    word.push(item.clone());
                    next.push(word);
                }
            }
            result.extend(next.iter().cloned());
            level = next;
        }
        result
    }

    let paths = words(&[0, 1], 4);
    let patterns = words(
        &[
            PatternSegment::Literal(0),
            PatternSegment::Literal(1),
            PatternSegment::Any,
            PatternSegment::AnyDepth,
        ],
        4,
    );
    for segments in patterns {
        let pattern = TokenPattern::from_segments(segments.iter().cloned());
        for path in &paths {
            let token = Token::from_segments(path.iter().copied());
            assert_eq!(
                pattern.matches(&token),
                reference(&segments, path),
                "{pattern:?} against {token:?}"
            );
        }
    }
}
