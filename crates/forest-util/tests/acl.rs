use forest_util::{
    acl::{Access, AccessPolicy, AclEntry, AclTable, SubjectPattern},
    token::{Token, TokenPattern},
};

fn path(text: &str) -> Token {
    text.parse().unwrap()
}

#[test]
fn exact_targets_deny_unknown_pairs_even_with_public_subject_rules() {
    let entity = path("/module/example");
    let key = path("/settings/name");
    let subject = path("/user/web/example");
    let acl = AclTable::new([AclEntry {
        entity: entity.clone(),
        key: key.clone(),
        owner: entity.clone(),
        access: AccessPolicy {
            readers: vec![SubjectPattern::Path("/**".parse().unwrap())],
            writers: vec![],
        },
    }])
    .unwrap();
    for (target, item) in [
        (path("/module/missing"), key.clone()),
        (entity.clone(), path("/settings/missing")),
        (entity.clone(), path("/settings")),
        (path("/module/example/child"), key.clone()),
    ] {
        for access in [Access::Read, Access::Write] {
            assert!(!acl.check(&subject, &target, &item, access));
        }
    }
    assert!(acl.check(&subject, &entity, &key, Access::Read));
    assert!(!acl.check(&subject, &entity, &key, Access::Write));
    let empty = AclTable::default();
    assert!(!empty.check(&subject, &entity, &key, Access::Read));
}

#[test]
fn self_module_binds_the_owner_and_write_does_not_imply_read() {
    let owner = path("/module/first-instance");
    let other = path("/module/second-instance");
    let entity = path("/bot/example");
    let key = path("/secret");
    let closed = path("/closed");
    let acl = AclTable::new([
        AclEntry {
            entity: entity.clone(),
            key: key.clone(),
            owner: owner.clone(),
            access: AccessPolicy {
                readers: vec![],
                writers: vec![SubjectPattern::SelfModule],
            },
        },
        AclEntry {
            entity: entity.clone(),
            key: closed.clone(),
            owner: owner.clone(),
            access: AccessPolicy {
                readers: vec![],
                writers: vec![],
            },
        },
    ])
    .unwrap();
    assert!(acl.check(&owner, &entity, &key, Access::Write));
    assert!(!acl.check(&owner, &entity, &key, Access::Read));
    for subject in [&other, &entity, &Token::root(), &path("/system/admin")] {
        assert!(!acl.check(subject, &entity, &key, Access::Write));
    }
    for access in [Access::Read, Access::Write] {
        assert!(!acl.check(&owner, &entity, &closed, access));
    }
}

#[test]
fn subjects_match_whole_paths_and_duplicate_targets_are_errors() {
    let entity = path("/module/example");
    let key = path("/status");
    let entry = AclEntry {
        entity: entity.clone(),
        key: key.clone(),
        owner: entity.clone(),
        access: AccessPolicy {
            readers: vec![SubjectPattern::Path("/user/web/*".parse().unwrap())],
            writers: vec![SubjectPattern::Path("/module/tools/**".parse().unwrap())],
        },
    };
    let acl = AclTable::new([entry.clone()]).unwrap();
    for (subject, access, expected) in [
        ("/user/web/operator", Access::Read, true),
        ("/user/web/operator/child", Access::Read, false),
        ("/user/platform/operator", Access::Read, false),
        ("/module/tools", Access::Write, true),
        ("/module/tools/first/second", Access::Write, true),
        ("/module/tools-extra", Access::Write, false),
        ("/module/example", Access::Write, false),
    ] {
        assert_eq!(acl.check(&path(subject), &entity, &key, access), expected);
    }
    let duplicate = AclTable::new([entry.clone(), entry]).unwrap_err();
    assert_eq!((duplicate.entity, duplicate.key), (entity, key));
}

#[test]
fn typed_segments_need_no_string_conversion_or_default() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    enum Id {
        Module(u64),
        Config(u64),
        User(u64),
    }
    let owner = Token::from_segments([Id::Module(1)]);
    let key = Token::from_segments([Id::Config(2)]);
    let reader = Token::from_segments([Id::User(3)]);
    let acl = AclTable::new([AclEntry {
        entity: owner.clone(),
        key: key.clone(),
        owner: owner.clone(),
        access: AccessPolicy {
            readers: vec![SubjectPattern::Path(TokenPattern::exact(reader.clone()))],
            writers: vec![SubjectPattern::SelfModule],
        },
    }])
    .unwrap();
    assert!(acl.check(&reader, &owner, &key, Access::Read));
    assert!(!acl.check(&reader, &owner, &key, Access::Write));
    assert!(acl.check(&owner, &owner, &key, Access::Write));
}
