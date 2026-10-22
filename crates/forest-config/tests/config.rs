use forest_config::{Change, ConfigError, ConfigStore, Declaration, Key};
use forest_serde::adt::{AsSchema, Value};
use forest_util::token::Token;
use tokio_rusqlite::rusqlite;

fn path(value: &str) -> Token {
    value.parse().unwrap()
}

#[tokio::test]
async fn default_updates_preserve_values_and_commit_as_one_declaration_set() {
    let store = ConfigStore::open_in_memory().await.unwrap();
    let owner = path("/module/defaults");
    let first = Key::<i64>::new(path("/first"));
    let second = Key::<i64>::new(path("/second"));
    store
        .register(
            owner.clone(),
            [first.declare(&1).unwrap(), second.declare(&2).unwrap()],
        )
        .await
        .unwrap();
    store.set(&owner, &first, &42).await.unwrap();
    store
        .update_defaults(
            &owner,
            [first.declare(&10).unwrap(), second.declare(&20).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(store.get(&owner, &first).await.unwrap(), 42);
    assert_eq!(store.get(&owner, &second).await.unwrap(), 2); // Equal to the old default is still a current value.
    assert_eq!(
        store
            .entry(&owner, first.path())
            .await
            .unwrap()
            .default_value(),
        &Value::I64(10)
    );
    assert!(matches!(
        store
            .update_defaults(
                &owner,
                [
                    first.declare(&100).unwrap(),
                    Key::<bool>::new(path("/second")).declare(&true).unwrap(),
                ]
            )
            .await,
        Err(ConfigError::KeyTypeMismatch { .. })
    ));
    assert_eq!(
        store
            .entry(&owner, first.path())
            .await
            .unwrap()
            .default_value(),
        &Value::I64(10)
    );
    store.reset(&owner, second.path()).await.unwrap();
    assert_eq!(store.get(&owner, &second).await.unwrap(), 20);
    assert!(matches!(
        store
            .update_defaults(&owner, [first.declare(&1).unwrap()])
            .await,
        Err(ConfigError::DeclarationMismatch { .. })
    ));
    store.close().await.unwrap();
}

#[tokio::test]
async fn declarations_have_defaults_and_reads_do_not_initialize_entries() {
    let store = ConfigStore::open_in_memory().await.unwrap();
    let owner = path("/module/example");
    let key = Key::<i64>::new(path("/counter"));
    assert!(matches!(
        Declaration::new(path("/bad"), i64::as_schema(), Value::Bool(true)),
        Err(ConfigError::InvalidDefault { .. })
    ));
    store
        .register(owner.clone(), [key.declare(&7).unwrap()])
        .await
        .unwrap();
    store.set(&owner, &key, &19).await.unwrap();
    assert_eq!(
        store
            .entry(&owner, key.path())
            .await
            .unwrap()
            .default_value(),
        &Value::I64(7)
    );
    assert!(matches!(
        store
            .register(owner.clone(), [key.declare(&99).unwrap()])
            .await,
        Err(ConfigError::DuplicateEntity { .. })
    ));
    assert_eq!(store.get(&owner, &key).await.unwrap(), 19);
    store.reset(&owner, key.path()).await.unwrap();
    assert_eq!(store.get(&owner, &key).await.unwrap(), 7);
    let before = serde_json::to_value(store.snapshot().await.unwrap()).unwrap();
    assert!(matches!(
        store.entry(&owner, &path("/missing")).await,
        Err(ConfigError::UnknownKey { .. })
    ));
    assert_eq!(
        serde_json::to_value(store.snapshot().await.unwrap()).unwrap(),
        before
    );
    let optional = Key::<Option<i64>>::new(path("/optional"));
    store
        .register(path("/user/example"), [optional.declare(&None).unwrap()])
        .await
        .unwrap();
    assert_eq!(
        store.get(&path("/user/example"), &optional).await.unwrap(),
        None
    );
    store.remove_entity(&owner).await.unwrap();
    assert!(matches!(
        store.get(&owner, &key).await,
        Err(ConfigError::UnknownEntity { .. })
    ));
}

#[tokio::test]
async fn invalid_declarations_and_batches_never_partially_commit() {
    let store = ConfigStore::open_in_memory().await.unwrap();
    let first = path("/module/first");
    let second = path("/module/second");
    let key = Key::<i64>::new(path("/counter"));
    assert!(matches!(
        store
            .register(
                first.clone(),
                [key.declare(&0).unwrap(), key.declare(&1).unwrap()]
            )
            .await,
        Err(ConfigError::DuplicateKey { .. })
    ));
    assert!(matches!(
        store.get(&first, &key).await,
        Err(ConfigError::UnknownEntity { .. })
    ));
    for entity in [&first, &second] {
        store
            .register(entity.clone(), [key.declare(&0).unwrap()])
            .await
            .unwrap();
    }
    let wrong = Key::<i32>::new(key.path().clone());
    assert!(matches!(
        store.get(&first, &wrong).await,
        Err(ConfigError::KeyTypeMismatch { .. })
    ));
    assert!(matches!(
        store.set(&first, &wrong, &3).await,
        Err(ConfigError::KeyTypeMismatch { .. })
    ));
    assert!(matches!(
        store
            .apply([
                key.set(first.clone(), &10),
                Change::set(second.clone(), key.path().clone(), Value::Bool(true)),
            ])
            .await,
        Err(ConfigError::InvalidValue { .. })
    ));
    assert_eq!(store.get(&first, &key).await.unwrap(), 0);
    assert_eq!(store.get(&second, &key).await.unwrap(), 0);
    let target = second.clone();
    assert!(matches!(
        store
            .update(move |view| {
                view.entry(&target, &path("/missing"))?;
                Ok(((), Vec::new()))
            })
            .await,
        Err(ConfigError::UnknownKey { .. })
    ));
    store
        .apply([
            key.set(first.clone(), &10),
            Change::reset(first.clone(), key.path().clone()),
            key.set(first.clone(), &11),
        ])
        .await
        .unwrap();
    assert_eq!(store.get(&first, &key).await.unwrap(), 11);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_updates_preserve_counts_and_cross_entity_read_consistency() {
    let store = ConfigStore::open_in_memory().await.unwrap();
    let first = path("/module/first");
    let second = path("/module/second");
    let key = Key::<i64>::new(path("/counter"));
    for entity in [&first, &second] {
        store
            .register(entity.clone(), [key.declare(&0).unwrap()])
            .await
            .unwrap();
    }
    let mut workers = Vec::new();
    for _ in 0..4 {
        let (store, first, second, key) =
            (store.clone(), first.clone(), second.clone(), key.clone());
        workers.push(tokio::spawn(async move {
            for _ in 0..50 {
                let (first, second, key) = (first.clone(), second.clone(), key.clone());
                store
                    .update(move |view| {
                        let count = view.get(&first, &key)?;
                        assert_eq!(view.get(&second, &key)?, count);
                        Ok((
                            (),
                            vec![key.set(first, &(count + 1)), key.set(second, &(count + 1))],
                        ))
                    })
                    .await
                    .unwrap();
            }
        }));
    }
    for _ in 0..100 {
        let (first, second, key) = (first.clone(), second.clone(), key.clone());
        store
            .read(move |view| {
                assert_eq!(view.get(&first, &key)?, view.get(&second, &key)?);
                Ok(())
            })
            .await
            .unwrap();
    }
    for worker in workers {
        worker.await.unwrap();
    }
    assert_eq!(store.get(&first, &key).await.unwrap(), 200);
    assert_eq!(store.get(&second, &key).await.unwrap(), 200);
}

#[tokio::test]
async fn disk_is_readable_and_storage_failure_rolls_back_the_whole_batch() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("forest.sqlite3");
    let first = path("/module/first");
    let second = path("/module/second");
    let key = Key::<i64>::new(path("/counter"));
    let store = ConfigStore::open(&file).await.unwrap();
    for entity in [&first, &second] {
        store
            .register(entity.clone(), [key.declare(&7).unwrap()])
            .await
            .unwrap();
    }
    store.register(path("/empty"), []).await.unwrap();
    store.set(&first, &key, &19).await.unwrap();
    // An ordinary SQLite connection can inspect the data while the host is open.
    let inspector = rusqlite::Connection::open(&file).unwrap();
    let row: (String, String, String) = inspector
        .query_row(
            "SELECT entity, path, value_json FROM config_entries WHERE entity = '/module/first'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            first.to_string(),
            key.path().to_string(),
            "{\"I64\":19}".into()
        )
    );
    inspector
        .execute_batch(
            "CREATE TRIGGER reject_second BEFORE UPDATE ON config_entries
         WHEN NEW.entity = '/module/second'
         BEGIN SELECT RAISE(ABORT, 'test storage failure'); END;",
        )
        .unwrap();
    assert!(matches!(
        store
            .apply([key.set(first.clone(), &99), key.set(second.clone(), &99),])
            .await,
        Err(ConfigError::Database(_))
    ));
    assert_eq!(store.get(&first, &key).await.unwrap(), 19);
    assert_eq!(store.get(&second, &key).await.unwrap(), 7);
    inspector
        .execute_batch("DROP TRIGGER reject_second")
        .unwrap();
    drop(inspector);
    store.close().await.unwrap();
    let reopened = ConfigStore::open(&file).await.unwrap();
    assert_eq!(reopened.get(&first, &key).await.unwrap(), 19);
    assert_eq!(
        reopened
            .entry(&first, key.path())
            .await
            .unwrap()
            .default_value(),
        &Value::I64(7)
    );
    let empty = path("/empty");
    assert!(
        reopened
            .read(move |view| view.entries(&empty))
            .await
            .unwrap()
            .is_empty()
    );
    reopened.reset(&first, key.path()).await.unwrap();
    assert_eq!(reopened.get(&first, &key).await.unwrap(), 7);
    reopened.remove_entity(&first).await.unwrap();
    reopened.close().await.unwrap();
    let inspector = rusqlite::Connection::open(&file).unwrap();
    let count: i64 = inspector
        .query_row(
            "SELECT count(*) FROM config_entries WHERE entity = '/module/first'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn imports_preserve_data_and_reject_invalid_or_conflicting_exports_atomically() {
    let store = ConfigStore::open_in_memory().await.unwrap();
    let owner = path("/module/example");
    let key = Key::<i64>::new(path("/counter"));
    store
        .register(owner.clone(), [key.declare(&7).unwrap()])
        .await
        .unwrap();
    store.register(path("/empty"), []).await.unwrap();
    store.set(&owner, &key, &19).await.unwrap();
    let snapshot = store.snapshot().await.unwrap();
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let restored = ConfigStore::open_in_memory().await.unwrap();
    restored
        .import(serde_json::from_slice(&bytes).unwrap())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.snapshot().await.unwrap()).unwrap(),
        bytes
    );
    assert_eq!(restored.get(&owner, &key).await.unwrap(), 19);
    restored.reset(&owner, key.path()).await.unwrap();
    assert_eq!(restored.get(&owner, &key).await.unwrap(), 7);
    let empty = ConfigStore::open_in_memory().await.unwrap();
    let mut invalid = snapshot.clone();
    invalid.entities[1].entries[0].default = Value::Bool(true);
    assert!(matches!(
        empty.import(invalid).await,
        Err(ConfigError::InvalidDefault { .. })
    ));
    let mut invalid = snapshot.clone();
    invalid.entities[1].entries[0].value = Value::Bool(true);
    assert!(matches!(
        empty.import(invalid).await,
        Err(ConfigError::InvalidValue { .. })
    ));
    let mut invalid = snapshot.clone();
    invalid.entities.push(snapshot.entities[0].clone());
    assert!(matches!(
        empty.import(invalid).await,
        Err(ConfigError::DuplicateEntity { .. })
    ));
    empty
        .register(owner.clone(), [key.declare(&0).unwrap()])
        .await
        .unwrap();
    // The empty entity sorts first and is inserted before the existing conflict.
    assert!(matches!(
        empty.import(snapshot.clone()).await,
        Err(ConfigError::DuplicateEntity { .. })
    ));
    assert_eq!(empty.get(&owner, &key).await.unwrap(), 0);
    assert_eq!(
        empty.read(|view| view.entities()).await.unwrap(),
        vec![owner]
    );
    let mut wire = serde_json::to_value(snapshot).unwrap();
    wire["entities"][1]["entries"][0]
        .as_object_mut()
        .unwrap()
        .remove("default");
    assert!(serde_json::from_value::<forest_config::Snapshot>(wire).is_err());
}
