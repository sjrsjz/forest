use crate::sqlite::Database;
use forest_protocol::service::{SqlStatement, SqlValue};
fn sql(sql: &str, parameters: Vec<SqlValue>) -> SqlStatement {
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
#[tokio::test]
async fn atomic_sql_isolation_and_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("one/data.sqlite3");
    let db = Database::new(path.clone());
    db.execute(vec![sql(
        "CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB)",
        vec![],
    )])
    .await
    .unwrap();
    let write = || {
        sql(
            "INSERT INTO kv VALUES (?,?)",
            vec![SqlValue::Text("a".into()), SqlValue::Blob(vec![0, 1, 255])],
        )
    };
    assert!(db.execute(vec![write(), write()]).await.is_err());
    let read = || sql("SELECT value FROM kv", vec![]);
    assert!(db.execute(vec![read()]).await.unwrap()[0].rows.is_empty());
    db.execute(vec![write()]).await.unwrap();
    for query in [
        "ATTACH DATABASE ':memory:' AS external",
        "COMMIT",
        "PRAGMA writable_schema=ON",
        "VACUUM INTO 'escape.sqlite3'",
        "SELECT 1; SELECT 2",
    ] {
        assert!(
            db.execute(vec![sql(query, vec![])]).await.is_err(),
            "{query}"
        );
    }
    db.close();
    let reopened = Database::new(path);
    assert_eq!(
        reopened.execute(vec![read()]).await.unwrap()[0].rows[0][0],
        SqlValue::Blob(vec![0, 1, 255])
    );
    let other = Database::new(dir.path().join("two/data.sqlite3"));
    assert!(other.execute(vec![read()]).await.is_err());
    reopened.close();
    other.close();
}
