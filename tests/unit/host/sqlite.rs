use super::*;
use serde_json::json;

async fn database() -> Result<(tempfile::TempDir, SqliteDatabase)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    rusqlite::Connection::open(&path)?.execute_batch(
        "PRAGMA journal_mode=WAL;
        CREATE TABLE __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))",
    )?;
    let database = SqliteDatabase::open(path).await?;
    Ok((directory, database))
}

async fn exec(
    database: &SqliteDatabase,
    sql: &str,
    bindings: Vec<SqlValue>,
    immediate: bool,
) -> Result<Executed> {
    database
        .call(DatabaseCall::Exec {
            sql: sql.into(),
            bindings,
            immediate,
        })
        .await
}

fn raw(value: serde_json::Value) -> Box<serde_json::value::RawValue> {
    serde_json::value::RawValue::from_string(value.to_string()).unwrap()
}

async fn fields(database: &SqliteDatabase) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(database.call(DatabaseCall::Fields).await?.result)?["fields"].clone())
}

#[tokio::test]
async fn values_keep_their_sqlite_storage_classes() -> Result<()> {
    let (_directory, database) = database().await?;
    exec(&database, "CREATE TABLE items (value)", vec![], false).await?;
    let values = vec![
        SqlValue::Null,
        SqlValue::Integer(i64::MAX),
        SqlValue::Real(1.5),
        SqlValue::Text("snowman ☃".into()),
        SqlValue::Blob(vec![0, 1, 255]),
    ];
    for value in &values {
        exec(
            &database,
            "INSERT INTO items VALUES (?)",
            vec![value.clone()],
            false,
        )
        .await?;
    }
    let selected = exec(
        &database,
        "SELECT value, typeof(value) AS kind FROM items ORDER BY rowid",
        vec![],
        false,
    )
    .await?;
    let DatabaseResult::Rows { columns, rows } = selected.result else {
        anyhow::bail!("expected rows");
    };
    assert_eq!(columns, ["value", "kind"]);
    assert_eq!(
        rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
        values
    );
    assert_eq!(
        rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
        ["null", "integer", "real", "text", "blob"].map(|kind| SqlValue::Text(kind.into()))
    );
    assert_eq!(
        serde_json::to_value(&values)?,
        json!([
            {"type": "null"},
            {"type": "integer", "value": "9223372036854775807"},
            {"type": "real", "value": 1.5},
            {"type": "text", "value": "snowman ☃"},
            {"type": "blob", "value": "AAH/"},
        ])
    );
    Ok(())
}

#[tokio::test]
async fn runtime_owned_statements_are_rejected() -> Result<()> {
    let (_directory, database) = database().await?;
    for (sql, message) in [
        ("BEGIN IMMEDIATE", "own SQLite transactions"),
        ("COMMIT", "own SQLite transactions"),
        ("SAVEPOINT nested", "own SQLite transactions"),
        ("RELEASE nested", "own SQLite transactions"),
        ("ROLLBACK", "own SQLite transactions"),
        ("ATTACH 'other.sqlite' AS other", "own SQLite transactions"),
        (
            "CREATE TABLE __terse_shadow (value TEXT)",
            "names are reserved",
        ),
        ("SELECT * FROM __terse_fields", "names are reserved"),
        (
            "PRAGMA journal_mode = DELETE",
            "managed by the actor runtime",
        ),
        ("SELECT 1; SELECT 2", "one SQL statement"),
    ] {
        let error = exec(&database, sql, vec![], false).await.unwrap_err();
        assert!(format!("{error:#}").contains(message), "{sql}: {error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn single_statements_may_end_with_semicolons_comments_and_trigger_bodies() -> Result<()> {
    let (_directory, database) = database().await?;
    for sql in [
        "CREATE TABLE events (name TEXT);",
        "CREATE TABLE audit (name TEXT); -- created",
        "CREATE TRIGGER copy AFTER INSERT ON events BEGIN INSERT INTO audit VALUES (new.name); INSERT INTO audit VALUES ('after'); END",
        "PRAGMA user_version = 3",
    ] {
        exec(&database, sql, vec![], false).await?;
    }
    exec(
        &database,
        "INSERT INTO events VALUES (?)",
        vec![SqlValue::Text("created".into())],
        false,
    )
    .await?;
    let audit = exec(
        &database,
        "SELECT name FROM audit ORDER BY rowid",
        vec![],
        false,
    )
    .await?;
    assert_eq!(
        serde_json::to_value(audit.result)?["rows"],
        json!([[{"type": "text", "value": "created"}], [{"type": "text", "value": "after"}]])
    );
    let version = exec(&database, "PRAGMA user_version", vec![], false).await?;
    assert_eq!(
        serde_json::to_value(version.result)?["rows"],
        json!([[{"type": "integer", "value": "3"}]])
    );
    Ok(())
}

#[tokio::test]
async fn immediate_writes_commit_once_and_atomic_writes_wait_for_commit() -> Result<()> {
    let (_directory, database) = database().await?;
    assert!(
        exec(&database, "CREATE TABLE notes (body TEXT)", vec![], true)
            .await?
            .committed
    );
    assert!(
        !exec(&database, "SELECT * FROM notes", vec![], true)
            .await?
            .committed
    );
    assert!(
        !exec(
            &database,
            "INSERT INTO notes VALUES ('draft')",
            vec![],
            false
        )
        .await?
        .committed
    );
    assert!(database.call(DatabaseCall::Commit).await?.committed);
    assert!(!database.call(DatabaseCall::Commit).await?.committed);
    Ok(())
}

#[tokio::test]
async fn rollback_discards_uncommitted_sql_and_fields() -> Result<()> {
    let (_directory, database) = database().await?;
    database
        .call(DatabaseCall::InitializeFields {
            fields: [("count".to_string(), raw(json!(1)))].into(),
        })
        .await?;
    exec(&database, "CREATE TABLE notes (body TEXT)", vec![], false).await?;
    assert!(database.call(DatabaseCall::Commit).await?.committed);
    database
        .call(DatabaseCall::PersistField {
            name: "count".into(),
            value: raw(json!(2)),
            immediate: false,
        })
        .await?;
    exec(
        &database,
        "INSERT INTO notes VALUES ('lost')",
        vec![],
        false,
    )
    .await?;
    database.call(DatabaseCall::Rollback).await?;
    assert_eq!(fields(&database).await?, json!({"count": 1}));
    let notes = exec(
        &database,
        "SELECT count(*) AS total FROM notes",
        vec![],
        false,
    )
    .await?;
    assert_eq!(
        serde_json::to_value(notes.result)?["rows"],
        json!([[{"type": "integer", "value": "0"}]])
    );
    assert!(!database.call(DatabaseCall::Commit).await?.committed);
    Ok(())
}

#[tokio::test]
async fn fields_preserve_json_text_and_replacement_removes_missing_names() -> Result<()> {
    let (_directory, database) = database().await?;
    database
        .call(DatabaseCall::InitializeFields {
            fields: [
                (
                    "nested".to_string(),
                    serde_json::value::RawValue::from_string(r#"{"z":1,"a":[true,null]}"#.into())?,
                ),
                ("stale".to_string(), raw(json!("old"))),
            ]
            .into(),
        })
        .await?;
    database
        .call(DatabaseCall::InitializeFields {
            fields: [("stale".to_string(), raw(json!("ignored")))].into(),
        })
        .await?;
    let listed = serde_json::to_string(&database.call(DatabaseCall::Fields).await?.result)?;
    assert!(
        listed.contains(r#""nested":{"z":1,"a":[true,null]}"#),
        "{listed}"
    );
    assert!(listed.contains(r#""stale":"old""#), "{listed}");
    database
        .call(DatabaseCall::ReplaceFields {
            fields: [("count".to_string(), raw(json!(3)))].into(),
        })
        .await?;
    database
        .call(DatabaseCall::DeleteField {
            name: "missing".into(),
            immediate: false,
        })
        .await?;
    assert_eq!(fields(&database).await?, json!({"count": 3}));
    Ok(())
}
