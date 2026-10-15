use crate::actor::{DatabaseCall, DatabaseResult, SqlValue};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{
    Connection, ToSql,
    hooks::{AuthAction, AuthContext, Authorization},
    types::{ToSqlOutput, ValueRef},
};
use serde_json::value::RawValue;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

const TRANSACTIONS: &str = "actor invocations own SQLite transactions and database files";
const RESERVED: &str = "SQLite runtime table names are reserved";
const PRAGMAS: &str = "this SQLite pragma is managed by the actor runtime";
const ALLOWED_PRAGMAS: [&str; 10] = [
    "table_info",
    "table_xinfo",
    "index_info",
    "index_xinfo",
    "index_list",
    "foreign_key_list",
    "foreign_key_check",
    "integrity_check",
    "quick_check",
    "user_version",
];

pub(crate) struct SqliteDatabase {
    engine: Arc<Mutex<Engine>>,
}

#[derive(Debug)]
pub(crate) struct Executed {
    pub result: DatabaseResult,
    pub committed: bool,
}

impl SqliteDatabase {
    pub(crate) async fn open(path: PathBuf) -> Result<Self> {
        let engine = tokio::task::spawn_blocking(move || Engine::open(path)).await??;
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
        })
    }

    pub(crate) async fn call(&self, call: DatabaseCall) -> Result<Executed> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            engine
                .lock()
                .map_err(|_| anyhow!("actor database is poisoned"))?
                .call(call)
        })
        .await?
    }
}

#[derive(Clone, Copy, PartialEq)]
struct ChangeToken(u64, i64, i64);

struct Engine {
    connection: Connection,
    synced: ChangeToken,
}

impl Engine {
    fn open(path: PathBuf) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;
            PRAGMA wal_autocheckpoint = 0; PRAGMA synchronous = FULL",
        )?;
        let check: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        anyhow::ensure!(check == "ok", "invalid actor SQLite database");
        let synced = token(&connection)?;
        Ok(Self { connection, synced })
    }

    fn call(&mut self, call: DatabaseCall) -> Result<Executed> {
        let immediate = call.immediate();
        let before = token(&self.connection)?;
        let result = match call {
            DatabaseCall::Commit => {
                let committed = self.commit()?;
                return Ok(Executed {
                    result: DatabaseResult::Done,
                    committed,
                });
            }
            DatabaseCall::Rollback => self.rollback()?,
            DatabaseCall::Fields => self.fields()?,
            DatabaseCall::Alarm
            | DatabaseCall::SetAlarm { .. }
            | DatabaseCall::DeleteAlarm { .. } => {
                unreachable!("alarm calls are handled by the control plane")
            }
            call => {
                self.begin()?;
                self.write(call)?
            }
        };
        let committed = immediate && token(&self.connection)? != before && self.commit()?;
        Ok(Executed { result, committed })
    }

    fn write(&self, call: DatabaseCall) -> Result<DatabaseResult> {
        match call {
            DatabaseCall::Exec { sql, bindings, .. } => return self.exec(&sql, &bindings),
            DatabaseCall::InitializeFields { fields } => self.initialize(&fields)?,
            DatabaseCall::PersistField { name, value, .. } => self.persist(&name, &value)?,
            DatabaseCall::DeleteField { name, .. } => self.delete(&name)?,
            DatabaseCall::ReplaceFields { fields } => self.replace(&fields)?,
            DatabaseCall::Fields
            | DatabaseCall::Alarm
            | DatabaseCall::SetAlarm { .. }
            | DatabaseCall::DeleteAlarm { .. }
            | DatabaseCall::Commit
            | DatabaseCall::Rollback => {
                unreachable!("read and lifecycle calls are not writes")
            }
        }
        Ok(DatabaseResult::Done)
    }

    fn exec(&self, sql: &str, bindings: &[SqlValue]) -> Result<DatabaseResult> {
        let mut statement = self.prepare_actor_statement(sql)?;
        let columns = statement
            .column_names()
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let mut query = statement.query(rusqlite::params_from_iter(bindings))?;
        let mut rows = Vec::new();
        while let Some(row) = query.next()? {
            rows.push(
                (0..columns.len())
                    .map(|index| row.get_ref(index).map(SqlValue::from))
                    .collect::<rusqlite::Result<_>>()?,
            );
        }
        Ok(DatabaseResult::Rows { columns, rows })
    }

    fn prepare_actor_statement(&self, sql: &str) -> Result<rusqlite::Statement<'_>> {
        let denied = Arc::new(Mutex::new(None));
        self.connection
            .authorizer(Some(authorize(denied.clone())))?;
        let prepared = self.connection.prepare(sql);
        self.connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
        if let Some(reason) = denied
            .lock()
            .map_err(|_| anyhow!("authorizer poisoned"))?
            .take()
        {
            bail!(reason);
        }
        match prepared {
            Err(rusqlite::Error::MultipleStatement) => {
                bail!("actor database exec accepts one SQL statement")
            }
            prepared => Ok(prepared?),
        }
    }

    fn fields(&self) -> Result<DatabaseResult> {
        let mut statement = self
            .connection
            .prepare("SELECT name, value FROM __terse_fields")?;
        let fields = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .map(|row| {
                let (name, value) = row?;
                Ok((name, RawValue::from_string(value)?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(DatabaseResult::Fields { fields })
    }

    fn initialize(&self, fields: &BTreeMap<String, Box<RawValue>>) -> Result<()> {
        let mut statement = self.connection.prepare(
            "INSERT INTO __terse_fields (name, value) VALUES (?, ?) ON CONFLICT(name) DO NOTHING",
        )?;
        for (name, value) in fields {
            statement.execute((name, value.get()))?;
        }
        Ok(())
    }

    fn persist(&self, name: &str, value: &RawValue) -> Result<()> {
        self.connection.execute(
            "INSERT INTO __terse_fields (name, value) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET value = excluded.value WHERE value <> excluded.value",
            (name, value.get()),
        )?;
        Ok(())
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM __terse_fields WHERE name = ?", [name])?;
        Ok(())
    }

    fn replace(&self, fields: &BTreeMap<String, Box<RawValue>>) -> Result<()> {
        for (name, value) in fields {
            self.persist(name, value)?;
        }
        self.connection.execute(
            "DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))",
            [serde_json::to_string(&fields.keys().collect::<Vec<_>>())?],
        )?;
        Ok(())
    }

    fn begin(&self) -> Result<()> {
        if self.connection.is_autocommit() {
            self.connection
                .execute_batch("BEGIN IMMEDIATE")
                .context("failed to begin actor transaction")?;
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<bool> {
        if !self.connection.is_autocommit() {
            self.connection.execute_batch("COMMIT")?;
        }
        let current = token(&self.connection)?;
        let changed = current != self.synced;
        self.synced = current;
        Ok(changed)
    }

    fn rollback(&mut self) -> Result<DatabaseResult> {
        if !self.connection.is_autocommit() {
            self.connection.execute_batch("ROLLBACK")?;
        }
        self.synced = token(&self.connection)?;
        Ok(DatabaseResult::Done)
    }
}

fn token(connection: &Connection) -> Result<ChangeToken> {
    Ok(ChangeToken(
        connection.total_changes(),
        connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?,
        connection.query_row("PRAGMA user_version", [], |row| row.get(0))?,
    ))
}

fn authorize(
    denied: Arc<Mutex<Option<&'static str>>>,
) -> impl for<'r> FnMut(AuthContext<'r>) -> Authorization + Send + 'static {
    move |context| match denial(&context.action) {
        Some(reason) => {
            if let Ok(mut denied) = denied.lock() {
                denied.get_or_insert(reason);
            }
            Authorization::Deny
        }
        None => Authorization::Allow,
    }
}

fn denial(action: &AuthAction<'_>) -> Option<&'static str> {
    match action {
        AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. } => Some(TRANSACTIONS),
        AuthAction::Pragma { pragma_name, .. }
            if !ALLOWED_PRAGMAS.contains(&pragma_name.to_ascii_lowercase().as_str()) =>
        {
            Some(PRAGMAS)
        }
        action if names(action).into_iter().any(reserved) => Some(RESERVED),
        _ => None,
    }
}

fn names<'a>(action: &AuthAction<'a>) -> Vec<&'a str> {
    use AuthAction::*;
    match *action {
        CreateIndex {
            index_name,
            table_name,
        }
        | CreateTempIndex {
            index_name,
            table_name,
        }
        | DropIndex {
            index_name,
            table_name,
        }
        | DropTempIndex {
            index_name,
            table_name,
        } => vec![index_name, table_name],
        CreateTrigger {
            trigger_name,
            table_name,
        }
        | CreateTempTrigger {
            trigger_name,
            table_name,
        }
        | DropTrigger {
            trigger_name,
            table_name,
        }
        | DropTempTrigger {
            trigger_name,
            table_name,
        } => vec![trigger_name, table_name],
        CreateView { view_name }
        | CreateTempView { view_name }
        | DropView { view_name }
        | DropTempView { view_name } => vec![view_name],
        CreateTable { table_name }
        | CreateTempTable { table_name }
        | DropTable { table_name }
        | DropTempTable { table_name }
        | Delete { table_name }
        | Insert { table_name }
        | Analyze { table_name }
        | Read { table_name, .. }
        | Update { table_name, .. }
        | AlterTable { table_name, .. }
        | CreateVtable { table_name, .. }
        | DropVtable { table_name, .. } => vec![table_name],
        _ => vec![],
    }
}

fn reserved(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("__terse_") || name.starts_with("_litestream_")
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value.as_bytes()),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

impl From<ValueRef<'_>> for SqlValue {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value),
            ValueRef::Text(value) => Self::Text(String::from_utf8_lossy(value).into_owned()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/host/sqlite.rs"]
mod tests;
