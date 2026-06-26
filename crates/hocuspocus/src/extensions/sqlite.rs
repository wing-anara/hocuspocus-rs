//! SQLite persistence, mirroring `@hocuspocus/extension-sqlite`.
//!
//! Uses the same single-table schema (`documents(name UNIQUE, data BLOB)`) and
//! upsert semantics, so it is wire-compatible with databases written by the
//! TypeScript extension. Blocking `rusqlite` calls run on the blocking pool.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::extensions::database::{Database, Storage};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS \"documents\" (\
    \"name\" varchar(255) NOT NULL,\
    \"data\" blob NOT NULL,\
    UNIQUE(name))";

const SELECT: &str = "SELECT data FROM \"documents\" WHERE name = ?1 ORDER BY rowid DESC LIMIT 1";
const UPSERT: &str = "INSERT INTO \"documents\" (\"name\", \"data\") VALUES (?1, ?2) \
    ON CONFLICT(name) DO UPDATE SET data = ?2";

/// A SQLite-backed [`Storage`].
pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at `path`. Use `":memory:"` for an
    /// ephemeral database.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn in_memory() -> anyhow::Result<Self> {
        Self::open(":memory:")
    }

    /// Convenience: build a ready-to-register [`Database`] extension.
    pub fn extension(self) -> Database<SqliteStorage> {
        Database::new(self)
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn fetch(&self, document_name: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let conn = self.conn.clone();
        let name = document_name.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            let mut stmt = conn.prepare_cached(SELECT)?;
            let result = stmt
                .query_row([&name], |row| row.get::<_, Vec<u8>>(0))
                .ok();
            Ok::<_, anyhow::Error>(result)
        })
        .await?
    }

    async fn store(&self, document_name: &str, state: &[u8]) -> anyhow::Result<()> {
        let conn = self.conn.clone();
        let name = document_name.to_string();
        let data = state.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock();
            conn.execute(UPSERT, rusqlite::params![name, data])?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
}
