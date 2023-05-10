use sled::Db;

use crate::{KvsEngine, KvsError};

/// Kvs engine implementation by seld database
#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl SledKvsEngine {
    /// Creates a `SledKvsEngine` from `sled::Db`.
    pub fn new(db: Db) -> SledKvsEngine {
        SledKvsEngine(db)
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.0.insert(&key, value.as_bytes())?;
        // self.0.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        Ok(self
            .0
            .get(&key)?
            .map(|v| String::from_utf8(v.as_ref().to_vec()))
            .transpose()?)
    }

    fn rm(&self, key: String) -> crate::Result<()> {
        self.0.remove(&key)?.ok_or(KvsError::KeyNotFound)?;
        self.0.flush()?;
        Ok(())
    }
}
