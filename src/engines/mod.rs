use crate::Result;

mod kvs;
mod sled;
pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

/// Defines the storage interface called by KvsServer
pub trait KvsEngine {
    /// Set the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given key
    ///   
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&mut self, key: String) -> Result<()>;
}
