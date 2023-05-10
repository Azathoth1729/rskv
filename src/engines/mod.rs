use crate::Result;

mod bitcask;
mod sled;
pub use self::bitcask::Bitcask;
pub use self::sled::SledKvsEngine;

/// Defines the storage interface called by KvsServer
pub trait KvsEngine: Clone + Send + 'static {
    /// Set the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Remove a given key
    ///   
    /// ## Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn rm(&self, key: String) -> Result<()>;
}
