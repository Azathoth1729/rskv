use thiserror::Error;

/// Error type for kvs.
#[derive(Error, Debug)]
pub enum KvsError {
    /// IO error
    #[error("{0}")]
    Io(#[from] std::io::Error),
    ///  Serialization or deserialization error.
    #[error("{0}")]
    Serde(#[from] serde_json::Error),
    /// Removing non-existent key error.
    #[error("Key don't exist")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[error("Unexpected command type")]
    Unknown,
}

/// Custom result type for KvsError
pub type Result<T> = std::result::Result<T, KvsError>;
