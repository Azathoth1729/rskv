#![deny(missing_docs)]
//! A simple key/value store.

mod kv;
use std::path::PathBuf;

pub use kv::KvStore;

mod error;
pub use error::{KvsError, Result};

pub mod args;

/// default kvs data directory
pub fn get_kvs_data_dir() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    dir.push("data/kvs");
    dir
}
