#![deny(missing_docs)]
//! A simple key/value store.

mod client;
mod engines;
mod error;
mod resp;
mod server;
pub mod thread_pool;

pub use client::KvsClient;
pub use engines::{Bitcask, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;


use std::path::PathBuf;

/// default kvstore data directory
pub fn get_kvstore_data_dir() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    dir.push("data/kvs");
    dir
}

/// default sled engine data directory
pub fn get_sled_data_dir() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    dir.push("data/sled");
    dir
}
