//! Commands and Subcommands for kvs

use clap::{Parser, Subcommand};

/// Args for kvs
#[derive(Parser)]
#[clap(author, version, about)]
#[clap(propagate_version = true)]
pub struct KVSArgs {
    /// Subcommand
    #[clap(subcommand)]
    pub command: Commands,
}

/// Enum type of subcommand for kvs
#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Set the value of a string key to a string
    Set {
        /// Key
        key: String,
        /// Value
        value: String,
    },
    /// Get the string value of a given string key
    Get {
        /// Key
        key: String,
    },
    /// Remove a given key
    Rm {
        /// Key
        key: String,
    },
}
