use std::{net::SocketAddr, process::exit};

use clap::{Parser, Subcommand};
use log::{error, LevelFilter};

use rskv::{KvsClient, Result};

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

/// Args for kvs-client
#[derive(Parser)]
#[clap(author, version, about)]
#[clap(propagate_version = true)]
struct ClientArgs {
    /// Subcommand
    #[clap(subcommand)]
    command: Commands,
}

/// Enum type of subcommand for kvs
#[derive(Debug, Subcommand)]
enum Commands {
    /// Get the string value of a given string key
    Get {
        /// Key
        key: String,
        /// Server listening address, default is 127.0.0.1:4000
        #[clap(short, long, value_parser)]
        addr: Option<SocketAddr>,
    },
    /// Set the value of a string key to a string
    Set {
        /// Key
        key: String,
        /// Value
        value: String,
        /// Server listening address, default is 127.0.0.1:4000
        #[clap(short, long, value_parser)]
        addr: Option<SocketAddr>,
    },
    /// Remove a given key
    Rm {
        /// Key
        key: String,
        /// Server listening address, default is 127.0.0.1:4000
        #[clap(short, long, value_parser)]
        addr: Option<SocketAddr>,
    },
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    if let Err(e) = run() {
        error!("{}", e);
        exit(1);
    }
}

fn run() -> Result<()> {
    let cli = ClientArgs::parse();

    match cli.command {
        Commands::Get { key, addr } => {
            let addr = addr.unwrap_or(DEFAULT_ADDR.parse().unwrap());

            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }

        Commands::Set { key, value, addr } => {
            let addr = addr.unwrap_or(DEFAULT_ADDR.parse().unwrap());
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }

        Commands::Rm { key, addr } => {
            let addr = addr.unwrap_or(DEFAULT_ADDR.parse().unwrap());
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}
