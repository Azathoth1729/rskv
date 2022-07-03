use std::process::exit;

use clap::Parser;

use kvs::args::{Commands, KVSArgs};
use kvs::KvStore;
use kvs::{get_kvs_data_dir, KvsError, Result};

fn main() -> Result<()> {
    let cli = KVSArgs::parse();

    let mut store = KvStore::open(get_kvs_data_dir())?;

    match &cli.command {
        Commands::Set { key, value } => store.set(key.clone(), value.clone())?,
        Commands::Get { key } => {
            if let Some(val) = store.get(key.clone())? {
                println!("{}", val)
            } else {
                println!("Key not found")
            }
        }
        Commands::Rm { key } => match store.remove(key.clone()) {
            Ok(_) => {}
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1)
            }
            Err(e) => return Err(e),
        },
    }

    Ok(())
}
