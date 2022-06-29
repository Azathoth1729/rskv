use clap::Parser;
use kvs::args::{Commands, KVSArgs};
use kvs::KvStore;

fn main() {
    let cli = KVSArgs::parse();

    let mut store = KvStore::new();

    match &cli.command {
        Commands::Set { key, value } => store.set(key.clone(), value.clone()),
        Commands::Get { key } => {
            let val = store.get(key.clone()).expect("not found");
            println!("kv[key]={}", val)
        }
        Commands::Rm { key } => store.remove(key.clone()),
    }
}
