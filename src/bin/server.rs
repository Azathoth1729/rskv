use std::{env::current_dir, fs, net::SocketAddr, process::exit};

use clap::{arg_enum, Parser};
use log::{error, info, warn, LevelFilter};

use rskv::{
    get_kvstore_data_dir, get_sled_data_dir,
    thread_pool::{RayonThreadPool, ThreadPool},
    Bitcask, KvsEngine, KvsServer, Result, SledKvsEngine,
};

/// Args for kvs-server
#[derive(Parser)]
#[clap(author, version, about)]
#[clap(propagate_version = true)]
struct ServerArgs {
    /// Server listening address, default is 127.0.0.1:4000
    #[clap(long, value_parser)]
    addr: Option<SocketAddr>,
    /// Engine type, default is kvs
    #[clap(long, arg_enum, value_parser)]
    engine: Option<Engine>,
}

arg_enum! {
  /// Engine enum type
  #[derive(Debug, Clone, PartialEq)]
  enum Engine {
      Kvs,
      Sled,
  }
}

const DEFAULT_ENGINE: Engine = Engine::Kvs;
const DEFAULT_ADDR: &str = "127.0.0.1:4000";

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let cli = ServerArgs::parse();

    let engine = cli.engine.unwrap_or(DEFAULT_ENGINE);
    let addr = cli.addr.unwrap_or(DEFAULT_ADDR.parse().unwrap());

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {:?}", engine);
    info!("Listening on {:?}", addr);

    let res = current_engine().and_then(|cur_engine| {
        if let Some(cur_engine) = cur_engine {
            if engine != cur_engine {
                error!("Wrong engine! Please modify the engine file in the root of project");
                exit(1);
            }
        }
        boot_engine(engine, addr)
    });

    if let Err(e) = res {
        error!("{}", e);
        exit(1);
    }
}

fn boot_engine(engine: Engine, addr: SocketAddr) -> Result<()> {
    // write engine to engine file
    fs::write(current_dir()?.join("engine"), format!("{:?}", engine))?;

    let pool = RayonThreadPool::new(num_cpus::get())?;
    match engine {
        Engine::Kvs => run_with_engine(Bitcask::open(get_kvstore_data_dir())?, pool, addr),
        Engine::Sled => run_with_engine(
            SledKvsEngine::new(sled::open(get_sled_data_dir())?),
            pool,
            addr,
        ),
    }
}

fn run_with_engine<E: KvsEngine, P: ThreadPool>(
    engine: E,
    pool: P,
    addr: SocketAddr,
) -> Result<()> {
    let server = KvsServer::new(engine, pool);
    server.run(addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {}", e);
            Ok(None)
        }
    }
}
