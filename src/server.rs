use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use log::{debug, error};
use serde_json::Deserializer;

use crate::{
    resp::{GetResponse, RemoveResponse, Request, SetResponse},
    thread_pool::ThreadPool,
    KvsEngine, Result,
};

/// The server of a key value store.
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }

    /// Running KvsServer on a certain ip address
    pub fn run<A: ToSocketAddrs>(self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle_stream(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed:: {}", e),
            })
        }
        Ok(())
    }
}

fn handle_stream<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let req_deserialzer = Deserializer::from_reader(reader).into_iter::<Request>();

    macro_rules! send_resp {
        ($resp:expr) => {{
            let resp = $resp;
            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
            debug!("Response sent to {}: {:?}", peer_addr, resp);
        }};
    }

    for req in req_deserialzer {
        let req = req?;
        debug!("Receive request from {}: {:?}", peer_addr, req);
        match req {
            Request::Get { key } => send_resp!(match engine.get(key) {
                Ok(val) => GetResponse::Ok(val),
                Err(e) => GetResponse::Err(e.to_string()),
            }),
            Request::Set { key, value } => send_resp!(match engine.set(key, value) {
                Ok(()) => SetResponse::Ok(()),
                Err(e) => SetResponse::Err(e.to_string()),
            }),
            Request::Rm { key } => send_resp!(match engine.rm(key) {
                Ok(()) => RemoveResponse::Ok(()),
                Err(e) => RemoveResponse::Err(e.to_string()),
            }),
        }
    }
    Ok(())
}
