use std::net::{SocketAddr, TcpListener, TcpStream};
use log::info;
use serde::Deserialize;

use crate::ThreadPool;
use crate::engines::KvsEngine;
use crate::util::{Command, Response};
use crate::error::Result;

/// a key-value store server 
pub struct Server<E: KvsEngine, P: ThreadPool> {
    engine: E,
    thread_pool: P,
}

impl<E: KvsEngine, P: ThreadPool> Server<E, P> {
    /// build a new Server and listen to the Client with a TCP listener.
    pub fn new(engine: E, thread_pool: P) -> Result<Server<E, P>> {
        Ok(Server { engine, thread_pool })
    }

    /// listen and handle commands from cients.
    pub fn listen(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).unwrap();

        // accept connections and process them serially
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.thread_pool.spawn(move || {
                handle_client(engine, stream.unwrap()).unwrap()
            });
        }
        info!("stop listening...");
        Ok(())
    }
}

fn handle_client<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let cmd = {
        let mut de = serde_json::Deserializer::from_reader(&stream);
        Command::deserialize(&mut de).unwrap()
    };

    let res = match cmd {
        Command::Set { key, value } => handle_set(engine, key, value),
        Command::Get { key } => handle_get(engine, key),
        Command::Rm { key } => handle_remove(engine, key),
    }.unwrap();

    // let serialized_res = serde_json::to_string(&res).unwrap();
    serde_json::to_writer(&stream, &res)?;

    Ok(())
}

fn handle_set<E: KvsEngine>(engine: E, key: String, value: String) -> Result<Response> {
    let set_result = engine.set(key, value);
    match set_result {
        Ok(_) => Ok(Response { res: true, info: "".to_string() }),
        Err(err) => Ok(Response { res: false, info: err.to_string() }),
    }
}

fn handle_get<E: KvsEngine>(engine: E, key: String) -> Result<Response> {
    let get_result = engine.get(key);
    match get_result {
        Ok(v) => {
            match v {
                Some(v) => Ok(Response { res: true, info: v.to_string()+"\n".into() }),
                None => Ok(Response { res: true, info: "Key not found".to_string() }),
            }
        },
        Err(err) => Ok(Response { res: false, info: err.to_string() }),
    }
}

fn handle_remove<E: KvsEngine>(engine: E, key: String) -> Result<Response> {
    let rm_result = engine.remove(key);
    match rm_result {
        Ok(_) => Ok(Response { res: true, info: "".to_string() }),
        Err(err) => Ok(Response { res: false, info: format!("{}", err) }),
    }
}