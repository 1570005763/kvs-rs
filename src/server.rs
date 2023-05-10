use std::fs::OpenOptions;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use log::info;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::engines::{KvsEngine, KvStore, SledKvsEngine};
use crate::util::{Command, EngineType, Response};
use crate::error::{KvsError, Result};

enum Engine {
    KvStore(KvStore),
    SledKvsEngine(SledKvsEngine),
}

/// a key-value store server 
pub struct Server {
    engine_type: EngineType,
    config_path: PathBuf,
    listener: TcpListener,
}

impl Server {
    /// build a new Server and listen to the Client with a TCP listener.
    pub fn new(addr: SocketAddr, engine_type: EngineType) -> Result<Server> {
        let mut server = Server {
            engine_type,
            config_path: "./config.json".into(),
            listener: TcpListener::bind(addr).unwrap(),
        };

        match server.check_engine() {
            Ok(engine_type) => {
                if server.engine_type == EngineType::DEFAULT {
                    server.engine_type = engine_type;
                }

                Ok(server)
            },
            Err(_) => {
                info!("check engine fail");
                exit(-1);
            },
        }
    }

    /// listen and handle commands from cients.
    pub fn listen(&self) -> Result<()> {
        // accept connections and process them serially
        info!("start listening...");
        for stream in self.listener.incoming() {
            self.handle_client(stream.unwrap())?;
            info!("im listening...");
        }
        info!("stop listening...");
        Ok(())
    }
    
    fn check_engine(&self) -> Result<EngineType> {
        let local_engine = {
            if self.config_path.exists() {
                let config: Value = {
                    let config = std::fs::read_to_string(&self.config_path)?;
                    serde_json::from_str(&config).unwrap()
                };
                let local_engine = config["engine_type"].as_str().unwrap();
                EngineType::from_str(local_engine).expect("Unable to parse engine.")
            } else {
                EngineType::DEFAULT
            }
        };
        info!("local_engine: {}, engine_type: {}", local_engine, self.engine_type);
        let engine_type = match (&local_engine, &self.engine_type) {
            (EngineType::KVS, EngineType::KVS) | 
            (EngineType::KVS, EngineType::DEFAULT) |
            (EngineType::DEFAULT, EngineType::KVS) |
            (EngineType::DEFAULT, EngineType::DEFAULT)
                => EngineType::KVS,
            (EngineType::SLED, EngineType::SLED) |
            (EngineType::SLED, EngineType::DEFAULT) |
            (EngineType::DEFAULT, EngineType::SLED)
                => EngineType::SLED,
            (EngineType::KVS, EngineType::SLED) |
            (EngineType::SLED, EngineType::KVS)
                => { return Err(KvsError::UnexpectedConfig); },
        };
        if local_engine == EngineType::DEFAULT {
            let config = json!({
                "engine_type": engine_type.to_string(),
            });
            let serialized_config = serde_json::to_string(&config).unwrap();

            let mut f = OpenOptions::new().write(true).create(true).open(&self.config_path)?;
            f.write(serialized_config.as_bytes()).expect("write config.json failed.");
        };
        Ok(engine_type)
    }

    fn handle_client(&self, stream: TcpStream) -> Result<()> {
        println!("start handle");
        let cmd = {
            let mut de = serde_json::Deserializer::from_reader(&stream);
            Command::deserialize(&mut de).unwrap()
        };
        // println!("cmd: {:#?}", cmd);
        // let cmd: Command = match serde_json::Deserializer::from_reader(&stream) {
        //     Ok(&mut v) => Command::deserialize(&mut v)?,
        //     Err(err) => {
        //         // failed to parse, return error.
        //         return Err(KvsError::Sered(err));
        //     },
        // };

        // start specified engine
        let engine = match self.engine_type {
            EngineType::KVS => Engine::KvStore(KvStore::open("./").unwrap()),
            EngineType::SLED => Engine::SledKvsEngine(SledKvsEngine::open("./").unwrap()),
            EngineType::DEFAULT => { return Err(KvsError::UnexpectedConfig); },
        };

        let res = match cmd {
            Command::Set { key, value } => self.handle_set(engine, key, value),
            Command::Get { key } => self.handle_get(engine, key),
            Command::Rm { key } => self.handle_remove(engine, key),
        }.unwrap();

        // let serialized_res = serde_json::to_string(&res).unwrap();
        serde_json::to_writer(&stream, &res)?;

        Ok(())
    }

    fn handle_set(&self, engine: Engine, key: String, value: String) -> Result<Response> {
        let set_result = match engine {
            Engine::KvStore(mut e) => e.set(key, value),
            Engine::SledKvsEngine(mut e) => e.set(key, value),
        };
        match set_result {
            Ok(_) => Ok(Response { res: true, info: "".to_string() }),
            Err(err) => Ok(Response { res: false, info: err.to_string() }),
        }
    }

    fn handle_get(&self, engine: Engine, key: String) -> Result<Response> {
        let get_result = match engine {
            Engine::KvStore(mut e) => e.get(key),
            Engine::SledKvsEngine(mut e) => e.get(key),
        };
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

    fn handle_remove(&self, engine: Engine, key: String) -> Result<Response> {
        let rm_result = match engine {
            Engine::KvStore(mut e) => e.remove(key),
            Engine::SledKvsEngine(mut e) => e.remove(key),
        };
        match rm_result {
            Ok(_) => Ok(Response { res: true, info: "".to_string() }),
            Err(err) => Ok(Response { res: false, info: format!("{}", err) }),
        }
    }
}