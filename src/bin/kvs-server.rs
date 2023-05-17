use std::env::{current_dir, self};
use std::fs::OpenOptions;
use std::io::Write;
use std::{net::SocketAddr, str::FromStr, path::PathBuf};
use clap::{arg, command};
use log::{info, error};
use serde_json::{Value, json};
use strum::{Display, EnumString};

use kvs::thread_pool::*;
use kvs::*;

const THREAD_NUM: u32 = 1;
// const CONFIG_PATH: &str = "./config.json";

/// defines the storage interface called by KvsServer
#[derive(Display, Clone, EnumString, PartialEq)]
pub enum EngineType {
    /// the built-in engine
    #[strum(serialize="KVS", serialize="kvs")]
    KVS,

    /// sled engine
    #[strum(serialize="SLED", serialize="sled")]
    SLED,

    /// default engine,
    #[strum(serialize="DEFAULT", serialize="default")]
    DEFAULT,
}

fn main() {
    env_logger::init();

    let matches = command!()
        .propagate_version(true)
        // .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(arg!(--addr <IP_PORT> "IP address(v4/v6) and a port number, with the format IP:PORT.").default_value("127.0.0.1:4000"))
        .arg(arg!(--engine <ENGINE_NAME> "Engine used by database, either kvs or sled.").default_value("default"))
        .get_matches();

    let addr = {
        let addr = matches.get_one::<String>("addr").unwrap();
        let addr = addr.parse().expect("Unable to parse socket address");
        addr
    };

    let engine_type = {
        let engine_type = matches.get_one::<String>("engine").unwrap();
        let engine_type = EngineType::from_str(engine_type).expect("Unable to parse engine.");
        let engine_type = get_engine_type(engine_type).unwrap();
        engine_type
    };

    let thread_pool = NaiveThreadPool::new(THREAD_NUM).unwrap();

    info!("server:");
    error!("version number: {}", env!("CARGO_PKG_VERSION"));
    error!("IP address and port: {}", addr);
    error!("storage engine: {}", engine_type);

    match engine_type {
        EngineType::KVS => run_with(KvStore::open(env::current_dir().unwrap()).unwrap(), thread_pool, addr),
        EngineType::SLED => run_with(SledKvsEngine::open(env::current_dir().unwrap()).unwrap(), thread_pool, addr),
        EngineType::DEFAULT => Err(KvsError::UnexpectedConfig),
    }.unwrap();
}

fn get_engine_type(engine_type: EngineType) -> Result<EngineType> {
    // let config_path: PathBuf = CONFIG_PATH.into();
    let config_path: PathBuf = current_dir()?.join("config.json");
    let local_engine = {
        if config_path.exists() {
            let config: Value = {
                let config = std::fs::read_to_string(&config_path)?;
                serde_json::from_str(&config).unwrap()
            };
            let local_engine = config["engine_type"].as_str().unwrap();
            EngineType::from_str(local_engine).expect("Unable to parse engine.")
        } else {
            EngineType::DEFAULT
        }
    };
    
    let engine_type = match (&local_engine, &engine_type) {
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

        let mut f = OpenOptions::new().write(true).create(true).open(&config_path)?;
        f.write(serialized_config.as_bytes()).expect("write config.json failed.");
    };

    Ok(engine_type)
}

fn run_with<E:KvsEngine, P: ThreadPool>(kvs_engine: E, thread_pool: P, addr: SocketAddr) -> Result<()> {
    let server = Server::new(kvs_engine, thread_pool).unwrap();
    server.listen(addr).unwrap();
    Ok(())
}