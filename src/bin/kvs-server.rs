use std::{net::SocketAddr, str::FromStr};
use clap::{arg, command};
use log::{info, error};

// use kvs::KvStore;
use kvs::{Server, EngineType};

fn main() {
    env_logger::init();

    let matches = command!()
        .propagate_version(true)
        // .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(arg!(--addr <IP_PORT> "IP address(v4/v6) and a port number, with the format IP:PORT.").default_value("127.0.0.1:4000"))
        .arg(arg!(--engine <ENGINE_NAME> "Engine used by database, either kvs or sled.").default_value("default"))
        .get_matches();

    let addr = matches.get_one::<String>("addr").unwrap();
    let engine = matches.get_one::<String>("engine").unwrap();

    let addr: SocketAddr = addr.parse().expect("Unable to parse socket address");
    let engine: EngineType = EngineType::from_str(engine).expect("Unable to parse engine.");

    info!("server:");
    error!("version number: {}", env!("CARGO_PKG_VERSION"));
    error!("IP address and port: {}", addr);
    error!("storage engine: {}", engine);

    let server = Server::new(addr, engine).unwrap();
    server.listen().unwrap();
}