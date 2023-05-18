use clap::{arg, command, Command};
use std::net::SocketAddr;

// use kvs::KvStore;
use kvs::{Client, Command as Cmd};

fn main() {
    let matches = command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            arg!(--addr <IP_PORT> "IP address(v4/v6) and a port number, with the format IP:PORT.")
                .default_value("127.0.0.1:4000")
                .global(true),
        )
        .subcommand(
            Command::new("set")
                .about("Add key-value to database")
                .arg(arg!([KEY]).required(true))
                .arg(arg!([VALUE]).required(true)),
        )
        .subcommand(
            Command::new("get")
                .about("Get value of key from database")
                .arg(arg!([KEY]).required(true)),
        )
        .subcommand(
            Command::new("rm")
                .about("remove key-value from database")
                .arg(arg!([KEY]).required(true)),
        )
        .get_matches();

    let addr = matches.get_one::<String>("addr").unwrap();

    let addr: SocketAddr = addr.parse().expect("Unable to parse socket address.");

    let cmd: Cmd = match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            let value: String = sub_matches.get_one::<String>("VALUE").unwrap().to_string();
            Cmd::Set { key, value }
        }
        Some(("get", sub_matches)) => {
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            Cmd::Get { key }
        }
        Some(("rm", sub_matches)) => {
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            Cmd::Rm { key }
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents `None`"),
    };

    let client = Client::new(addr).unwrap();
    client.send(cmd).unwrap();
}
