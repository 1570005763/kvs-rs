use clap::{arg, command, Command};
use std::process;
use tempfile::TempDir;

use kvs::KvStore;

fn main() {
    let matches = command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("set")
                .about("Add key-value to store")
                .arg(arg!([KEY]).required(true))
                .arg(arg!([VALUE]).required(true)),
        )
        .subcommand(
            Command::new("get")
                .about("Get value of key")
                .arg(arg!([KEY]).required(true)),
        )
        .subcommand(
            Command::new("rm")
                .about("remove key-value")
                .arg(arg!([KEY]).required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let mut kv_store = KvStore::open("./").expect("Can not open KvStore.");
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            let value : String = sub_matches.get_one::<String>("VALUE").unwrap().to_string();
            kv_store.set(key, value).expect("Set key error.");
        }
        Some(("get", sub_matches)) => {
            let kv_store = KvStore::open("./").expect("Can not open KvStore.");
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            let value = kv_store.get(key).expect("Get key error.");
            match value {
                None => println!("Key not found"),
                Some(v) => println!("{}", v),
            }
        }
        Some(("rm", sub_matches)) => {
            let mut kv_store = KvStore::open("./").expect("Can not open KvStore.");
            let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            let rm_res = kv_store.remove(key);
            match rm_res {
                Ok(()) => {},
                Err(_) => {
                    println!("Key not found");
                    panic!("Key not found");
                },
            }
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents `None`"),
        // }
    }
}
