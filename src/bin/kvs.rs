use clap::{arg, command, Command};
use std::process;

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

    let mut kv_store: KvStore = KvStore::new();

    // loop {
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            // let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            // let value : String = sub_matches.get_one::<String>("VALUE").unwrap().to_string();
            // kv_store.set(key, value);
            unimplemented!("unimplemented");
            process::exit(-1);
        }
        Some(("get", sub_matches)) => {
            // let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            // kv_store.get(key);
            unimplemented!("unimplemented");
            process::exit(-1);
        }
        Some(("rm", sub_matches)) => {
            // let key: String = sub_matches.get_one::<String>("KEY").unwrap().to_string();
            // kv_store.remove(key);
            unimplemented!("unimplemented");
            process::exit(-1);
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents `None`"),
        // }
    }
}
