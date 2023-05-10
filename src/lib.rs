#![deny(missing_docs)]
//! A simple key/value store.

// pub use kv::KvStore;

pub use engines::{KvsEngine, KvStore, SledKvsEngine};
pub use util::{Command, EngineType};
pub use error::{KvsError, Result};
pub use server::Server;
pub use client::Client;

mod client;
mod server;
mod engines;
mod util;
mod error;
