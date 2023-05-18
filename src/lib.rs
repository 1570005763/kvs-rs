#![deny(missing_docs)]
//! A simple key/value store.

// pub use kv::KvStore;

pub use client::Client;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::Server;
pub use thread_pool::ThreadPool;
pub use util::Command;

mod client;
mod engines;
mod error;
mod server;
/// ?
pub mod thread_pool;
mod util;
