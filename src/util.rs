use serde::{Serialize, Deserialize};
use strum_macros::{Display, EnumString};
// use std::str::FromStr;

/// data structure of KvStore operation for serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    Set {
        /// key
        key: String, 
        /// value
        value: String 
    },

    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    Get { 
        /// key
        key: String 
    },

    /// Remove a given string key.
    /// Return an error if the key does not exit or value is not read successfully.
    Rm {
        /// key
        key: String 
    },
}

/// data structure of response for serialization and deserialization
#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    /// operation result
    /// true for success
    /// false for fail
    pub res: bool, 

    /// detail infomation for error or value
    pub info: String,
}

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