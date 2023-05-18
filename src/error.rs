use failure::Fail;
use std::io;

/// Error type for kvs
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Non-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,

    /// Unexpected config error.
    #[fail(display = "Unexpected config")]
    UnexpectedConfig,

    /// IO error
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),

    /// Serialization or deserialization error
    #[fail(display = "serde_json error: {}", _0)]
    Sered(#[cause] serde_json::Error),

    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Sered(err)
    }
}

/// A type alias for Result that includes your concrete error type,
/// so that you don't need to type Result<T, YourErrorType> everywhere,
/// but can simply type Result<T>.
pub type Result<T> = std::result::Result<T, KvsError>;
