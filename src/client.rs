use std::net::{SocketAddr, TcpStream};

// use log::{info, error};

use crate::util::{Command, Response};
use crate::error::{KvsError, Result};

/// a command-line key-value store client
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// build a new Client and connect to the Server with a TCP stream.
    pub fn new(addr: SocketAddr) -> Result<Client> {
        let client = Client {
            stream: TcpStream::connect(addr).unwrap(),
        };

        Ok(client)
    }

    /// send a command to the server and handle response.
    pub fn send(self, cmd: Command) -> Result<()> {
        // let serialized_cmd = serde_json::to_string(&cmd).unwrap();
        // println!("serialized_cmd: {}", serialized_cmd);
        serde_json::to_writer(&self.stream, &cmd)?;

        // println!("already to writer.");

        let res: Response = match serde_json::from_reader(&self.stream) {
            Ok(v) => v,
            Err(err) => {
                // failed to parse, return error.
                return Err(KvsError::Sered(err));
            },
        };

        match res.res {
            true => { print!("{}", res.info); },
            false => { return Err(KvsError::StringError(res.info)); },
        }

        Ok(())
    }
}