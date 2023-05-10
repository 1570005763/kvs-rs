use std::path::{PathBuf};

use sled;

use crate::{KvsError, Result, KvsEngine};

/// implements KvsEngine for the sled storage engine.
#[derive(Debug)]
pub struct SledKvsEngine {
    sled_db: sled::Db,
}

impl KvsEngine for SledKvsEngine {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.sled_db.insert(key, value.as_bytes()).unwrap();
        Ok(())
    }

    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let get_result = self.sled_db.get(key).unwrap();
        match get_result {
            Some(iv) => Ok(Some(std::str::from_utf8(iv.as_ref()).unwrap().to_string())),
            None => Ok(None),
        }
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    fn remove(&mut self, key: String) -> Result<()> {
        let rm_result = self.sled_db.remove(key).unwrap();
        match rm_result {
            Some(_) => Ok(()),
            None => Err(KvsError::KeyNotFound),
        }
    }
}

impl SledKvsEngine {
    /// Opens a `SledKvsEngine` with the given path.
    ///
    /// This will create a new database file if the given one does not exist.
    pub fn open(db_path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let mut path: PathBuf = db_path.into();
        path.push("sled");
        path.set_extension("db");
        Ok(SledKvsEngine {
            sled_db: sled::open(&path).unwrap(),
        })
    }
}