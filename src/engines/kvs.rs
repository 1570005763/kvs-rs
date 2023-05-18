use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
// use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use crate::util::Command;
use crate::{KvsEngine, KvsError, Result};

const COMPACT_INTERVAL: u32 = 10000;

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// use kvs::KvsEngine;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Default, Debug)]
pub struct KvStore {
    kv_log_path: PathBuf,
    kv_hashmap: Arc<Mutex<HashMap<String, u64>>>,
    compact_count: Arc<Mutex<u32>>,
    file: Arc<Mutex<Option<File>>>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            kv_log_path: self.kv_log_path.clone(),
            kv_hashmap: Arc::clone(&self.kv_hashmap),
            compact_count: Arc::clone(&self.compact_count),
            file: Arc::clone(&self.file),
        }
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };

        {
            // Mutex: kv_hashmap
            let mut kv_hashmap = self.kv_hashmap.lock().unwrap();
            let pos = self.append_to_log(&cmd)?;
            kv_hashmap.insert(key, pos);
        }

        self.try_compact_log()?;

        Ok(())
    }

    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        {
            let kv_hashmap = self.kv_hashmap.lock().unwrap();

            let pos = kv_hashmap.get(&key).cloned();
            match pos {
                Some(p) => {
                    let value = self.read_from_log(p)?;
                    Ok(value)
                }
                None => Ok(None),
            }
        }
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    fn remove(&self, key: String) -> Result<()> {
        let cmd = Command::Rm { key: key.clone() };

        {
            let mut kv_hashmap = self.kv_hashmap.lock().unwrap();
            if !kv_hashmap.contains_key(&key) {
                return Err(KvsError::KeyNotFound);
            }
            self.append_to_log(&cmd)?;
            kv_hashmap.remove(&key);
        }

        self.try_compact_log()?;

        Ok(())
    }
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let kv_log_path = {
            let mut kv_log_path = path.into();
            kv_log_path.push("log");
            kv_log_path.set_extension("json");
            kv_log_path
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&kv_log_path)?;

        let kv_store = KvStore {
            kv_log_path,
            kv_hashmap: Arc::new(Mutex::new(HashMap::new())),
            compact_count: Arc::new(Mutex::new(0)),
            file: Arc::new(Mutex::new(Some(file))),
        };

        kv_store.build_hashmap_from_log()?;
        kv_store.try_compact_log()?;

        Ok(kv_store)
    }

    fn build_hashmap_from_log(&self) -> Result<()> {
        let mut kv_hashmap = self.kv_hashmap.lock().unwrap();
        let file = self.file.lock().unwrap();
        match &*file {
            Some(f) => {
                let mut stream = Deserializer::from_reader(f).into_iter::<Command>();
                let mut pos = 0;
                while let Some(cmd) = stream.next() {
                    let new_pos = stream.byte_offset() as u64;
                    match cmd? {
                        Command::Set { key, .. } => {
                            kv_hashmap.insert(key, pos);
                        }
                        Command::Get { .. } => {
                            // do nothing
                        }
                        Command::Rm { key } => {
                            kv_hashmap.remove(&key);
                        }
                    }
                    pos = new_pos;
                }
                Ok(())
            }
            None => Err(KvsError::StringError("file not initialized".to_string())),
        }
    }

    fn try_compact_log(&self) -> Result<()> {
        {
            // Mutex: compact_count
            let mut compact_count = self.compact_count.lock().unwrap();
            if *compact_count > 0 && *compact_count < COMPACT_INTERVAL {
                *compact_count += 1;
                return Ok(());
            }
            *compact_count = 1;
        }

        let log_path = self.kv_log_path.as_path();
        let log_backup_path = self.kv_log_path.parent().unwrap().join("log.backup.json");
        let log_backup_path = log_backup_path.as_path();

        let mut backup_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(log_backup_path)
            .unwrap();

        {
            // Mutex: kv_hashmap
            let mut kv_backup_hashmap: HashMap<String, u64> = HashMap::new();
            let mut kv_hashmap = self.kv_hashmap.lock().unwrap();
            for (key, pos) in kv_hashmap.iter() {
                let value = self.read_from_log(*pos)?.unwrap();
                let backup_pos: u64 = backup_file.stream_position()?;
                let cmd = Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                };
                let serialized_operation = serde_json::to_string(&cmd).unwrap();
                backup_file
                    .write_all(serialized_operation.as_bytes())
                    .expect("write log.backup.json failed.");
                kv_backup_hashmap.insert(key.clone(), backup_pos);
            }

            {
                // Mutex: Option<File>
                let mut file = self.file.lock().unwrap();
                match &mut *file {
                    Some(_) => *file = None,
                    None => return Err(KvsError::StringError("file not initialized".to_string())),
                }
                fs::rename(log_backup_path, log_path).expect("rename log.backup.json failed.");
                *file = Some(
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&self.kv_log_path)?,
                );
            }

            *kv_hashmap = kv_backup_hashmap;
        }

        Ok(())
    }

    fn append_to_log(&self, cmd: &Command) -> Result<u64> {
        let serialized_operation = serde_json::to_string(&cmd).unwrap();

        {
            let mut file = self.file.lock().unwrap();
            match &mut *file {
                Some(f) => {
                    f.seek(SeekFrom::End(0))?;
                    let pos: u64 = f.stream_position()?;

                    // Write to a file
                    f.write_all(serialized_operation.as_bytes())
                        .expect("write log.json failed.");
                    Ok(pos)
                }
                None => Err(KvsError::StringError("file not initialized".to_string())),
            }
        }
    }

    fn read_from_log(&self, pos: u64) -> Result<Option<String>> {
        {
            let mut file = self.file.lock().unwrap();
            match &mut *file {
                Some(f) => {
                    f.seek(SeekFrom::Start(pos))?;
                    let mut stream = Deserializer::from_reader(f).into_iter::<Command>();
                    if let Some(cmd) = stream.next() {
                        let value: Option<String> = match cmd? {
                            Command::Set { value, .. } => Some(value),
                            Command::Get { .. } => None,
                            Command::Rm { .. } => None,
                        };
                        return Ok(value);
                    }
                }
                None => return Err(KvsError::StringError("file not initialized".to_string())),
            }
        }
        Err(KvsError::UnexpectedCommandType)
    }
}
