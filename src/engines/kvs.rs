use std::collections::HashMap;
use std::path::PathBuf;
// use serde::{Serialize, Deserialize};
use serde_json::Deserializer;
use std::fs::{self, OpenOptions};
use std::io::{Write, Seek, SeekFrom};

use crate::util::Command;
use crate::{KvsError, Result, KvsEngine};

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
    kv_hashmap: HashMap<String, u64>,
    kv_log_path: PathBuf,
    compact_count: u32,
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key: key.clone(), value: value.clone() };
        let pos = self.append_to_log(&cmd)?;

        self.kv_hashmap.insert(key, pos);
        self.compact_count += 1;
        if self.compact_count >= 10000 {
            self.compact_log()?;
            self.compact_count = 0;
        }

        return Ok(());
    }

    /// Get the string value of a string key.
    /// If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let pos = self.kv_hashmap.get(&key).cloned();
        match pos {
            Some(p) => {
                let value = self.read_from_log(p)?;
                return Ok(value);
            },
            None => return Ok(None), 
        }
    }

    /// Remove a given key.
    /// Return an error if the key does not exist or is not removed successfully.
    fn remove(&mut self, key: String) -> Result<()> {
        if !self.kv_hashmap.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let cmd = Command::Rm { key: key.clone() };
        self.append_to_log(&cmd)?;

        self.kv_hashmap.remove(&key);
        self.compact_count += 1;
        if self.compact_count >= 10000 {
            self.compact_log()?;
            self.compact_count = 0;
        }

        return Ok(());
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
        let mut kv_store = KvStore {
            kv_hashmap: HashMap::new(),
            kv_log_path: path.into(),
            compact_count: 0,
        };
        kv_store.kv_log_path.push("log");
        kv_store.kv_log_path.set_extension("json");
        // kv_store.kv_log_path.push("log.json");
        // println!("kv_log_path:{:?}", kv_store.kv_log_path);

        // println!("{:?}", kv_store.kv_log_path);

        if !kv_store.kv_log_path.exists() {
            OpenOptions::new().write(true).create(true).open(&kv_store.kv_log_path)?;
            return Ok(kv_store);
        }
        
        let f = OpenOptions::new().read(true).open(&kv_store.kv_log_path)?;
        let mut stream = Deserializer::from_reader(f).into_iter::<Command>();
        let mut pos = 0;
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            // print!("cmd: {:?}", cmd);
            match cmd? {
                Command::Set { key, .. } => {
                    // println!("Set {{ key = {:?}, value = {:?} }}, pos = {}", key, value, pos);
                    kv_store.kv_hashmap.insert(key, pos);
                }
                Command::Get { .. } => {
                    // println!("Get {{ key = {:?} }}, pos = {}", key, pos);
                    // do nothing
                }
                Command::Rm { key } => {
                    // println!("Remove {{ key = {:?} }}, pos = {}", key, pos);
                    kv_store.kv_hashmap.remove(&key);
                }
            }
            pos = new_pos;
        }

        // compaction on open up
        kv_store.compact_log()?;

        return Ok(kv_store);
    }

    fn compact_log(&mut self) -> Result<()> {
        let log_path = self.kv_log_path.as_path();
        let log_backup_path= self.kv_log_path.parent().unwrap().join("log.backup.json");
        let log_backup_path = log_backup_path.as_path();
        // fs::copy(log_path, log_backup_path).expect("Copy log.json failed.");

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(log_backup_path)
            .unwrap();

        let mut kv_backup_hashmap: HashMap<String, u64> = HashMap::new();
        for (key, pos) in self.kv_hashmap.iter() {
            let value = self.read_from_log(pos.clone())?.unwrap();
            let backup_pos: u64 = f.stream_position()?;
            let cmd = Command::Set { key: key.clone(), value: value.clone() };
            let serialized_operation = serde_json::to_string(&cmd).unwrap();
            f.write(serialized_operation.as_bytes()).expect("write log.backup.json failed.");
            kv_backup_hashmap.insert(key.clone(), backup_pos);
            // println!("{} / {}", key, value);
        }

        // fs::remove_file(log_backup_path).expect("Delete log.backup.json failed.");
        fs::rename(log_backup_path, log_path).expect("rename log.backup.json failed.");
        self.kv_hashmap = kv_backup_hashmap;

        return Ok(());
    }

    fn append_to_log(&self, cmd: &Command) -> Result<u64> {
        let serialized_operation = serde_json::to_string(&cmd).unwrap();

        // Open a file with append option
        let mut data_file = OpenOptions::new()
            .append(true)
            .read(true)
            .write(true)
            .open(&self.kv_log_path)
            .expect("cannot open log.json.");

        data_file.seek(SeekFrom::End(0))?;
        let pos: u64 = data_file.stream_position()?;
        // println!("pos: {}", pos);

        // Write to a file
        data_file
            .write(serialized_operation.as_bytes())
            .expect("write log.json failed.");

        return Ok(pos);
    }

    fn read_from_log(&self, pos: u64) -> Result<Option<String>> {
        let mut f = OpenOptions::new().read(true).open(&self.kv_log_path)?;
        f.seek(SeekFrom::Start(pos))?;
        let mut stream = Deserializer::from_reader(f).into_iter::<Command>();

        if let Some(cmd) = stream.next(){
            let value: Option<String> = match cmd? {
                Command::Set { value, .. } => {
                    // println!("Set {{ key = {:?}, value = {:?} }}, pos = {}", key, value, pos);
                    // kv_store.kv_hashmap.insert(key, pos);
                    Some(value)
                }
                Command::Get { .. } => {
                    // println!("Get {{ key = {:?} }}, pos = {}", key, pos);
                    // do nothing
                    None
                }
                Command::Rm { .. } => {
                    // println!("Remove {{ key = {:?} }}, pos = {}", key, pos);
                    // kv_store.kv_hashmap.remove(&key);
                    None
                }
            };

            return Ok(value);
        }

        return Err(KvsError::UnexpectedCommandType);
    }
}
