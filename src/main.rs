use std::{
    collections::HashMap, error::Error, fmt::{self, write, Display}, fs::read, path::{Path, PathBuf}
};
use async_trait::async_trait;

use serde::{Deserialize, Serialize};
use tokio::{fs::{ File, OpenOptions }, io::{AsyncBufReadExt, AsyncWriteExt, BufReader}};
struct Db {
  log: Log,
  memtable: Memtable
}

struct Log {
    path: PathBuf,
    log: File,
}

#[derive(Serialize, Deserialize)]
struct Put {
    key: String,
    value: String,
}

struct Memtable{
    memtable: HashMap<String, String>,
}

#[derive(Debug)]
enum DBError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl Error for DBError{}

impl Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::Io(err) => write!(f, "IO error: {}", err),
            DBError::Serde(err) => write!(f, "Serde error: {}", err),
        }
    }
}

impl From<std::io::Error> for DBError {
    fn from(err: std::io::Error) -> DBError {
        DBError::Io(err)
    }
}

impl From<serde_json::Error> for DBError {
    fn from(value: serde_json::Error) -> Self {
        DBError::Serde(value)
    }
}

impl Memtable {
    fn new(memtable: HashMap<String, String>) -> Self {
      Memtable {
        memtable: HashMap::new(),
      }
    }

    fn put(&mut self , key: String, value: String) {
        self.memtable.insert(key, value);
    }
}


#[async_trait]
trait Queryable {
    async fn get(&self, key: &str) -> Result<Option<String>, DBError>;
}


#[async_trait]
impl Queryable for Memtable {
    async fn get(&self, key: &str) -> Result<Option<String>, DBError> {
        Ok(self.memtable.get(key).map(|s| s.to_string()))
    }
}

impl Log {
    async fn open(path: impl AsRef<Path>) -> Result<Log, DBError> {
        let log = OpenOptions::new()
                                                            .append(true)
                                                            .create(true)
                                                            .open(&path).await?;
        Ok(Log {
            path: path.as_ref().to_path_buf(),
            log,
        })
    }

    async fn put(&mut self, key: String, value: String) -> Result<(), DBError> {
        let put = Put { key, value };
        let data = serde_json::to_string(&put)?;
        self.log.write_all(data.as_bytes()).await?;
        self.log.write_all(b"\n").await?;
        self.log.sync_all().await?;
        Ok(())
     } 

     async fn hydrate(&self) -> Result<Memtable, DBError> {
        let reader = File::open(&self.path).await?;
        let reader = BufReader::new(reader);
        let mut lines  = reader.lines();
        let mut memtable = HashMap::new();
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            memtable.insert(put.key, put.value);
        }
        Ok(Memtable::new(memtable))

     }
}


#[async_trait]
impl Queryable for Log {

    async fn get(&self, key: &str) -> Result<Option<String>, DBError> {
        let reader = File::open(&self.path).await?;
        let reader = BufReader::new(reader);
        let mut lines = reader.lines();
        let mut result = None;
        while let Some(line) = lines.next_line().await? {
            let put: Put = serde_json::from_str(&line)?;
            if put.key == key {
                result = Some(put.value);
            }
        }
        Ok(result)

    }

 }

 impl Db {
    async fn new(dir: impl AsRef<Path>) -> Result<Db, DBError> {
        if !dir.as_ref().exists() {
            tokio::fs::create_dir_all(&dir).await?;
        }
        let log = Log::open(dir.as_ref().join("log")).await?;
        let memtable = log.hydrate().await?;
        Ok(Db { log, memtable })
    }

    async fn put(&mut self, key: &str, value: &str) -> Result<(), DBError> {
        self.log.put(key.into(), value.into()).await?;
        self.memtable.put(key.into(), value.into());

        Ok(())
    }

    async fn get(&mut self, key: &str) -> Result<Option<String>, DBError> {
        Ok(self.memtable.get(key.as_ref()).await?)
    }
 }


#[tokio::main]
async fn main() -> Result<(), DBError> {
    let mut db = Db::new("db").await?;
     db.put("foo", "bar").await?;
    db.put("baz", "qux").await?;
    db.put("foo", "goo").await?;
    println!("{:?}", db.get("foo").await?);
    Ok(())


}


