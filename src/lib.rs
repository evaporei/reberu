#[derive(Debug)]
pub enum Error {
    KeyNotFound,
}

pub trait KV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error>;
    fn has(&self, key: &[u8]) -> Result<bool, Error>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
}

use std::collections::BTreeMap;

pub struct Database {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
}

impl KV for Database {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        match self.map.get(key) {
            Some(value) => Ok(value.to_vec()),
            None => Err(Error::KeyNotFound),
        }
    }
    fn has(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.map.contains_key(key))
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.map.remove(key);
        Ok(())
    }
}

#[test]
fn test_full() {
    let mut db = Database::new();

    assert!(!db.has(b"abc").unwrap());

    db.put(b"abc", b"xyz").unwrap();

    assert!(db.has(b"abc").unwrap());
    assert_eq!(db.get(b"abc").unwrap(), b"xyz");

    db.delete(b"abc").unwrap();

    assert!(!db.has(b"abc").unwrap());
}
