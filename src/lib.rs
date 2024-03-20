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
    // TODO: for now just mock but we'll
    // do it properly in the file system :)
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

pub struct DBIterator {
    map: std::collections::btree_map::IntoIter<Vec<u8>, Vec<u8>>,
}

impl IntoIterator for Database {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = DBIterator;
    fn into_iter(self) -> Self::IntoIter {
        DBIterator {
            map: self.map.into_iter(),
        }
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        self.map.next()
    }
}

#[test]
fn test_iter() {
    let mut db = Database::new();
    let numbers = vec!["one", "two", "three"];

    for (i, n) in numbers.iter().enumerate() {
        db.put((i + 1).to_string().as_bytes(), n.as_bytes()).unwrap();
    }

    assert_eq!(
        db.into_iter().collect::<Vec<_>>(),
        vec![
            (b"1".to_vec(), b"one".to_vec()),
            (b"2".to_vec(), b"two".to_vec()),
            (b"3".to_vec(), b"three".to_vec())
        ]
    );
}
