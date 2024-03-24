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
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, BufRead, Write};

pub struct Database {
    file: File,
    writer: io::BufWriter<File>,
    // TODO: for now just mock but we'll
    // do it properly in the file system :)
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Database {
    pub fn new(filename: &str, truncate: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate)
            // .append(true)
            .open(filename)?;

        Ok(Self {
            writer: io::BufWriter::new(file.try_clone()?),
            file,
            map: BTreeMap::new(),
        })
    }

    // very inefficient
    fn read(&self) -> impl Iterator<Item = Vec<Vec<u8>>> {
        let mut file_copy = self.file.try_clone().unwrap();
        file_copy.seek(SeekFrom::Start(0)).unwrap();
        io::BufReader::new(file_copy)
            .lines().map(Result::unwrap)
            .map(|line| {
                line.split(',')
                    .map(|s| s.as_bytes().to_vec())
                    .collect::<Vec<Vec<u8>>>()
            })
    }

    // extremely inefficient
    fn read_rev(&self) -> impl Iterator<Item = Vec<Vec<u8>>> {
        let reader = io::BufReader::new(self.file.try_clone().unwrap());
        let mut lines: Vec<_> = reader.lines().map(Result::unwrap).collect();
        lines.reverse();
        lines.into_iter().map(|line| {
            line.split(',')
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<Vec<u8>>>()
        })
    }
}

impl KV for Database {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        for mut line in self.read_rev() {
            let value = line.pop().unwrap();
            let line_key = line.pop().unwrap();

            if line_key == key {
                return Ok(value.to_vec());
            }
        }

        match self.map.get(key) {
            Some(value) => Ok(value.to_vec()),
            None => Err(Error::KeyNotFound),
        }
    }
    fn has(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.read().find(|line| line[0] == key).is_some())
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.writer.write_all(key).unwrap();
        self.writer.write_all(b",").unwrap();
        self.writer.write_all(value).unwrap();
        self.writer.write_all(b"\n").unwrap();
        self.writer.flush().unwrap();
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
    let mut db = Database::new("/tmp/test_full", true).unwrap();

    assert!(!db.has(b"abc").unwrap());

    db.put(b"abc", b"xyz").unwrap();

    assert!(db.has(b"abc").unwrap());
    assert_eq!(db.get(b"abc").unwrap(), b"xyz");

    // db.delete(b"abc").unwrap();

    // assert!(!db.has(b"abc").unwrap());
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
    let mut db = Database::new("/tmp/test_iter", true).unwrap();
    let numbers = vec!["one", "two", "three"];

    for (i, n) in numbers.iter().enumerate() {
        db.put((i + 1).to_string().as_bytes(), n.as_bytes())
            .unwrap();
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
