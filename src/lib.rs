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

use indexmap::IndexMap;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Seek, SeekFrom, Write};

pub struct Database {
    file: RefCell<File>,
    reader: RefCell<io::BufReader<File>>,
    writer: io::BufWriter<File>,
    idxs: IndexMap<Vec<u8>, u64>,
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
            reader: io::BufReader::new(file.try_clone()?).into(),
            writer: io::BufWriter::new(file.try_clone()?),
            file: file.into(),
            idxs: IndexMap::new(),
        })
    }
}

impl KV for Database {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        let idx = match self.idxs.get(key) {
            Some(idx) => idx,
            None => return Err(Error::KeyNotFound),
        };
        self.file.borrow_mut().seek(SeekFrom::Start(*idx)).unwrap();
        let mut value = vec![];
        self.reader
            .borrow_mut()
            .read_until(b'\n', &mut value)
            .unwrap();
        // remove \n
        value.pop();
        Ok(value)
    }
    fn has(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.idxs.contains_key(key))
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.writer.write_all(key).unwrap();
        self.writer.write_all(b",").unwrap();
        self.writer.flush().unwrap();
        self.idxs.insert(
            key.to_vec(),
            self.file.borrow_mut().stream_position().unwrap(),
        );
        self.writer.write_all(value).unwrap();
        self.writer.write_all(b"\n").unwrap();
        self.writer.flush().unwrap();
        Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        // O(n)
        self.idxs.shift_remove(key);
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

    db.delete(b"abc").unwrap();

    assert!(!db.has(b"abc").unwrap());
}

pub struct DBIterator {
    reader: RefCell<io::BufReader<File>>,
    idxs: indexmap::map::IntoIter<Vec<u8>, u64>,
}

impl IntoIterator for Database {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = DBIterator;
    fn into_iter(self) -> Self::IntoIter {
        DBIterator {
            reader: self.reader,
            idxs: self.idxs.into_iter(),
        }
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);
    // very similar code to Database::get()
    // perhaps we could abstract
    fn next(&mut self) -> Option<Self::Item> {
        let (key, offset) = self.idxs.next()?;
        self.reader
            .borrow_mut()
            .seek(SeekFrom::Start(offset))
            .unwrap();
        let mut value = vec![];
        self.reader
            .borrow_mut()
            .read_until(b'\n', &mut value)
            .unwrap();
        // remove \n
        value.pop();
        Some((key, value))
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
