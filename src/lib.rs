pub enum Error {
    KeyNotFound,
}

pub trait KV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Error>;
    fn has(&self, key: &[u8]) -> Result<bool, Error>;
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    fn delete(&mut self, key: &[u8]) -> Result<(), Error>;
}
