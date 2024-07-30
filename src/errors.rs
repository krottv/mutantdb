use std::{io, result};
use std::sync::Arc;
use prost::{DecodeError, EncodeError};
use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    // arc is necessary, so that error class is cloneable.
    Io(Arc<io::Error>),

    #[error("protobuf encode error")]
    EncodeError(EncodeError),
    
    #[error("protobuf decode error")]
    DecodeError(DecodeError),
    
    #[error("unknown error")]
    Unknown,
    
    #[error("absent key")]
    AbsentKey,

    #[error("read invalid range {0}")]
    ReadInvalidRange(String),

    #[error("corrupted file because of crc verification failed")]
    CorruptedFileError,
    
    #[error("Expected version {0}")]
    ManifestFormatVersionErr(u32)
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(Arc::new(value))
    }
}

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        Error::EncodeError(value)
    }
}

impl From<DecodeError> for Error {
    fn from(value: DecodeError) -> Self {
        Error::DecodeError(value)
    }
}