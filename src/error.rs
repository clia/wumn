use cfg_if::cfg_if;

use std::{
    error::Error,
    fmt,
};
use url;
// use r2d2::R2D2Error;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    use crate::pg::PostgresError;
}}

#[derive(Debug)]
pub enum ConnectError {
    NoSuchPoolConnection,
    ParseError(ParseError),
    UnsupportedDb(String),
    R2d2Error(r2d2::Error),
}

impl Error for ConnectError {}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug)]
pub enum ParseError {
    DbUrlParseError(url::ParseError),
}

#[derive(Debug)]
pub enum PlatformError {
    #[cfg(feature = "with-postgres")]
    PostgresError(PostgresError),
    #[cfg(feature = "with-sqlite")]
    SqliteError(SqliteError),
}

#[cfg(feature = "with-postgres")]
impl From<PostgresError> for PlatformError {
    fn from(e: PostgresError) -> Self {
        PlatformError::PostgresError(e)
    }
}

#[cfg(feature = "with-postgres")]
impl From<PostgresError> for DbError {
    fn from(e: PostgresError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::PlatformError(PlatformError::SqliteError(SqliteError::from(e)))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for PlatformError {
    fn from(e: SqliteError) -> Self {
        PlatformError::SqliteError(e)
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for DbError {
    fn from(e: SqliteError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[derive(Debug)]
pub enum DbError {
    SqlInjectionAttempt(String),
    DataError(DataError),
    PlatformError(PlatformError),
    ConvertError(ConvertError),
    ConnectError(ConnectError), //agnostic connection error
    UnsupportedOperation(String),
    // PoolError(String),
}

impl From<PlatformError> for DbError {
    fn from(e: PlatformError) -> Self {
        DbError::PlatformError(e)
    }
}

// impl From<R2d2Error> for DbError {
//     fn from(e: R2D2Error) -> Self {
//         DbError::PoolError(e.to_string())
//     }
// }

#[derive(Debug)]
pub enum ConvertError {
    UnknownDataType,
    UnsupportedDataType(String),
}

#[derive(Debug)]
pub enum DataError {
    ZeroRecordReturned,
    MoreThan1RecordReturned,
}
