use crate::Value;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::fmt::Debug;

#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

impl Error for ConvertError {
    fn description(&self) -> &str {
        "Conversion is not supported"
    }
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

#[derive(Debug)]
pub enum DaoError<'a, T>
where
    T: TryFrom<&'a Value>,
    T::Error: Debug,
{
    ConvertError(T::Error),
    NoSuchValueError(String),
}
