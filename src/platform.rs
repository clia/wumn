use crate::{
    error::ParseError,
    Database,
};
use cfg_if::cfg_if;
use log::*;
use std::{
    convert::TryFrom,
    ops::Deref,
};
use url::Url;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    use crate::pg::PostgresDB;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use crate::sq::SqliteDB;
}}

pub enum DBPlatform {
    #[cfg(feature = "with-postgres")]
    Postgres(PostgresDB),
    #[cfg(feature = "with-sqlite")]
    Sqlite(SqliteDB),
}

impl Deref for DBPlatform {
    type Target = dyn Database;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(ref pg) => pg.deref(),
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(ref sq) => sq.deref(),
        }
    }
}

pub(crate) enum Platform {
    #[cfg(feature = "with-postgres")]
    Postgres,
    #[cfg(feature = "with-sqlite")]
    Sqlite(String),
    Unsupported(String),
}

impl<'a> TryFrom<&'a str> for Platform {
    type Error = ParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let url = Url::parse(s);
        match url {
            Ok(url) => {
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "with-postgres")]
                    "postgres" => Ok(Platform::Postgres),
                    #[cfg(feature = "with-sqlite")]
                    "sqlite" => {
                        let host = url.host_str().unwrap();
                        let path = url.path();
                        let path = if path == "/" { "" } else { path };
                        let db_file = format!("{}{}", host, path);
                        Ok(Platform::Sqlite(db_file))
                    }
                    _ => Ok(Platform::Unsupported(scheme.to_string())),
                }
            }
            Err(e) => Err(ParseError::DbUrlParseError(e)),
        }
    }
}
