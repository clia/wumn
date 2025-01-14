use base64;
use bigdecimal::BigDecimal;
use geo_types::geometry::Point;
use log::*;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::string::FromUtf8Error;
use wumn_dao::{value::Array, Interval, Rows};
//use openssl::ssl::{SslConnectorBuilder, SslMethod};
use postgres;
use postgres::{Client, NoTls};
//use postgres::tls::openssl::OpenSsl;
use crate::{
    table::SchemaContent,
    users::{Role, User},
    Database, DatabaseName, DbError, EntityManager, PlatformError, Table, TableName, Value,
};
use postgres::types::private::BytesMut;
use postgres::types::{self, FromSql, IsNull, ToSql, Type};
use postgres_types::Kind;
use postgres_types::Kind::Enum;
use serde_json;
use tree_magic;
// use r2d2_postgres::PostgresConnectionManager;
//use crate::*;
use self::interval::PgInterval;
use self::numeric::PgNumeric;

mod column_info;
#[allow(unused)]
mod interval;
mod numeric;
mod table_info;

pub fn init_connection(db_url: &str) -> Client {
    let conn = Client::connect(db_url, NoTls).unwrap();

    conn
}

// pub fn init_pool(
//     db_url: &str,
// ) -> Result<r2d2::Pool<r2d2_postgres::PostgresConnectionManager<NoTls>>, PostgresError> {
//     // test_connection(db_url)?;
//     let manager = r2d2_postgres::PostgresConnectionManager::new(db_url.parse().unwrap(), NoTls);
//         // .map_err(|e| PostgresError::SqlError(e, "Connection Manager Error".into()))?;
//     let pool = r2d2::Pool::new(manager)?;
//     Ok(pool)
// }

// pub fn test_connection(db_url: &str) -> Result<(), PostgresError> {
//     let manager = r2d2_postgres::PostgresConnectionManager::new(db_url.parse().unwrap(), NoTls);
//         // .map_err(|e| PostgresError::SqlError(e, "Connection Manager Error".into()))?;
//     let mut conn = manager
//         .connect()
//         .map_err(|e| PostgresError::SqlError(e, "Connect Error".into()))?;
//     manager
//         .is_valid(&mut conn)
//         .map_err(|e| PostgresError::SqlError(e, "Invalid Connection".into()))?;
//     Ok(())
// }


//#[allow(unused)]
//fn get_tls() -> TlsMode {
//    let mut builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
//    builder
//        .set_ca_file("/etc/ssl/certs/ca-certificates.crt")
//        .unwrap();
//    let negotiator = OpenSsl::from(builder.build());
//    TlsMode::Require(Box::new(negotiator))
//}

pub struct PostgresDB(pub RefCell<postgres::Client>);
// pub struct PostgresDB(pub r2d2::Pool<PostgresConnectionManager<NoTls>>);

impl Database for PostgresDB {
    fn execute_sql_with_return(&self, sql: &str, param: &[&Value]) -> Result<Rows, DbError> {
        let stmt = self.0.borrow_mut().prepare(&sql);
        match stmt {
            Ok(stmt) => {
                let pg_values = to_pg_values(param);
                let sql_types = to_sql_types(&pg_values);
                let rows = self.0.borrow_mut().query(&stmt, sql_types.as_slice());
                match rows {
                    Ok(rows) => {
                        if rows.len() > 0 {
                            let columns = rows[0].columns();
                            let column_names: Vec<String> =
                                columns.iter().map(|c| c.name().to_string()).collect();
                            let mut records = Rows::new(column_names);
                            for r in rows.iter() {
                                let mut record: Vec<Value> = vec![];
                                for (i, column) in columns.iter().enumerate() {
                                    let value: Option<Result<OwnedPgValue, postgres::Error>> =
                                        Some(r.try_get(i));
                                    match value {
                                        Some(value) => {
                                            match value {
                                                Ok(value) => record.push(value.0),
                                                Err(e) => {
                                                    //info!("Row {:?}", r);
                                                    info!("column {:?} index: {}", column, i);
                                                    let msg = format!(
                                                        "Error converting column {:?} at index {}",
                                                        column, i
                                                    );
                                                    return Err(DbError::PlatformError(
                                                        PlatformError::PostgresError(
                                                            PostgresError::GenericError(msg, e),
                                                        ),
                                                    ));
                                                }
                                            }
                                        }
                                        None => {
                                            record.push(Value::Nil); // Note: this is important to not mess the spacing of records
                                        }
                                    }
                                }
                                records.push(record);
                            }
                            Ok(records)
                        } else {
                            Ok(Rows::new(vec![]))
                        }
                    }
                    Err(e) => Err(DbError::PlatformError(PlatformError::PostgresError(
                        PostgresError::SqlError(e, sql.to_string()),
                    ))),
                }
            }
            Err(e) => Err(DbError::PlatformError(PlatformError::PostgresError(
                PostgresError::SqlError(e, sql.to_string()),
            ))),
        }
    }

    fn get_table(&self, em: &EntityManager, table_name: &TableName) -> Result<Table, DbError> {
        table_info::get_table(em, table_name)
    }

    fn get_all_tables(&self, em: &EntityManager) -> Result<Vec<Table>, DbError> {
        table_info::get_all_tables(em)
    }

    fn get_grouped_tables(&self, em: &EntityManager) -> Result<Vec<SchemaContent>, DbError> {
        table_info::get_organized_tables(em)
    }

    /// get the list of database users
    fn get_users(&self, em: &EntityManager) -> Result<Vec<User>, DbError> {
        let sql = "SELECT oid::int AS sysid,
               rolname AS username,
               rolsuper AS is_superuser,
               rolinherit AS is_inherit,
               rolcreaterole AS can_create_role,
               rolcreatedb AS can_create_db,
               rolcanlogin AS can_login,
               rolreplication AS can_do_replication,
               rolbypassrls AS can_bypass_rls,
               CASE WHEN rolconnlimit < 0 THEN NULL
                    ELSE rolconnlimit END AS conn_limit,
               '*************' AS password,
               CASE WHEN rolvaliduntil = 'infinity'::timestamp THEN NULL
                   ELSE rolvaliduntil
                   END AS valid_until
               FROM pg_authid";
        em.execute_sql_with_return(&sql, &[])
    }

    /// get the list of roles for this user
    fn get_roles(&self, em: &EntityManager, username: &str) -> Result<Vec<Role>, DbError> {
        let sql = "SELECT
            (SELECT rolname FROM pg_roles WHERE oid = m.roleid) AS role_name
            FROM pg_auth_members m
            LEFT JOIN pg_roles
            ON m.member = pg_roles.oid
            WHERE pg_roles.rolname = $1
        ";
        em.execute_sql_with_return(&sql, &[&username.to_owned()])
    }

    fn get_database_name(&self, em: &EntityManager) -> Result<Option<DatabaseName>, DbError> {
        let sql = "SELECT current_database() AS name,
                        description FROM pg_database
                        LEFT JOIN pg_shdescription ON objoid = pg_database.oid
                        WHERE datname = current_database()";
        em.execute_sql_with_one_return(&sql, &[]).map(Some)
    }
}

fn to_pg_values<'a>(values: &[&'a Value]) -> Vec<PgValue<'a>> {
    values.iter().map(|v| PgValue(v)).collect()
}

fn to_sql_types<'a>(values: &'a [PgValue]) -> Vec<&'a (dyn ToSql + Sync)> {
    let mut sql_types = vec![];
    for v in values.iter() {
        sql_types.push(&*v as &(dyn ToSql + Sync));
    }
    sql_types
}

/// need to wrap Value in order to be able to implement ToSql trait for it
/// both of which are defined from some other traits
/// otherwise: error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
/// For inserting, implement only ToSql
#[derive(Debug)]
pub struct PgValue<'a>(&'a Value);

/// need to wrap Value in order to be able to implement ToSql trait for it
/// both of which are defined from some other traits
/// otherwise: error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
/// For retrieval, implement only FromSql
#[derive(Debug)]
pub struct OwnedPgValue(Value);

impl<'a> ToSql for PgValue<'a> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Sync + Send>> {
        match *self.0 {
            Value::Bool(ref v) => v.to_sql(ty, out),
            Value::Tinyint(ref v) => v.to_sql(ty, out),
            Value::Smallint(ref v) => v.to_sql(ty, out),
            Value::Int(ref v) => v.to_sql(ty, out),
            Value::Bigint(ref v) => v.to_sql(ty, out),
            Value::Float(ref v) => v.to_sql(ty, out),
            Value::Double(ref v) => v.to_sql(ty, out),
            Value::Blob(ref v) => v.to_sql(ty, out),
            Value::ImageUri(ref _v) => {
                panic!("ImageUri is only used for reading data from DB, not inserting into DB")
            }
            Value::Char(ref v) => v.to_string().to_sql(ty, out),
            Value::Text(ref v) => v.to_sql(ty, out),
            Value::Uuid(ref v) => v.to_sql(ty, out),
            Value::Date(ref v) => v.to_sql(ty, out),
            Value::Timestamp(ref v) => v.to_sql(ty, out),
            Value::DateTime(ref v) => v.to_sql(ty, out),
            Value::Time(ref v) => v.to_sql(ty, out),
            Value::Interval(ref _v) => panic!("storing interval in DB is not supported"),
            Value::BigDecimal(ref v) => {
                let numeric: PgNumeric = v.into();
                numeric.to_sql(ty, out)
            }
            Value::Json(ref v) => v.to_sql(ty, out),
            Value::Point(ref v) => v.to_sql(ty, out),
            Value::Array(ref v) => match *v {
                Array::Text(ref av) => av.to_sql(ty, out),
                Array::Int(ref av) => av.to_sql(ty, out),
                Array::Float(ref av) => av.to_sql(ty, out),
            },
            Value::Nil => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for OwnedPgValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        macro_rules! match_type {
            ($variant:ident) => {
                FromSql::from_sql(ty, raw).map(|v| OwnedPgValue(Value::$variant(v)))
            };
        }
        let kind = ty.kind();
        match *kind {
            Enum(_) => match_type!(Text),
            Kind::Array(ref array_type) => {
                let array_type_kind = array_type.kind();
                match *array_type_kind {
                    Enum(_) => FromSql::from_sql(ty, raw)
                        .map(|v| OwnedPgValue(Value::Array(Array::Text(v)))),
                    _ => match *ty {
                        types::Type::TEXT_ARRAY
                        | types::Type::NAME_ARRAY
                        | types::Type::VARCHAR_ARRAY => FromSql::from_sql(ty, raw)
                            .map(|v| OwnedPgValue(Value::Array(Array::Text(v)))),
                        types::Type::INT4_ARRAY => FromSql::from_sql(ty, raw)
                            .map(|v| OwnedPgValue(Value::Array(Array::Int(v)))),
                        types::Type::FLOAT4_ARRAY => FromSql::from_sql(ty, raw)
                            .map(|v| OwnedPgValue(Value::Array(Array::Float(v)))),
                        _ => panic!("Array type {:?} is not yet covered", array_type),
                    },
                }
            }
            Kind::Simple => {
                match *ty {
                    types::Type::BOOL => match_type!(Bool),
                    types::Type::INT2 => match_type!(Smallint),
                    types::Type::INT4 => match_type!(Int),
                    types::Type::INT8 => match_type!(Bigint),
                    types::Type::FLOAT4 => match_type!(Float),
                    types::Type::FLOAT8 => match_type!(Double),
                    types::Type::TEXT
                    | types::Type::VARCHAR
                    | types::Type::NAME
                    | types::Type::UNKNOWN => {
                        match_type!(Text)
                    }
                    types::Type::TS_VECTOR => {
                        let text = String::from_utf8(raw.to_owned());
                        match text {
                            Ok(text) => Ok(OwnedPgValue(Value::Text(text))),
                            Err(e) => Err(Box::new(PostgresError::FromUtf8Error(e))),
                        }
                    }
                    types::Type::BPCHAR => {
                        let v: Result<String, _> = FromSql::from_sql(&types::Type::TEXT, raw);
                        match v {
                            Ok(v) => {
                                // TODO: Need to unify char and character array in one Value::Text
                                // variant to simplify handling them in some column
                                if v.chars().count() == 1 {
                                    Ok(OwnedPgValue(Value::Char(v.chars().next().unwrap())))
                                } else {
                                    FromSql::from_sql(ty, raw).map(|v: String| {
                                        let value_string: String = v.trim_end().to_string();
                                        OwnedPgValue(Value::Text(value_string))
                                    })
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    types::Type::UUID => match_type!(Uuid),
                    types::Type::DATE => match_type!(Date),
                    types::Type::TIMESTAMPTZ | types::Type::TIMESTAMP => match_type!(Timestamp),
                    types::Type::TIME | types::Type::TIMETZ => match_type!(Time),
                    types::Type::BYTEA => {
                        let mime_type = tree_magic::from_u8(raw);
                        info!("mime_type: {}", mime_type);
                        let bytes: Vec<u8> = FromSql::from_sql(ty, raw).unwrap();
                        //assert_eq!(raw, &*bytes);
                        let base64 = base64::encode_config(&bytes, base64::MIME);
                        match &*mime_type {
                            "image/jpeg" | "image/png" => Ok(OwnedPgValue(Value::ImageUri(
                                format!("data:{};base64,{}", mime_type, base64),
                            ))),
                            _ => match_type!(Blob),
                        }
                    }
                    types::Type::NUMERIC => {
                        let numeric: PgNumeric = FromSql::from_sql(ty, raw)?;
                        let bigdecimal = BigDecimal::from(numeric);
                        Ok(OwnedPgValue(Value::BigDecimal(bigdecimal)))
                    }
                    types::Type::JSON | types::Type::JSONB => {
                        let value: serde_json::Value = FromSql::from_sql(ty, raw)?;
                        let text = serde_json::to_string(&value).unwrap();
                        Ok(OwnedPgValue(Value::Json(text)))
                    }
                    types::Type::INTERVAL => {
                        let pg_interval: PgInterval = FromSql::from_sql(ty, raw)?;
                        let interval = Interval::new(
                            pg_interval.microseconds,
                            pg_interval.days,
                            pg_interval.months,
                        );
                        Ok(OwnedPgValue(Value::Interval(interval)))
                    }
                    types::Type::POINT => {
                        let p: Point<f64> = FromSql::from_sql(ty, raw)?;
                        Ok(OwnedPgValue(Value::Point(p)))
                    }
                    types::Type::INET => {
                        info!("inet raw:{:?}", raw);
                        match_type!(Text)
                    }
                    _ => panic!("unable to convert from {:?}", ty),
                }
            }
            _ => panic!("not yet handling this kind: {:?}", kind),
        }
    }
    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(OwnedPgValue(Value::Nil))
    }
    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&[u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }
}

#[derive(Debug)]
pub enum PostgresError {
    GenericError(String, postgres::Error),
    // PoolError(String),
    SqlError(postgres::Error, String),
    ConvertStringToCharError(String),
    FromUtf8Error(FromUtf8Error),
    ConvertNumericToBigDecimalError,
}

impl From<postgres::Error> for PostgresError {
    fn from(e: postgres::Error) -> Self {
        PostgresError::GenericError("From conversion".into(), e)
    }
}

// impl From<r2d2::Error> for PostgresError {
//     fn from(e: r2d2::Error) -> Self {
//         PostgresError::PoolError("From conversion".into())
//     }
// }

impl Error for PostgresError {}

impl fmt::Display for PostgresError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[cfg(test)]
mod test {

    use crate::pool::*;
    use crate::Pool;
    use crate::*;
    use log::*;
    use postgres::Client;
    use std::ops::Deref;

    #[test]
    fn test_character_array_data_type() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let dm = pool.dm(db_url).unwrap();
        let sql = format!("SELECT language_id, name FROM language",);
        let languages: Result<Rows, DbError> = dm.execute_sql_with_return(&sql, &[]);
        println!("languages: {:#?}", languages);
        assert!(languages.is_ok());
    }

    #[test]
    fn test_ts_vector() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let dm = pool.dm(db_url).unwrap();
        let sql = format!("SELECT film_id, title, fulltext::text FROM film LIMIT 40",);
        let films: Result<Rows, DbError> = dm.execute_sql_with_return(&sql, &[]);
        println!("film: {:#?}", films);
        assert!(films.is_ok());
    }
    #[test]
    fn connect_test_query() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn {
            PooledConn::PooledPg(ref pooled_pg) => {
                let rows = pooled_pg.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter() {
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
            #[cfg(any(feature = "with-sqlite"))]
            _ => unreachable!(),
        }
    }
    #[test]
    fn connect_test_query_explicit_deref() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn {
            PooledConn::PooledPg(ref pooled_pg) => {
                let c: &Client = pooled_pg.deref(); //explicit deref here
                let rows = c.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter() {
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
            #[cfg(any(feature = "with-sqlite"))]
            _ => unreachable!(),
        }
    }
    #[test]
    fn test_unknown_type() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec!["hi".into(), true.into(), 42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = (&db).execute_sql_with_return(
            "select 'Hello', $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT",
            &bvalues,
        );
        info!("rows: {:#?}", rows);
        assert!(rows.is_ok());
    }
    #[test]
    // only text can be inferred to UNKNOWN types
    fn test_unknown_type_i32_f32() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec![42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = (&db).execute_sql_with_return("select $1, $2", &bvalues);
        info!("rows: {:#?}", rows);
        assert!(!rows.is_ok());
    }

    #[test]
    fn using_values() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec!["hi".into(), true.into(), 42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = (&db).execute_sql_with_return(
            "select 'Hello'::TEXT, $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT",
            &bvalues,
        );
        info!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter() {
                info!("row {:?}", row);
                let v4: Result<f64, _> = row.get("float8");
                assert_eq!(v4.unwrap(), 1.0f64);

                let v3: Result<i32, _> = row.get("int4");
                assert_eq!(v3.unwrap(), 42i32);

                let hi: Result<String, _> = row.get("text");
                assert_eq!(hi.unwrap(), "hi");

                let b: Result<bool, _> = row.get("bool");
                assert_eq!(b.unwrap(), true);
            }
        }
    }

    #[test]
    fn with_nulls() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db = pool.db(db_url).unwrap();
        let rows:Result<Rows, DbError> = (&db).execute_sql_with_return("select 'rust'::TEXT AS name, NULL::TEXT AS schedule, NULL::TEXT AS specialty from actor", &[]);
        info!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter() {
                info!("row {:?}", row);
                let name: Result<Option<String>, _> = row.get("name");
                info!("name: {:?}", name);
                assert_eq!(name.unwrap().unwrap(), "rust");

                let schedule: Result<Option<String>, _> = row.get("schedule");
                info!("schedule: {:?}", schedule);
                assert_eq!(schedule.unwrap(), None);

                let specialty: Result<Option<String>, _> = row.get("specialty");
                info!("specialty: {:?}", specialty);
                assert_eq!(specialty.unwrap(), None);
            }
        }
    }

    #[test]
    fn test_get_users() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let em = pool.em(db_url).unwrap();
        let users = em.get_users();
        info!("users: {:#?}", users);
        assert!(users.is_ok());
    }
}
