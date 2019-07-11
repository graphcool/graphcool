#![allow(dead_code)]

mod database_inspector_impl;
mod database_schema;
mod empty_impl;
mod information_schema;
mod mysql_inspector;
mod postgres_inspector;
mod sqlite_inspector;

pub use database_inspector_impl::*;
pub use database_schema::*;
pub use empty_impl::*;
use mysql_inspector::MysqlInspector;
use postgres::Config as PostgresConfig;
use postgres_inspector::Postgres;
use prisma_query::connector::{Database, Mysql as MysqlDriver, PostgreSql as PostgresDriver, Sqlite as SqliteDriver};
use sqlite_inspector::Sqlite;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub trait DatabaseInspector {
    fn introspect(&self, schema: &String) -> DatabaseSchema;
}

impl DatabaseInspector {
    pub fn empty() -> EmptyDatabaseInspectorImpl {
        EmptyDatabaseInspectorImpl {}
    }

    pub fn sqlite(file_path: String) -> Sqlite {
        let connection_limit = 5;
        let test_mode = false;
        let conn = Arc::new(SqliteDriver::new(file_path, connection_limit, test_mode).unwrap());
        Self::sqlite_with_database(conn)
    }

    pub fn sqlite_with_database(database: Arc<Database>) -> Sqlite {
        Sqlite::new(database)
    }

    pub fn postgres(url: String) -> Postgres {
        // TODO: this needs to move into prisma-query
        // TODO: the setup calls in here look wrong
        let parsed_url = Url::parse(&url).expect("Parsing of the provided connector url failed.");
        let connection_limit = 5;
        let mut config = PostgresConfig::new();
        if let Some(host) = parsed_url.host_str() {
            config.host(host);
        }
        config.user(parsed_url.username());
        if let Some(password) = parsed_url.password() {
            config.password(password);
        }
        let mut db_name = parsed_url.path().to_string();
        db_name.replace_range(..1, ""); // strip leading slash
        config.connect_timeout(Duration::from_secs(5));

        let root_connection = Arc::new(PostgresDriver::new(config.clone(), 1).unwrap());
        let db_sql = format!("CREATE DATABASE \"{}\";", &db_name);
        let schema_name = parsed_url
            .query_pairs()
            .into_iter()
            .find(|qp| qp.0 == Cow::Borrowed("schema"))
            .expect("schema param is missing")
            .1
            .to_string();
        let _ = root_connection.query_on_raw_connection(&schema_name, &db_sql, &[]); // ignoring errors as there's no CREATE DATABASE IF NOT EXISTS in Postgres

        config.dbname(&db_name);

        let schema_connection = Arc::new(PostgresDriver::new(config, connection_limit).unwrap());
        let schema_sql = dbg!(format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", &schema_name));
        schema_connection
            .query_on_raw_connection(&schema_name, &schema_sql, &[])
            .expect("Creation of Postgres Schema failed");
        Postgres::new(schema_connection)
    }

    pub fn postgres_with_database(database: Arc<Database>) -> Postgres {
        Postgres::new(database)
    }

    pub fn mysql(url: String) -> MysqlInspector {
        let database = MysqlDriver::new_from_url(&url).unwrap();
        Self::mysql_with_database(Arc::new(database))
    }

    pub fn mysql_with_database(database: Arc<Database>) -> MysqlInspector {
        MysqlInspector::new(database)
    }
}
