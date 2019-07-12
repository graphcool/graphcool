pub mod database_inspector;
mod database_schema_calculator;
mod database_schema_differ;
mod error;
mod sql_database_migration_inferrer;
mod sql_database_step_applier;
mod sql_destructive_changes_checker;
mod sql_migration;
mod sql_migration_persistence;

use database_inspector::DatabaseInspector;
pub use error::*;
use migration_connector::*;
use postgres::Config as PostgresConfig;
use prisma_query::connector::{Database, Mysql, PostgreSql, Sqlite};
use serde_json;
use sql_database_migration_inferrer::*;
use sql_database_step_applier::*;
use sql_destructive_changes_checker::*;
pub use sql_migration::*;
use sql_migration_persistence::*;
use std::borrow::Cow;
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

#[allow(unused, dead_code)]
pub struct SqlMigrationConnector {
    pub file_path: Option<String>,
    pub sql_family: SqlFamily,
    pub schema_name: String,
    pub database: Arc<Database>,
    pub migration_persistence: Arc<MigrationPersistence>,
    pub database_migration_inferrer: Arc<DatabaseMigrationInferrer<SqlMigration>>,
    pub database_migration_step_applier: Arc<DatabaseMigrationStepApplier<SqlMigration>>,
    pub destructive_changes_checker: Arc<DestructiveChangesChecker<SqlMigration>>,
    pub database_inspector: Arc<DatabaseInspector>,
}

#[derive(Copy, Clone, PartialEq)]
pub enum SqlFamily {
    Sqlite,
    Postgres,
    Mysql,
}

impl SqlFamily {
    fn connector_type_string(&self) -> &'static str {
        match self {
            SqlFamily::Postgres => "postgresql",
            SqlFamily::Mysql => "mysql",
            SqlFamily::Sqlite => "sqlite",
        }
    }
}

impl SqlMigrationConnector {
    #[allow(unused)]
    pub fn exists(sql_family: SqlFamily, url: &str) -> bool {
        match sql_family {
            SqlFamily::Sqlite => {
                let sqlite = Sqlite::try_from(url).expect("Loading SQLite failed");
                sqlite.does_file_exist()
            }
            SqlFamily::Postgres => {
                let postgres_helper = Self::postgres_helper(&url);
                let check_sql = format!(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}';",
                    postgres_helper.schema
                );
                let result_set = postgres_helper
                    .db_connection
                    .query_on_raw_connection("", &check_sql, &[]);
                result_set.into_iter().next().is_some()
            }
            SqlFamily::Mysql => {
                // TODO: implement this actually
                false
            }
        }
    }

    pub fn new(sql_family: SqlFamily, url: &str) -> Arc<MigrationConnector<DatabaseMigration = SqlMigration>> {
        match sql_family {
            SqlFamily::Sqlite => {
                assert!(url.starts_with("file:"), "the url for sqlite must start with 'file:'");
                let conn = Arc::new(Sqlite::try_from(url).expect("Loading SQLite failed"));
                let schema_name = "lift".to_string();
                let file_path = url.trim_start_matches("file:").to_string();
                Self::create_connector(conn, sql_family, schema_name, Some(file_path))
            }
            SqlFamily::Postgres => {
                assert!(
                    url.starts_with("postgresql:"),
                    "the url for postgres must start with 'postgresql:'"
                );
                let postgres_helper = Self::postgres_helper(&url);
                Self::create_connector(postgres_helper.db_connection, sql_family, postgres_helper.schema, None)
            }
            SqlFamily::Mysql => {
                assert!(url.starts_with("mysql:"), "the url for mysql must start with 'mysql:'");
                let helper = Self::mysql_helper(&url);
                Self::create_connector(helper.db_connection, sql_family, helper.schema, None)
            }
        }
    }

    pub fn postgres_helper(url: &str) -> DatabaseHelper {
        let parsed_url = Url::parse(url).expect("Parsing of the provided connector url failed.");
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

        match PostgreSql::new(config.clone(), 1) {
            Ok(root_connection) => {
                let db_sql = format!("CREATE DATABASE \"{}\";", &db_name);
                let _ = root_connection.query_on_raw_connection("", &db_sql, &[]); // ignoring errors as there's no CREATE DATABASE IF NOT EXISTS in Postgres
            }
            Err(_) => {
                // this means that the user did not have access to the root database
            }
        }

        let schema = parsed_url
            .query_pairs()
            .into_iter()
            .find(|qp| qp.0 == Cow::Borrowed("schema"))
            .map(|pair| pair.1.to_string())
            .unwrap_or("public".to_string());

        let connection_limit = parsed_url
            .query_pairs()
            .into_iter()
            .find(|qp| qp.0 == Cow::Borrowed("connection_limit"))
            .map(|pair| {
                let as_int: u32 = pair.1.parse().expect("connection_limit parameter was not an int");
                as_int
            })
            .unwrap_or(1);

        config.dbname(&db_name);
        let db_connection = Arc::new(PostgreSql::new(config, connection_limit).expect("Connecting to Postgres failed"));

        DatabaseHelper { db_connection, schema }
    }

    pub fn mysql_helper(url: &str) -> DatabaseHelper {
        let mut builder = mysql::OptsBuilder::new();
        let parsed_url = Url::parse(url).expect("url parsing failed");

        builder.ip_or_hostname(parsed_url.host_str());
        builder.tcp_port(parsed_url.port().unwrap_or(3306));
        builder.user(Some(parsed_url.username()));
        builder.pass(parsed_url.password());
        builder.verify_peer(false);
        builder.stmt_cache_size(Some(1000));

        let db_name = parsed_url
            .path_segments()
            .and_then(|mut segments| segments.next())
            .expect("db name must be set");

        let root_connection = Mysql::new(builder);
        match root_connection {
            Ok(root_connection) => {
                let db_sql = format!("CREATE SCHEMA IF NOT EXISTS `{}`;", &db_name);
                root_connection
                    .query_on_raw_connection("", &db_sql, &[])
                    .expect("Creating the schema failed");
            }
            Err(_) => {
                // this means that the user did not have root access
            }
        }
        let mysql = Mysql::new_from_url(&url).expect("Connecting to MySQL failed");

        DatabaseHelper {
            db_connection: Arc::new(mysql),
            schema: db_name.to_string(),
        }
    }

    pub fn virtual_variant(
        sql_family: SqlFamily,
        url: &str,
    ) -> Arc<MigrationConnector<DatabaseMigration = SqlMigration>> {
        println!("Loading a virtual connector!");
        // TODO: duplicated from above
        let path = Path::new(&url);
        let schema_name = path
            .file_stem()
            .expect("file url must contain a file name")
            .to_str()
            .unwrap()
            .to_string();
        Arc::new(VirtualSqlMigrationConnector {
            sql_family: sql_family,
            schema_name: schema_name,
        })
    }

    fn create_connector(
        conn: Arc<Database>,
        sql_family: SqlFamily,
        schema_name: String,
        file_path: Option<String>,
    ) -> Arc<SqlMigrationConnector> {
        let inspector: Arc<DatabaseInspector> = match sql_family {
            SqlFamily::Sqlite => Arc::new(DatabaseInspector::sqlite_with_database(Arc::clone(&conn))),
            SqlFamily::Postgres => Arc::new(DatabaseInspector::postgres_with_database(Arc::clone(&conn))),
            SqlFamily::Mysql => Arc::new(DatabaseInspector::mysql_with_database(Arc::clone(&conn))),
        };
        let migration_persistence = Arc::new(SqlMigrationPersistence {
            sql_family,
            connection: Arc::clone(&conn),
            schema_name: schema_name.clone(),
            file_path: file_path.clone(),
        });
        let database_migration_inferrer = Arc::new(SqlDatabaseMigrationInferrer {
            sql_family,
            inspector: Arc::clone(&inspector),
            schema_name: schema_name.to_string(),
        });
        let database_migration_step_applier = Arc::new(SqlDatabaseStepApplier {
            sql_family: sql_family,
            schema_name: schema_name.clone(),
            conn: Arc::clone(&conn),
        });
        let destructive_changes_checker = Arc::new(SqlDestructiveChangesChecker {});
        Arc::new(SqlMigrationConnector {
            file_path,
            sql_family,
            schema_name,
            database: Arc::clone(&conn),
            migration_persistence,
            database_migration_inferrer,
            database_migration_step_applier,
            destructive_changes_checker,
            database_inspector: Arc::clone(&inspector),
        })
    }
}

pub struct DatabaseHelper {
    pub db_connection: Arc<Database>,
    pub schema: String,
}

impl MigrationConnector for SqlMigrationConnector {
    type DatabaseMigration = SqlMigration;

    fn connector_type(&self) -> &'static str {
        self.sql_family.connector_type_string()
    }

    fn initialize(&self) -> ConnectorResult<()> {
        match self.sql_family {
            SqlFamily::Sqlite => {
                if let Some(file_path) = &self.file_path {
                    let path_buf = PathBuf::from(&file_path);
                    match path_buf.parent() {
                        Some(parent_directory) => {
                            fs::create_dir_all(parent_directory).expect("creating the database folders failed")
                        }
                        None => {}
                    }
                }
            }
            SqlFamily::Postgres => {
                let schema_sql = dbg!(format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", &self.schema_name));
                self.database
                    .query_on_raw_connection(&self.schema_name, &schema_sql, &[])
                    .expect("Creation of Postgres Schema failed");
            }
            SqlFamily::Mysql => {
                let schema_sql = dbg!(format!(
                    "CREATE SCHEMA IF NOT EXISTS `{}` DEFAULT CHARACTER SET latin1;",
                    &self.schema_name
                ));
                self.database
                    .query_on_raw_connection(&self.schema_name, &schema_sql, &[])
                    .expect("Creation of Mysql Schema failed");
            }
        }
        self.migration_persistence.init();
        Ok(())
    }

    fn reset(&self) -> ConnectorResult<()> {
        self.migration_persistence.reset();
        Ok(())
    }

    fn migration_persistence(&self) -> Arc<MigrationPersistence> {
        Arc::clone(&self.migration_persistence)
    }

    fn database_migration_inferrer(&self) -> Arc<DatabaseMigrationInferrer<SqlMigration>> {
        Arc::clone(&self.database_migration_inferrer)
    }

    fn database_migration_step_applier(&self) -> Arc<DatabaseMigrationStepApplier<SqlMigration>> {
        Arc::clone(&self.database_migration_step_applier)
    }

    fn destructive_changes_checker(&self) -> Arc<DestructiveChangesChecker<SqlMigration>> {
        Arc::clone(&self.destructive_changes_checker)
    }

    fn deserialize_database_migration(&self, json: serde_json::Value) -> SqlMigration {
        serde_json::from_value(json).expect("Deserializing the database migration failed.")
    }
}

struct VirtualSqlMigrationConnector {
    sql_family: SqlFamily,
    schema_name: String,
}
impl MigrationConnector for VirtualSqlMigrationConnector {
    type DatabaseMigration = SqlMigration;

    fn connector_type(&self) -> &'static str {
        self.sql_family.connector_type_string()
    }

    fn initialize(&self) -> ConnectorResult<()> {
        Ok(())
    }

    fn reset(&self) -> ConnectorResult<()> {
        Ok(())
    }

    fn migration_persistence(&self) -> Arc<MigrationPersistence> {
        Arc::new(EmptyMigrationPersistence {})
    }

    fn database_migration_inferrer(&self) -> Arc<DatabaseMigrationInferrer<SqlMigration>> {
        Arc::new(VirtualSqlDatabaseMigrationInferrer {
            sql_family: self.sql_family,
            schema_name: self.schema_name.clone(),
        })
    }

    fn database_migration_step_applier(&self) -> Arc<DatabaseMigrationStepApplier<SqlMigration>> {
        Arc::new(VirtualSqlDatabaseStepApplier {
            sql_family: self.sql_family,
            schema_name: self.schema_name.clone(),
        })
    }

    fn destructive_changes_checker(&self) -> Arc<DestructiveChangesChecker<SqlMigration>> {
        Arc::new(EmptyDestructiveChangesChecker::new())
    }

    fn deserialize_database_migration(&self, json: serde_json::Value) -> SqlMigration {
        serde_json::from_value(json).expect("Deserializing the database migration failed.")
    }
}
