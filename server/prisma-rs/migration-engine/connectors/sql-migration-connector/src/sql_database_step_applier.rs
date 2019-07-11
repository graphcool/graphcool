use crate::*;
use datamodel::Value;
use migration_connector::*;
use prisma_query::connector::Database;
use std::sync::Arc;

pub struct SqlDatabaseStepApplier {
    pub sql_family: SqlFamily,
    pub schema_name: String,
    pub conn: Arc<Database>,
}

#[allow(unused, dead_code)]
impl DatabaseMigrationStepApplier<SqlMigration> for SqlDatabaseStepApplier {
    fn apply_step(&self, database_migration: &SqlMigration, index: usize) -> ConnectorResult<bool> {
        Ok(self.apply_next_step(&database_migration.steps, index)?)
    }

    fn unapply_step(&self, database_migration: &SqlMigration, index: usize) -> ConnectorResult<bool> {
        Ok(self.apply_next_step(&database_migration.rollback, index)?)
    }

    fn render_steps_pretty(&self, database_migration: &SqlMigration) -> ConnectorResult<serde_json::Value> {
        Ok(render_steps_pretty(
            &database_migration,
            self.sql_family,
            &self.schema_name,
        )?)
    }
}

pub struct VirtualSqlDatabaseStepApplier {
    pub sql_family: SqlFamily,
    pub schema_name: String,
}
impl DatabaseMigrationStepApplier<SqlMigration> for VirtualSqlDatabaseStepApplier {
    fn apply_step(&self, _database_migration: &SqlMigration, _index: usize) -> ConnectorResult<bool> {
        unimplemented!("Not allowed on a VirtualSqlDatabaseStepApplier")
    }

    fn unapply_step(&self, _database_migration: &SqlMigration, _index: usize) -> ConnectorResult<bool> {
        unimplemented!("Not allowed on a VirtualSqlDatabaseStepApplier")
    }

    fn render_steps_pretty(&self, database_migration: &SqlMigration) -> ConnectorResult<serde_json::Value> {
        Ok(render_steps_pretty(
            &database_migration,
            self.sql_family,
            &self.schema_name,
        )?)
    }
}

impl SqlDatabaseStepApplier {
    fn apply_next_step(&self, steps: &Vec<SqlMigrationStep>, index: usize) -> SqlResult<bool> {
        let has_this_one = steps.get(index).is_some();
        if !has_this_one {
            return Ok(false);
        }

        let step = &steps[index];
        let sql_string = render_raw_sql(&step, self.sql_family, &self.schema_name);
        dbg!(&sql_string);
        let result = self.conn.query_on_raw_connection(&self.schema_name, &sql_string, &[]);

        // TODO: this does not evaluate the results of SQLites PRAGMA foreign_key_check
        result?;

        let has_more = steps.get(index + 1).is_some();
        Ok(has_more)
    }
}

fn render_steps_pretty(
    database_migration: &SqlMigration,
    sql_family: SqlFamily,
    schema_name: &str,
) -> ConnectorResult<serde_json::Value> {
    let jsons = database_migration
        .steps
        .iter()
        .map(|step| {
            let cloned = step.clone();
            let mut json_value = serde_json::to_value(&step).unwrap();
            let json_object = json_value.as_object_mut().unwrap();
            json_object.insert(
                "raw".to_string(),
                serde_json::Value::String(render_raw_sql(&cloned, sql_family, schema_name)),
            );
            json_value
        })
        .collect();
    Ok(serde_json::Value::Array(jsons))
}

fn render_raw_sql(step: &SqlMigrationStep, sql_family: SqlFamily, schema_name: &str) -> String {
    let schema_name = schema_name.to_string();

    match step {
        SqlMigrationStep::CreateTable(CreateTable {
            name,
            columns,
            primary_columns,
        }) => {
            let cloned_columns = columns.clone();
            let primary_columns = primary_columns.clone();
            let mut lines = Vec::new();
            for column in cloned_columns.clone() {
                let col_sql = render_column(sql_family, schema_name.to_string(), &column, false);
                lines.push(col_sql);
            }
            if primary_columns.len() > 0 {
                let column_names: Vec<String> = primary_columns
                    .clone()
                    .into_iter()
                    .map(|col| quote(&col, sql_family))
                    .collect();
                lines.push(format!("PRIMARY KEY ({})", column_names.join(",")))
            }
            format!(
                "CREATE TABLE {}.{}({});",
                quote(&schema_name, sql_family),
                quote(name, sql_family),
                lines.join(",")
            )
        }
        SqlMigrationStep::DropTable(DropTable { name }) => format!(
            "DROP TABLE {}.{};",
            quote(&schema_name, sql_family),
            quote(name, sql_family)
        ),
        SqlMigrationStep::DropTables(DropTables { names }) => {
            let fully_qualified_names: Vec<String> = names
                .iter()
                .map(|name| format!("{}.{}", quote(&schema_name, sql_family), quote(name, sql_family)))
                .collect();
            format!("DROP TABLE {};", fully_qualified_names.join(","))
        }
        SqlMigrationStep::RenameTable { name, new_name } => {
            let new_name = match sql_family {
                SqlFamily::Sqlite => format!("{}", quote(new_name, sql_family)),
                _ => format!("{}.{}", quote(&schema_name, sql_family), quote(new_name, sql_family)),
            };
            format!(
                "ALTER TABLE {}.{} RENAME TO {};",
                quote(&schema_name, sql_family),
                quote(name, sql_family),
                new_name
            )
        }
        SqlMigrationStep::AlterTable(AlterTable { table, changes }) => {
            let mut lines = Vec::new();
            for change in changes.clone() {
                match change {
                    TableChange::AddColumn(AddColumn { column }) => {
                        let col_sql = render_column(sql_family, schema_name.to_string(), &column, true);
                        lines.push(format!("ADD COLUMN {}", col_sql));
                    }
                    TableChange::DropColumn(DropColumn { name }) => {
                        // TODO: this does not work on MySQL for columns with foreign keys. Here the FK must be dropped first by name.
                        let name = quote(&name, sql_family);
                        lines.push(format!("DROP COLUMN {}", name));
                    }
                    TableChange::AlterColumn(AlterColumn { name, column }) => {
                        let name = quote(&name, sql_family);
                        lines.push(format!("DROP COLUMN {}", name));
                        let col_sql = render_column(sql_family, schema_name.to_string(), &column, true);
                        lines.push(format!("ADD COLUMN {}", col_sql));
                    }
                }
            }
            format!(
                "ALTER TABLE {}.{} {};",
                quote(&schema_name, sql_family),
                quote(table, sql_family),
                lines.join(",")
            )
        }
        SqlMigrationStep::CreateIndex(CreateIndex {
            table,
            name,
            tpe,
            columns,
        }) => {
            let index_type = match tpe {
                IndexType::Unique => "UNIQUE",
                IndexType::Normal => "",
            };
            let index_name = match sql_family {
                SqlFamily::Sqlite => format!("{}.{}", quote(&schema_name, sql_family), quote(&name, sql_family)),
                _ => quote(&name, sql_family),
            };
            let table_reference = match sql_family {
                SqlFamily::Sqlite => quote(&table, sql_family),
                _ => format!("{}.{}", quote(&schema_name, sql_family), quote(&table, sql_family)),
            };
            let columns: Vec<String> = columns.iter().map(|c| quote(c, sql_family)).collect();
            format!(
                "CREATE {} INDEX {} ON {}({})",
                index_type,
                index_name,
                table_reference,
                columns.join(",")
            )
        }
        SqlMigrationStep::DropIndex(DropIndex { table, name }) => match sql_family {
            SqlFamily::Mysql => format!(
                "DROP INDEX {} ON {}.{}",
                quote(&name, sql_family),
                quote(&schema_name, sql_family),
                quote(&table, sql_family)
            ),
            SqlFamily::Postgres | SqlFamily::Sqlite => format!(
                "DROP INDEX {}.{}",
                quote(&schema_name, sql_family),
                quote(&name, sql_family)
            ),
        },
        SqlMigrationStep::RawSql { raw } => raw.to_string(),
    }
}

fn quote(name: &str, sql_family: SqlFamily) -> String {
    match sql_family {
        SqlFamily::Sqlite => format!("\"{}\"", name),
        SqlFamily::Postgres => format!("\"{}\"", name),
        SqlFamily::Mysql => format!("`{}`", name),
    }
}

fn render_column(
    sql_family: SqlFamily,
    schema_name: String,
    column_description: &ColumnDescription,
    add_fk_prefix: bool,
) -> String {
    let column_name = quote(&column_description.name, sql_family);
    let tpe_str = render_column_type(sql_family, column_description.tpe);
    let nullability_str = if column_description.required { "NOT NULL" } else { "" };
    let default_str = match &column_description.default {
        Some(value) => {
            match render_value(value) {
                Some(ref default) if column_description.required => format!("DEFAULT {}", default),
                Some(_) => "".to_string(), // we use the default value right now only to smoothen migrations. So we only use it when absolutely needed.
                None => "".to_string(),
            }
        }
        None => "".to_string(),
    };
    let references_str = match (sql_family, &column_description.foreign_key) {
        (SqlFamily::Postgres, Some(fk)) => {
            format!("REFERENCES \"{}\".\"{}\"(\"{}\")", schema_name, fk.table, fk.column)
        }
        (SqlFamily::Mysql, Some(fk)) => format!("REFERENCES `{}`.`{}`(`{}`)", schema_name, fk.table, fk.column),
        (SqlFamily::Sqlite, Some(fk)) => format!("REFERENCES {}({})", fk.table, fk.column),
        (_, None) => "".to_string(),
    };
    match (sql_family, &column_description.foreign_key) {
        (SqlFamily::Mysql, Some(_)) => {
            let add = if add_fk_prefix { "ADD" } else { "" };
            let fk_line = format!("{} FOREIGN KEY ({}) {}", add, column_name, references_str);
            format!(
                "{} {} {} {},{}",
                column_name, tpe_str, nullability_str, default_str, fk_line
            )
        }
        _ => format!(
            "{} {} {} {} {}",
            column_name, tpe_str, nullability_str, default_str, references_str
        ),
    }
}

// TODO: this returns None for expressions
fn render_value(value: &Value) -> Option<String> {
    match value {
        Value::Boolean(x) => Some(if *x { "true".to_string() } else { "false".to_string() }),
        Value::Int(x) => Some(format!("{}", x)),
        Value::Float(x) => Some(format!("{}", x)),
        Value::Decimal(x) => Some(format!("{}", x)),
        Value::String(x) => Some(format!("'{}'", x)),

        Value::DateTime(x) => {
            let mut raw = format!("{}", x); // this will produce a String 1970-01-01 00:00:00 UTC
            raw.truncate(raw.len() - 4); // strip the UTC suffix
            Some(format!("'{}'", raw)) // add quotes
        }
        Value::ConstantLiteral(x) => Some(format!("'{}'", x)), // this represents enum values
        _ => None,
    }
}

// TODO: this must become database specific akin to our TypeMappers in Scala
fn render_column_type(sql_family: SqlFamily, t: ColumnType) -> String {
    match sql_family {
        SqlFamily::Sqlite => render_column_type_sqlite(t),
        SqlFamily::Postgres => render_column_type_postgres(t),
        SqlFamily::Mysql => render_column_type_mysql(t),
    }
}

fn render_column_type_sqlite(t: ColumnType) -> String {
    match t {
        ColumnType::Boolean => format!("BOOLEAN"),
        ColumnType::DateTime => format!("DATE"),
        ColumnType::Float => format!("REAL"),
        ColumnType::Int => format!("INTEGER"),
        ColumnType::String => format!("TEXT"),
    }
}

fn render_column_type_postgres(t: ColumnType) -> String {
    match t {
        ColumnType::Boolean => format!("boolean"),
        ColumnType::DateTime => format!("timestamp(3)"),
        ColumnType::Float => format!("Decimal(65,30)"),
        ColumnType::Int => format!("integer"),
        ColumnType::String => format!("text"),
    }
}

fn render_column_type_mysql(t: ColumnType) -> String {
    match t {
        ColumnType::Boolean => format!("boolean"),
        ColumnType::DateTime => format!("datetime(3)"),
        ColumnType::Float => format!("Decimal(65,30)"),
        ColumnType::Int => format!("int"),
        ColumnType::String => format!("varchar(1000)"), // we use varchar right now as mediumtext doesn't allow default values
    }
}
