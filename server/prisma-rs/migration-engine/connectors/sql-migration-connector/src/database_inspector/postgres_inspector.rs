use super::database_inspector_impl::{convert_introspected_columns, IntrospectedForeignKey};
use super::information_schema::InformationSchema;
use super::*;
use prisma_query::connector::Database;
use std::sync::Arc;

pub struct Postgres {
    pub database: Arc<Database>,
    information_schema: InformationSchema,
}

impl DatabaseInspector for Postgres {
    fn introspect(&self, schema: &String) -> DatabaseSchema {
        DatabaseSchema {
            tables: self
                .information_schema
                .get_table_names(schema)
                .into_iter()
                .map(|t| self.get_table(schema, &t))
                .collect(),
        }
    }
}

impl Postgres {
    pub fn new(database: Arc<Database>) -> Postgres {
        Postgres {
            database: Arc::clone(&database),
            information_schema: InformationSchema {
                database,
                data_type_column: "udt_name".to_string(),
            },
        }
    }

    fn get_table(&self, schema: &String, table: &String) -> Table {
        let introspected_columns = self.information_schema.get_columns(&schema, &table);
        let introspected_foreign_keys = self.get_foreign_key_constraints(&schema, &table);
        let primary_key_columns = self.information_schema.get_primary_keys(&schema, &table);
        Table {
            name: table.to_string(),
            columns: convert_introspected_columns(
                introspected_columns,
                introspected_foreign_keys,
                Box::new(column_type),
            ),
            indexes: Vec::new(),
            primary_key_columns: primary_key_columns,
        }
    }

    fn get_foreign_key_constraints(&self, schema: &String, table: &String) -> Vec<IntrospectedForeignKey> {
        let sql = format!(
            r#"
            SELECT
	            kcu.constraint_name as "fkConstraintName",
                kcu.table_name as "fkTableName",
                kcu.column_name as "fkColumnName",
                ccu.table_name as "referencedTableName",
                ccu.column_name as "referencedColumnName"
            FROM
                information_schema.key_column_usage kcu
            INNER JOIN
                information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_catalog = kcu.constraint_catalog
                AND ccu.constraint_schema = kcu.constraint_schema
                AND ccu.constraint_name = kcu.constraint_name
            INNER JOIN
                information_schema.referential_constraints as rc
                ON rc.constraint_catalog = kcu.constraint_catalog
                AND rc.constraint_schema = kcu.constraint_schema
                AND rc.constraint_name = kcu.constraint_name
            WHERE
                kcu.table_schema = '{}' AND
                kcu.table_name = '{}'
        "#,
            schema, table
        );

        let result_set = self.database.query_on_raw_connection(&schema, &sql, &[]).unwrap();
        result_set
            .into_iter()
            .map(|row| IntrospectedForeignKey {
                name: row["fkConstraintName"].into_string().unwrap(),
                table: row["fkTableName"].into_string().unwrap(),
                column: row["fkColumnName"].into_string().unwrap(),
                referenced_table: row["referencedTableName"].into_string().unwrap(),
                referenced_column: row["referencedColumnName"].into_string().unwrap(),
            })
            .collect()
    }
}

fn column_type(column: &IntrospectedColumn) -> ColumnType {
    match column.tpe.as_ref() {
        s if s.starts_with("int") => ColumnType::Int,
        s if s.starts_with("float") => ColumnType::Float,
        "numeric" => ColumnType::Float,
        "bool" => ColumnType::Boolean,
        "text" => ColumnType::String,
        s if s.starts_with("varchar") => ColumnType::String,
        "date" | "timestamp" => ColumnType::DateTime,
        x => panic!(format!(
            "type {} is not supported here yet. Column was: {}",
            x, column.name
        )),
    }
}
