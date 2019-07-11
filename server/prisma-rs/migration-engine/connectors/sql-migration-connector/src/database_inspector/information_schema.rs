use super::database_inspector_impl::IntrospectedColumn;
use prisma_query::ast::ParameterizedValue;
use prisma_query::connector::Database;
use std::sync::Arc;

pub struct InformationSchema {
    pub database: Arc<Database>,
    pub data_type_column: String,
}

impl InformationSchema {
    pub fn get_table_names(&self, schema: &str) -> Vec<String> {
        let sql = format!(
            r#"
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
            table_schema = '{}' AND
            -- Views are not supported yet
            table_type = 'BASE TABLE'
        "#,
            schema
        );

        let result_set = self.database.query_on_raw_connection(&schema, &sql, &[]).unwrap();

        result_set
            .into_iter()
            .map(|row| row[0].into_string().unwrap())
            .collect()
    }

    pub fn get_columns(&self, schema: &String, table: &String) -> Vec<IntrospectedColumn> {
        let sql = format!(
            r#"
            SELECT
                cols.ordinal_position,
                cols.column_name,
                cols.{} AS data_type,
                cols.column_default,
                cols.is_nullable = 'YES' as is_nullable
            FROM
                information_schema.columns AS cols
            WHERE
                cols.table_schema = '{}'
                AND cols.table_name  = '{}'
        "#,
            self.data_type_column, schema, table
        );

        let result_set = self.database.query_on_raw_connection(&schema, &sql, &[]).unwrap();
        let columns = result_set
            .into_iter()
            .map(|row| {
                let default_value = match row["column_default"] {
                    ParameterizedValue::Text(v) => Some(v.to_string()),
                    ParameterizedValue::Null => None,
                    p => panic!(format!("expectd a string value but got {:?}", p)),
                };
                IntrospectedColumn {
                    name: row["column_name"].into_string().unwrap(),
                    table: table.to_string(),
                    tpe: row["data_type"].into_string().unwrap(),
                    is_required: !row["is_nullable"].as_bool().unwrap(),
                    default: default_value,
                    pk: 1 as u32, // TODO: implement foreign key llokup
                }
            })
            .collect();

        columns
    }

    pub fn get_primary_keys(&self, schema: &str, table: &str) -> Vec<String> {
        let sql = format!(
            r#"
            SELECT kcu.table_schema,
                kcu.table_name,
                tco.constraint_name,
                kcu.ordinal_position as position,
                kcu.column_name as key_column
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
                ON kcu.constraint_name = tco.constraint_name
                AND kcu.constraint_schema = tco.constraint_schema
                AND kcu.constraint_name = tco.constraint_name
            WHERE tco.constraint_type = 'PRIMARY KEY'
                AND kcu.table_schema = '{}'
                AND kcu.table_name = '{}'
            ORDER BY kcu.table_schema,
                    kcu.table_name,
                    position;
        "#,
            schema, table
        );

        let result_set = self.database.query_on_raw_connection(&schema, &sql, &[]).unwrap();
        let mut pks: Vec<String> = result_set
            .into_iter()
            .map(|row| row["key_column"].into_string().unwrap())
            .collect();
        pks.dedup(); // this query yields duplicates on MySQL
        pks
    }
}
