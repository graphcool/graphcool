use crate::database_inspector::{DatabaseInspector, DatabaseSchema, Table};
use crate::database_schema_calculator::{DatabaseSchemaCalculator, FieldExtensions, ModelExtensions};
use crate::database_schema_differ::{DatabaseSchemaDiff, DatabaseSchemaDiffer};
use crate::*;
use datamodel::*;
use migration_connector::steps::*;
use migration_connector::*;
use std::sync::Arc;

pub struct SqlDatabaseMigrationInferrer {
    pub sql_family: SqlFamily,
    pub inspector: Arc<DatabaseInspector>,
    pub schema_name: String,
}

impl DatabaseMigrationInferrer<SqlMigration> for SqlDatabaseMigrationInferrer {
    fn infer(
        &self,
        previous: &Datamodel,
        next: &Datamodel,
        steps: &Vec<MigrationStep>,
    ) -> ConnectorResult<SqlMigration> {
        let current_database_schema = self.inspector.introspect(&self.schema_name);
        let expected_database_schema = DatabaseSchemaCalculator::calculate(next)?;
        infer(
            &current_database_schema,
            &expected_database_schema,
            &self.schema_name,
            self.sql_family,
            previous,
            next,
            steps,
        )
    }
}

pub struct VirtualSqlDatabaseMigrationInferrer {
    pub sql_family: SqlFamily,
    pub schema_name: String,
}
impl DatabaseMigrationInferrer<SqlMigration> for VirtualSqlDatabaseMigrationInferrer {
    fn infer(
        &self,
        previous: &Datamodel,
        next: &Datamodel,
        steps: &Vec<MigrationStep>,
    ) -> ConnectorResult<SqlMigration> {
        let current_database_schema = DatabaseSchemaCalculator::calculate(previous)?;
        let expected_database_schema = DatabaseSchemaCalculator::calculate(next)?;
        infer(
            &current_database_schema,
            &expected_database_schema,
            &self.schema_name,
            self.sql_family,
            previous,
            next,
            steps,
        )
    }
}

fn infer(
    current_database_schema: &DatabaseSchema,
    expected_database_schema: &DatabaseSchema,
    schema_name: &str,
    sql_family: SqlFamily,
    previous: &Datamodel,
    next: &Datamodel,
    model_steps: &Vec<MigrationStep>,
) -> ConnectorResult<SqlMigration> {
    let mut db_schema_diff_based = infer_based_on_db_schema_diff(
        &current_database_schema,
        &expected_database_schema,
        schema_name,
        sql_family,
    )?;
    let mut datamodel_diff_based = infer_based_on_datamodel_diff(previous, next, model_steps)?;
    let mut combined_steps = Vec::new();
    let mut combined_rollback = Vec::new();
    combined_steps.append(&mut db_schema_diff_based.steps);
    combined_steps.append(&mut datamodel_diff_based.steps);
    combined_rollback.append(&mut db_schema_diff_based.rollback);
    combined_rollback.append(&mut datamodel_diff_based.rollback);
    Ok(SqlMigration {
        steps: combined_steps,
        rollback: combined_rollback,
    })
}

// TODO: here we infer the migration based on the datamodel diff because the introspection is not fully featured yet. We will switch once introspection is in a good shape.
fn infer_based_on_datamodel_diff(
    previous: &Datamodel,
    next: &Datamodel,
    model_steps: &Vec<MigrationStep>,
) -> ConnectorResult<SqlMigration> {
    let mut steps = Vec::new();
    let mut rollback = Vec::new();
    for step in model_steps {
        match step {
            MigrationStep::CreateField(create_field) => {
                let model = next
                    .models()
                    .find(|m| m.name == create_field.model)
                    .expect("Model for MigrationStep not found");
                let field = model
                    .fields()
                    .find(|f| f.name == create_field.name)
                    .expect("Field for MigrationStep not found");
                let index_name = format!("{}.{}._UNIQUE", model.db_name(), field.db_name());
                if create_field.is_unique {
                    steps.push(SqlMigrationStep::CreateIndex(CreateIndex {
                        table: model.db_name(),
                        name: index_name.clone(),
                        tpe: IndexType::Unique,
                        columns: vec![field.db_name()],
                    }));
                    rollback.push(SqlMigrationStep::DropIndex(DropIndex {
                        table: model.db_name(),
                        name: index_name,
                    }));
                }
            }
            MigrationStep::UpdateField(update_field) => {
                let old_model = previous
                    .models()
                    .find(|m| m.name == update_field.model)
                    .expect("old Model for MigrationStep not found");
                let old_field = old_model
                    .fields()
                    .find(|f| f.name == update_field.name)
                    .expect("old Field for MigrationStep not found");
                let new_model = next
                    .models()
                    .find(|m| m.name == update_field.model)
                    .expect("new Model for MigrationStep not found");
                let new_field = new_model
                    .fields()
                    .find(|f| f.name == update_field.name)
                    .expect("new Field for MigrationStep not found");
                let index_name = format!("{}.{}._UNIQUE", old_model.db_name(), old_field.db_name());

                let drop_index = SqlMigrationStep::DropIndex(DropIndex {
                    table: new_model.db_name(),
                    name: index_name.clone(),
                });
                let create_index = SqlMigrationStep::CreateIndex(CreateIndex {
                    table: old_model.db_name(),
                    name: index_name,
                    tpe: IndexType::Unique,
                    columns: vec![new_field.db_name()],
                });

                match (old_field.is_unique, new_field.is_unique) {
                    (true, false) => {
                        steps.push(drop_index);
                        rollback.push(create_index);
                    }
                    (false, true) => {
                        steps.push(create_index);
                        rollback.push(drop_index);
                    }
                    (_, _) => {}
                }
            }
            _ => {}
        }
    }
    Ok(SqlMigration { steps, rollback })
}

fn infer_based_on_db_schema_diff(
    current: &DatabaseSchema,
    next: &DatabaseSchema,
    schema_name: &str,
    sql_family: SqlFamily,
) -> ConnectorResult<SqlMigration> {
    let steps = infer_database_migration_steps_and_fix(&current, &next, &schema_name, sql_family)?;
    let rollback = infer_database_migration_steps_and_fix(&next, &current, &schema_name, sql_family)?;
    Ok(SqlMigration {
        steps: steps,
        rollback: rollback,
    })
}

fn infer_database_migration_steps_and_fix(
    from: &DatabaseSchema,
    to: &DatabaseSchema,
    schema_name: &str,
    sql_family: SqlFamily,
) -> SqlResult<Vec<SqlMigrationStep>> {
    let diff = DatabaseSchemaDiffer::diff(&from, &to);
    let is_sqlite = sql_family == SqlFamily::Sqlite;

    if is_sqlite {
        fix_stupid_sqlite(diff, &from, &to, &schema_name)
    } else {
        let steps = delay_foreign_key_creation(diff);
        fix_id_column_type_change(&from, &to, schema_name, steps)
    }
}

fn fix_id_column_type_change(
    from: &DatabaseSchema,
    to: &DatabaseSchema,
    _schema_name: &str,
    steps: Vec<SqlMigrationStep>,
) -> SqlResult<Vec<SqlMigrationStep>> {
    let has_id_type_change = steps
        .iter()
        .find(|step| match step {
            SqlMigrationStep::AlterTable(alter_table) => {
                if let Ok(current_table) = from.table(&alter_table.table) {
                    let change_to_id_column = alter_table.changes.iter().find(|c| match c {
                        TableChange::AlterColumn(alter_column) => {
                            let current_column = current_table.column_bang(&alter_column.name);
                            let current_column_type = DatabaseSchemaDiffer::convert_column_type(current_column.tpe);
                            let has_type_changed = current_column_type != alter_column.column.tpe;
                            let is_part_of_pk = current_table.primary_key_columns.contains(&alter_column.name);
                            is_part_of_pk && has_type_changed
                        }
                        _ => false,
                    });
                    change_to_id_column.is_some()
                } else {
                    false
                }
            }
            _ => false,
        })
        .is_some();

    // TODO: There's probably a much more graceful way to handle this. But this would also involve a lot of data loss probably. Let's tackle that after P Day
    if has_id_type_change {
        let mut radical_steps = Vec::new();
        let tables_to_drop: Vec<String> = from
            .tables
            .iter()
            .filter(|t| t.name != "_Migration")
            .map(|t| t.name.clone())
            .collect();
        radical_steps.push(SqlMigrationStep::DropTables(DropTables { names: tables_to_drop }));
        let diff_from_empty = DatabaseSchemaDiffer::diff(&DatabaseSchema::empty(), &to);
        let mut steps_from_empty = delay_foreign_key_creation(diff_from_empty);
        radical_steps.append(&mut steps_from_empty);

        Ok(radical_steps)
    } else {
        Ok(steps)
    }
}

// this function caters for the case that a table gets created that has a foreign key to a table that still needs to be created
// Example: Table A has a reference to Table B and Table B has a reference to Table A.
// We therefore split the creation of foreign key columns into separate steps when the referenced tables are not existing yet.
// FIXME: This does not work with SQLite. A required column might get delayed. SQLite then fails with: "Cannot add a NOT NULL column with default value NULL"
fn delay_foreign_key_creation(mut diff: DatabaseSchemaDiff) -> Vec<SqlMigrationStep> {
    let names_of_tables_that_get_created: Vec<String> = diff.create_tables.iter().map(|t| t.name.clone()).collect();
    let mut extra_alter_tables = Vec::new();

    // This mutates the CreateTables in place to remove the foreign key creation. Instead the foreign key creation is moved into separate AlterTable statements.
    for create_table in diff.create_tables.iter_mut() {
        let mut column_that_need_to_be_done_later_for_this_table = Vec::new();
        for column in &create_table.columns {
            if let Some(ref foreign_key) = column.foreign_key {
                let references_non_existent_table = names_of_tables_that_get_created.contains(&foreign_key.table);
                let is_part_of_primary_key = create_table.primary_columns.contains(&column.name);
                let is_relation_table = create_table.name.starts_with("_"); // todo: this is a very weak check. find a better one

                if references_non_existent_table && !is_part_of_primary_key && !is_relation_table {
                    let change = column.clone();
                    column_that_need_to_be_done_later_for_this_table.push(change);
                }
            }
        }
        // remove columns from the create that will be instead added later
        create_table
            .columns
            .retain(|c| !column_that_need_to_be_done_later_for_this_table.contains(&c));
        let changes = column_that_need_to_be_done_later_for_this_table
            .into_iter()
            .map(|c| TableChange::AddColumn(AddColumn { column: c }))
            .collect();

        let alter_table = AlterTable {
            table: create_table.name.clone(),
            changes: changes,
        };
        if !alter_table.changes.is_empty() {
            extra_alter_tables.push(alter_table);
        }
    }
    diff.alter_tables.append(&mut extra_alter_tables);
    diff.into_steps()
}

fn fix_stupid_sqlite(
    diff: DatabaseSchemaDiff,
    current_database_schema: &DatabaseSchema,
    next_database_schema: &DatabaseSchema,
    schema_name: &str,
) -> SqlResult<Vec<SqlMigrationStep>> {
    let steps = diff.into_steps();
    let mut result = Vec::new();
    for step in steps {
        match step {
            SqlMigrationStep::AlterTable(ref alter_table) if needs_fix(&alter_table) => {
                let current_table = current_database_schema.table(&alter_table.table)?;
                let next_table = next_database_schema.table(&alter_table.table)?;
                let mut altered_steps = fix(&alter_table, &current_table, &next_table, &schema_name);
                result.append(&mut altered_steps);
            }
            x => result.push(x),
        }
    }
    Ok(result)
}

fn needs_fix(alter_table: &AlterTable) -> bool {
    let change_that_does_not_work_on_sqlite = alter_table.changes.iter().find(|change| match change {
        TableChange::AddColumn(add_column) => {
            // sqlite does not allow adding not null columns without a default value even if the table is empty
            // hence we just use our normal migration process
            // https://laracasts.com/discuss/channels/general-discussion/migrations-sqlite-general-error-1-cannot-add-a-not-null-column-with-default-value-null
            add_column.column.required
        }
        TableChange::DropColumn(_) => true,
        TableChange::AlterColumn(_) => true,
    });
    change_that_does_not_work_on_sqlite.is_some()
}

fn fix(_alter_table: &AlterTable, current: &Table, next: &Table, schema_name: &str) -> Vec<SqlMigrationStep> {
    // based on 'Making Other Kinds Of Table Schema Changes' from https://www.sqlite.org/lang_altertable.html
    let name_of_temporary_table = format!("new_{}", next.name.clone());
    vec![
        SqlMigrationStep::RawSql {
            raw: "PRAGMA foreign_keys=OFF;".to_string(),
        },
        // todo: start transaction now
        SqlMigrationStep::CreateTable(CreateTable {
            name: name_of_temporary_table.clone(),
            columns: DatabaseSchemaDiffer::column_descriptions(&next.columns),
            primary_columns: next.primary_key_columns.clone(),
        }),
        // copy table contents; Here we have to handle escpaing ourselves.
        {
            let current_columns: Vec<String> = current.columns.iter().map(|c| c.name.clone()).collect();
            let next_columns: Vec<String> = next.columns.iter().map(|c| c.name.clone()).collect();
            let intersection_columns: Vec<String> = current_columns
                .into_iter()
                .filter(|c| next_columns.contains(&c))
                .collect();
            let columns_string = intersection_columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<String>>()
                .join(",");
            let sql = format!(
                "INSERT INTO \"{}\" ({}) SELECT {} from \"{}\"",
                name_of_temporary_table,
                columns_string,
                columns_string,
                next.name.clone()
            );
            SqlMigrationStep::RawSql { raw: sql.to_string() }
        },
        SqlMigrationStep::DropTable(DropTable {
            name: current.name.clone(),
        }),
        SqlMigrationStep::RenameTable {
            name: name_of_temporary_table,
            new_name: next.name.clone(),
        },
        // todo: recreate indexes + triggers
        SqlMigrationStep::RawSql {
            raw: format!(r#"PRAGMA "{}".foreign_key_check;"#, schema_name),
        },
        // todo: commit transaction
        SqlMigrationStep::RawSql {
            raw: "PRAGMA foreign_keys=ON;".to_string(),
        },
    ]
}

pub fn wrap_as_step<T, F>(steps: Vec<T>, mut wrap_fn: F) -> Vec<SqlMigrationStep>
where
    F: FnMut(T) -> SqlMigrationStep,
{
    steps.into_iter().map(|x| wrap_fn(x)).collect()
}
