use crate::database_inspector::{Column, DatabaseSchema, Table};
use crate::*;

const MIGRATION_TABLE_NAME: &str = "_Migration";

pub struct DatabaseSchemaDiffer<'a> {
    previous: &'a DatabaseSchema,
    next: &'a DatabaseSchema,
}

#[derive(Clone)]
pub struct DatabaseSchemaDiff {
    pub drop_tables: Vec<DropTable>,
    pub create_tables: Vec<CreateTable>,
    pub alter_tables: Vec<AlterTable>,
}

impl DatabaseSchemaDiff {
    pub fn into_steps(self) -> Vec<SqlMigrationStep> {
        let mut steps = Vec::new();
        steps.append(&mut wrap_as_step(self.drop_tables, |x| SqlMigrationStep::DropTable(x)));
        steps.append(&mut wrap_as_step(self.create_tables, |x| {
            SqlMigrationStep::CreateTable(x)
        }));
        steps.append(&mut wrap_as_step(self.alter_tables, |x| {
            SqlMigrationStep::AlterTable(x)
        }));
        steps
    }
}

impl<'a> DatabaseSchemaDiffer<'a> {
    pub fn diff(previous: &DatabaseSchema, next: &DatabaseSchema) -> DatabaseSchemaDiff {
        let differ = DatabaseSchemaDiffer { previous, next };
        differ.diff_internal()
    }

    fn diff_internal(&self) -> DatabaseSchemaDiff {
        DatabaseSchemaDiff {
            drop_tables: self.drop_tables(),
            create_tables: self.create_tables(),
            alter_tables: self.alter_tables(),
        }
    }

    fn create_tables(&self) -> Vec<CreateTable> {
        let mut result = Vec::new();
        for next_table in &self.next.tables {
            if !self.previous.has_table(&next_table.name) && next_table.name != MIGRATION_TABLE_NAME {
                let create = CreateTable {
                    name: next_table.name.clone(),
                    columns: Self::column_descriptions(&next_table.columns),
                    primary_columns: next_table.primary_key_columns.clone(),
                };
                result.push(create);
            }
        }
        result
    }

    fn drop_tables(&self) -> Vec<DropTable> {
        let mut result = Vec::new();
        for previous_table in &self.previous.tables {
            if !self.next.has_table(&previous_table.name) && previous_table.name != MIGRATION_TABLE_NAME {
                let drop = DropTable {
                    name: previous_table.name.clone(),
                };
                result.push(drop);
            }
        }
        result
    }

    fn alter_tables(&self) -> Vec<AlterTable> {
        // TODO: this does not diff primary key columns yet
        let mut result = Vec::new();
        for previous_table in &self.previous.tables {
            if let Ok(next_table) = self.next.table(&previous_table.name) {
                let mut changes = Vec::new();
                changes.append(&mut Self::drop_columns(&previous_table, &next_table));
                changes.append(&mut Self::add_columns(&previous_table, &next_table));
                changes.append(&mut Self::alter_columns(&previous_table, &next_table));

                if !changes.is_empty() {
                    let update = AlterTable {
                        table: previous_table.name.clone(),
                        changes: changes,
                    };
                    result.push(update);
                }
            }
        }
        result
    }

    fn drop_columns(previous: &Table, next: &Table) -> Vec<TableChange> {
        let mut result = Vec::new();
        for previous_column in &previous.columns {
            if !next.has_column(&previous_column.name) {
                let change = DropColumn {
                    name: previous_column.name.clone(),
                };
                result.push(TableChange::DropColumn(change));
            }
        }
        result
    }

    fn add_columns(previous: &Table, next: &Table) -> Vec<TableChange> {
        let mut result = Vec::new();
        for next_column in &next.columns {
            if !previous.has_column(&next_column.name) {
                let change = AddColumn {
                    column: Self::column_description(next_column),
                };
                result.push(TableChange::AddColumn(change));
            }
        }
        result
    }

    fn alter_columns(previous: &Table, next: &Table) -> Vec<TableChange> {
        let mut result = Vec::new();
        for next_column in &next.columns {
            if let Some(previous_column) = previous.column(&next_column.name) {
                if previous_column.differs_in_something_except_default(next_column) {
                    let change = AlterColumn {
                        name: previous_column.name.clone(),
                        column: Self::column_description(next_column),
                    };
                    result.push(TableChange::AlterColumn(change));
                }
            }
        }
        result
    }

    pub fn column_descriptions(columns: &Vec<Column>) -> Vec<ColumnDescription> {
        columns.iter().map(Self::column_description).collect()
    }

    fn column_description(column: &Column) -> ColumnDescription {
        let fk = column.foreign_key.as_ref().map(|fk| ForeignKey {
            table: fk.table.clone(),
            column: fk.column.clone(),
        });
        ColumnDescription {
            name: column.name.clone(),
            tpe: Self::convert_column_type(column.tpe),
            required: column.is_required,
            foreign_key: fk,
            default: column.default.clone(),
        }
    }

    pub fn convert_column_type(inspector_type: database_inspector::ColumnType) -> ColumnType {
        match inspector_type {
            database_inspector::ColumnType::Boolean => ColumnType::Boolean,
            database_inspector::ColumnType::Int => ColumnType::Int,
            database_inspector::ColumnType::Float => ColumnType::Float,
            database_inspector::ColumnType::String => ColumnType::String,
            database_inspector::ColumnType::DateTime => ColumnType::DateTime,
        }
    }
}
