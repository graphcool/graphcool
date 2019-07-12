use crate::database_inspector::*;
use crate::SqlResult;
use chrono::*;
use datamodel::common::*;
use datamodel::*;
use prisma_models::{DatamodelConverter, TempManifestationHolder, TempRelationHolder};

pub struct DatabaseSchemaCalculator<'a> {
    data_model: &'a Datamodel,
}

impl<'a> DatabaseSchemaCalculator<'a> {
    pub fn calculate(data_model: &Datamodel) -> SqlResult<DatabaseSchema> {
        let calculator = DatabaseSchemaCalculator { data_model };
        calculator.calculate_internal()
    }

    fn calculate_internal(&self) -> SqlResult<DatabaseSchema> {
        let mut tables = Vec::new();
        let model_tables_without_inline_relations = self.calculate_model_tables()?;
        let mut model_tables = self.add_inline_relations_to_model_tables(model_tables_without_inline_relations)?;
        let mut scalar_list_tables = self.calculate_scalar_list_tables()?;
        let mut relation_tables = self.calculate_relation_tables()?;

        tables.append(&mut model_tables);
        tables.append(&mut scalar_list_tables);
        tables.append(&mut relation_tables);

        Ok(DatabaseSchema { tables })
    }

    fn calculate_model_tables(&self) -> SqlResult<Vec<ModelTable>> {
        self.data_model
            .models()
            .map(|model| {
                let columns = model
                    .fields()
                    .flat_map(|f| match (&f.field_type, &f.arity) {
                        (FieldType::Base(_), arity) | (FieldType::Enum(_), arity) if arity != &FieldArity::List => {
                            Some(Column {
                                name: f.db_name(),
                                tpe: column_type(f),
                                is_required: arity == &FieldArity::Required,
                                foreign_key: None,
                                sequence: None,
                                default: Some(f.migration_value(&self.data_model)),
                            })
                        }
                        _ => None,
                    })
                    .collect();

                let table = Table {
                    name: model.db_name(),
                    columns: columns,
                    indexes: Vec::new(),
                    primary_key_columns: vec![model.id_field()?.db_name()],
                };
                Ok(ModelTable {
                    model: model.clone(),
                    table: table,
                })
            })
            .collect()
    }

    fn calculate_scalar_list_tables(&self) -> SqlResult<Vec<Table>> {
        let mut result = Vec::new();

        for model in self.data_model.models() {
            let list_fields: Vec<&Field> = model
                .fields()
                .filter(|f| f.arity == FieldArity::List && is_scalar(f))
                .collect();
            for field in list_fields {
                let id_field = model.id_field()?;
                let table = Table {
                    name: format!("{}_{}", model.db_name(), field.db_name()),
                    columns: vec![
                        Column::with_foreign_key(
                            "nodeId".to_string(),
                            column_type(&id_field),
                            true,
                            ForeignKey::new(model.db_name(), model.id_field()?.db_name()),
                        ),
                        Column::new("position".to_string(), ColumnType::Int, true),
                        Column::new("value".to_string(), column_type(&field), true),
                    ],
                    indexes: Vec::new(),
                    primary_key_columns: vec!["nodeId".to_string(), "position".to_string()],
                };
                result.push(table);
            }
        }

        Ok(result)
    }

    fn add_inline_relations_to_model_tables(&self, model_tables: Vec<ModelTable>) -> SqlResult<Vec<Table>> {
        let mut result = Vec::new();
        let relations = self.calculate_relations();
        for mut model_table in model_tables {
            for relation in relations.iter() {
                match &relation.manifestation {
                    TempManifestationHolder::Inline {
                        in_table_of_model,
                        column,
                    } if in_table_of_model == &model_table.model.name => {
                        let related_model = if model_table.model == relation.model_a {
                            &relation.model_b
                        } else {
                            &relation.model_a
                        };
                        let column = Column::with_foreign_key(
                            column.to_string(),
                            column_type(related_model.id_field()?),
                            relation.field_a.is_required() || relation.field_b.is_required(),
                            ForeignKey::new(related_model.db_name(), related_model.id_field()?.db_name()),
                        );
                        model_table.table.columns.push(column);
                    }
                    _ => {}
                }
            }
            result.push(model_table.table);
        }
        Ok(result)
    }

    fn calculate_relation_tables(&self) -> SqlResult<Vec<Table>> {
        let mut result = Vec::new();
        for relation in self.calculate_relations().iter() {
            match &relation.manifestation {
                TempManifestationHolder::Table => {
                    let table = Table {
                        name: relation.table_name(),
                        columns: vec![
                            Column::with_foreign_key(
                                relation.model_a_column(),
                                column_type(relation.model_a.id_field()?),
                                true,
                                ForeignKey::new(relation.model_a.db_name(), relation.model_a.id_field()?.db_name()),
                            ),
                            Column::with_foreign_key(
                                relation.model_b_column(),
                                column_type(relation.model_b.id_field()?),
                                true,
                                ForeignKey::new(relation.model_b.db_name(), relation.model_b.id_field()?.db_name()),
                            ),
                        ],
                        indexes: Vec::new(),
                        primary_key_columns: Vec::new(),
                    };
                    result.push(table);
                }
                _ => {}
            }
        }
        Ok(result)
    }

    fn calculate_relations(&self) -> Vec<TempRelationHolder> {
        DatamodelConverter::calculate_relations(&self.data_model)
    }
}

#[derive(PartialEq, Debug)]
struct ModelTable {
    table: Table,
    model: Model,
}

pub trait ModelExtensions {
    fn id_field(&self) -> Result<&Field, String>;

    fn db_name(&self) -> String;
}

impl ModelExtensions for Model {
    fn id_field(&self) -> Result<&Field, String> {
        match self.fields().find(|f| f.is_id()) {
            Some(f) => Ok(f),
            None => Err(format!("Model {} does not have an id field", self.name)),
        }
    }

    fn db_name(&self) -> String {
        self.database_name.clone().unwrap_or_else(|| self.name.clone())
    }
}

pub trait FieldExtensions {
    fn is_id(&self) -> bool;

    fn is_list(&self) -> bool;

    fn is_required(&self) -> bool;

    fn db_name(&self) -> String;

    fn migration_value(&self, datamodel: &Datamodel) -> Value;
}

impl FieldExtensions for Field {
    fn is_id(&self) -> bool {
        self.id_info.is_some()
    }

    fn is_list(&self) -> bool {
        self.arity == FieldArity::List
    }

    fn is_required(&self) -> bool {
        self.arity == FieldArity::Required
    }

    fn db_name(&self) -> String {
        self.database_name.clone().unwrap_or_else(|| self.name.clone())
    }

    fn migration_value(&self, datamodel: &Datamodel) -> Value {
        self.default_value.clone().unwrap_or_else(|| match self.field_type {
            FieldType::Base(PrismaType::Boolean) => Value::Boolean(false),
            FieldType::Base(PrismaType::Int) => Value::Int(0),
            FieldType::Base(PrismaType::Float) => Value::Float(0.0),
            FieldType::Base(PrismaType::String) => Value::String("".to_string()),
            FieldType::Base(PrismaType::Decimal) => Value::Decimal(0.0),
            FieldType::Base(PrismaType::DateTime) => {
                let naive = NaiveDateTime::from_timestamp(0, 0);
                let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);
                PrismaValue::DateTime(datetime)
            }
            FieldType::Enum(ref enum_name) => {
                let inum = datamodel
                    .find_enum(&enum_name)
                    .expect(&format!("Enum {} was not present in the Datamodel.", enum_name));
                let first_value = inum
                    .values
                    .first()
                    .expect(&format!("Enum {} did not contain any values.", enum_name));
                Value::String(first_value.to_string())
            }
            _ => unimplemented!("this functions must only be called for scalar fields"),
        })
    }
}

fn is_scalar(field: &Field) -> bool {
    match field.field_type {
        FieldType::Base(_) => true,
        _ => false,
    }
}

fn column_type(field: &Field) -> ColumnType {
    match &field.field_type {
        FieldType::Base(ref scalar) => column_type_for_scalar_type(&scalar),
        FieldType::Enum(_) => ColumnType::String,
        x => panic!(format!(
            "This field type is not suported here. Field type is {:?} on field {}",
            x, field.name
        )),
    }
}

fn column_type_for_scalar_type(scalar_type: &ScalarType) -> ColumnType {
    match scalar_type {
        ScalarType::Int => ColumnType::Int,
        ScalarType::Float => ColumnType::Float,
        ScalarType::Boolean => ColumnType::Boolean,
        ScalarType::String => ColumnType::String,
        ScalarType::DateTime => ColumnType::DateTime,
        ScalarType::Decimal => unimplemented!(),
    }
}
