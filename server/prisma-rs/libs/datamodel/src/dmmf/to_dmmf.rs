use super::dmmf::*;
use crate::common::PrismaType;
use crate::dml;
use serde_json;

fn get_field_kind(field: &dml::Field) -> String {
    match field.field_type {
        dml::FieldType::Relation(_) => String::from("object"),
        dml::FieldType::Enum(_) => String::from("enum"),
        dml::FieldType::Base(_) => String::from("scalar"),
        _ => unimplemented!("DMMF does not support field type {:?}", field.field_type),
    }
}

fn type_to_string(scalar: &PrismaType) -> String {
    scalar.to_string()
}

fn get_field_type(field: &dml::Field) -> String {
    match &field.field_type {
        dml::FieldType::Relation(relation_info) => relation_info.to.clone(),
        dml::FieldType::Enum(t) => t.clone(),
        dml::FieldType::Base(t) => type_to_string(t),
        dml::FieldType::ConnectorSpecific {
            base_type: t,
            connector_type: _,
        } => type_to_string(t),
    }
}

pub fn enum_to_dmmf(en: &dml::Enum) -> Enum {
    Enum {
        name: en.name.clone(),
        values: en.values.iter().map(|v| v.name.to_string()).collect(),
        db_name: en.database_name.clone(),
        documentation: en.documentation.clone(),
    }
}

pub fn default_value_to_serde(container: &Option<dml::Value>) -> Option<serde_json::Value> {
    match container {
        Some(value) => Some(value_to_serde(value)),
        None => None,
    }
}

pub fn value_to_serde(value: &dml::Value) -> serde_json::Value {
    match value {
        dml::Value::Boolean(val) => serde_json::Value::Bool(*val),
        dml::Value::String(val) => serde_json::Value::String(val.clone()),
        dml::Value::ConstantLiteral(val) => serde_json::Value::String(val.clone()),
        dml::Value::Float(val) => serde_json::Value::Number(serde_json::Number::from_f64(*val as f64).unwrap()),
        dml::Value::Int(val) => serde_json::Value::Number(serde_json::Number::from_f64(*val as f64).unwrap()),
        dml::Value::Decimal(val) => serde_json::Value::Number(serde_json::Number::from_f64(*val as f64).unwrap()),
        dml::Value::DateTime(val) => serde_json::Value::String(val.to_rfc3339()),
        dml::Value::Expression(name, return_type, args) => function_to_serde(&name, *return_type, &args),
    }
}

pub fn function_to_serde(name: &str, return_type: PrismaType, args: &Vec<dml::Value>) -> serde_json::Value {
    let func = Function {
        name: String::from(name),
        return_type: return_type.to_string(),
        args: args.iter().map(|arg| value_to_serde(arg)).collect(),
    };

    serde_json::to_value(&func).expect("Failed to render function JSON")
}

pub fn get_relation_name(field: &dml::Field) -> Option<String> {
    match &field.field_type {
        dml::FieldType::Relation(relation_info) => Some(relation_info.name.clone()),
        _ => None,
    }
}

pub fn get_relation_to_fields(field: &dml::Field) -> Option<Vec<String>> {
    match &field.field_type {
        dml::FieldType::Relation(relation_info) => Some(relation_info.to_fields.clone()),
        _ => None,
    }
}

pub fn get_relation_delete_strategy(field: &dml::Field) -> Option<String> {
    match &field.field_type {
        dml::FieldType::Relation(relation_info) => Some(relation_info.on_delete.to_string()),
        _ => None,
    }
}

pub fn field_to_dmmf(field: &dml::Field) -> Field {
    Field {
        name: field.name.clone(),
        kind: get_field_kind(field),
        db_name: field.database_name.clone(),
        is_required: field.arity == dml::FieldArity::Required,
        is_list: field.arity == dml::FieldArity::List,
        is_id: field.id_info.is_some(),
        default: default_value_to_serde(&field.default_value),
        is_unique: field.is_unique,
        relation_name: get_relation_name(field),
        relation_to_fields: get_relation_to_fields(field),
        relation_on_delete: get_relation_delete_strategy(field),
        field_type: get_field_type(field),
        is_generated: Some(field.is_generated),
        is_updated_at: Some(field.is_updated_at),
        documentation: field.documentation.clone(),
    }
}

pub fn model_to_dmmf(model: &dml::Model) -> Model {
    Model {
        name: model.name.clone(),
        db_name: model.database_name.clone(),
        is_embedded: model.is_embedded,
        fields: model.fields().map(&field_to_dmmf).collect(),
        is_generated: Some(model.is_generated),
        documentation: model.documentation.clone(),
    }
}

pub fn schema_to_dmmf(schema: &dml::Datamodel) -> Datamodel {
    let mut datamodel = Datamodel {
        models: vec![],
        enums: vec![],
    };

    for model in schema.models() {
        datamodel.models.push(model_to_dmmf(&model));
    }

    for enum_model in schema.enums() {
        datamodel.enums.push(enum_to_dmmf(&enum_model));
    }

    return datamodel;
}

pub fn render_to_dmmf(schema: &dml::Datamodel) -> String {
    let dmmf = schema_to_dmmf(schema);
    serde_json::to_string_pretty(&dmmf).expect("Failed to render JSON")
}

pub fn render_to_dmmf_value(schema: &dml::Datamodel) -> serde_json::Value {
    let dmmf = schema_to_dmmf(schema);
    return serde_json::to_value(&dmmf).expect("Failed to render JSON");
}
