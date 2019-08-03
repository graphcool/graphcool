extern crate datamodel;

use datamodel::{common::PrismaType, configuration::SourceDefinition, dml, errors::*};

pub trait FieldAsserts {
    fn assert_base_type(&self, t: &PrismaType) -> &Self;
    fn assert_enum_type(&self, en: &str) -> &Self;
    fn assert_relation_name(&self, t: &str) -> &Self;
    fn assert_relation_to(&self, t: &str) -> &Self;
    fn assert_relation_delete_strategy(&self, t: dml::OnDeleteStrategy) -> &Self;
    fn assert_relation_to_fields(&self, t: &[&str]) -> &Self;
    fn assert_arity(&self, arity: &dml::FieldArity) -> &Self;
    fn assert_with_db_name(&self, t: &str) -> &Self;
    fn assert_with_documentation(&self, t: &str) -> &Self;
    fn assert_default_value(&self, t: dml::Value) -> &Self;
    fn assert_is_generated(&self, b: bool) -> &Self;
    fn assert_is_id(&self, b: bool) -> &Self;
    fn assert_is_unique(&self, b: bool) -> &Self;
    fn assert_is_updated_at(&self, b: bool) -> &Self;
    fn assert_id_strategy(&self, strategy: dml::IdStrategy) -> &Self;
    fn assert_id_sequence(&self, strategy: Option<dml::Sequence>) -> &Self;
}

pub trait ModelAsserts {
    fn assert_has_field(&self, t: &str) -> &dml::Field;
    fn assert_is_embedded(&self, t: bool) -> &Self;
    fn assert_with_db_name(&self, t: &str) -> &Self;
    fn assert_with_documentation(&self, t: &str) -> &Self;
}

pub trait EnumAsserts {
    fn assert_has_value(&self, t: &str) -> &dml::EnumValue;
}

pub trait EnumValueAsserts {
    fn assert_has_comment(&self, c: &str) -> &Self;
}

pub trait DatamodelAsserts {
    fn assert_has_model(&self, t: &str) -> &dml::Model;
    fn assert_has_enum(&self, t: &str) -> &dml::Enum;
}

pub trait ErrorAsserts {
    fn assert_is(&self, error: ValidationError) -> &Self;
    fn assert_is_at(&self, index: usize, error: ValidationError) -> &Self;
}

impl FieldAsserts for dml::Field {
    fn assert_base_type(&self, t: &PrismaType) -> &Self {
        if let dml::FieldType::Base(base_type) = &self.field_type {
            assert_eq!(base_type, t);
        } else {
            panic!("Scalar expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_enum_type(&self, en: &str) -> &Self {
        if let dml::FieldType::Enum(enum_type) = &self.field_type {
            assert_eq!(enum_type, en);
        } else {
            panic!("Enum expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_relation_to(&self, t: &str) -> &Self {
        if let dml::FieldType::Relation(info) = &self.field_type {
            assert_eq!(info.to, t);
        } else {
            panic!("Relation expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_relation_name(&self, t: &str) -> &Self {
        if let dml::FieldType::Relation(info) = &self.field_type {
            assert_eq!(info.name, String::from(t));
        } else {
            panic!("Relation expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_relation_delete_strategy(&self, t: dml::OnDeleteStrategy) -> &Self {
        if let dml::FieldType::Relation(info) = &self.field_type {
            assert_eq!(info.on_delete, t);
        } else {
            panic!("Relation expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_relation_to_fields(&self, t: &[&str]) -> &Self {
        if let dml::FieldType::Relation(info) = &self.field_type {
            assert_eq!(info.to_fields, t);
        } else {
            panic!("Relation expected, but found {:?}", self.field_type);
        }

        return self;
    }

    fn assert_arity(&self, arity: &dml::FieldArity) -> &Self {
        assert_eq!(self.arity, *arity);

        return self;
    }

    fn assert_with_db_name(&self, t: &str) -> &Self {
        assert_eq!(self.database_name, Some(String::from(t)));

        return self;
    }

    fn assert_with_documentation(&self, t: &str) -> &Self {
        assert_eq!(self.documentation, Some(String::from(t)));

        return self;
    }

    fn assert_default_value(&self, t: dml::Value) -> &Self {
        assert_eq!(self.default_value, Some(t));

        return self;
    }

    fn assert_is_id(&self, b: bool) -> &Self {
        assert_eq!(self.id_info.is_some(), b);

        return self;
    }

    fn assert_is_generated(&self, b: bool) -> &Self {
        assert_eq!(self.is_generated, b);

        return self;
    }

    fn assert_is_unique(&self, b: bool) -> &Self {
        assert_eq!(self.is_unique, b);

        return self;
    }

    fn assert_is_updated_at(&self, b: bool) -> &Self {
        assert_eq!(self.is_updated_at, b);

        return self;
    }

    fn assert_id_strategy(&self, strategy: dml::IdStrategy) -> &Self {
        if let Some(id_info) = &self.id_info {
            assert_eq!(id_info.strategy, strategy)
        } else {
            panic!("Id field expected, but no id info given");
        }

        return self;
    }

    fn assert_id_sequence(&self, sequence: Option<dml::Sequence>) -> &Self {
        if let Some(id_info) = &self.id_info {
            assert_eq!(id_info.sequence, sequence)
        } else {
            panic!("Id field expected, but no id info given");
        }

        return self;
    }
}

impl DatamodelAsserts for dml::Datamodel {
    fn assert_has_model(&self, t: &str) -> &dml::Model {
        self.find_model(&String::from(t))
            .expect(format!("Model {} not found", t).as_str())
    }
    fn assert_has_enum(&self, t: &str) -> &dml::Enum {
        self.find_enum(&String::from(t))
            .expect(format!("Enum {} not found", t).as_str())
    }
}

impl ModelAsserts for dml::Model {
    fn assert_has_field(&self, t: &str) -> &dml::Field {
        self.find_field(&String::from(t))
            .expect(format!("Field {} not found", t).as_str())
    }
    fn assert_is_embedded(&self, t: bool) -> &Self {
        assert_eq!(self.is_embedded, t);

        return self;
    }
    fn assert_with_db_name(&self, t: &str) -> &Self {
        assert_eq!(self.database_name, Some(String::from(t)));

        return self;
    }
    fn assert_with_documentation(&self, t: &str) -> &Self {
        assert_eq!(self.documentation, Some(String::from(t)));

        return self;
    }
}

impl EnumAsserts for dml::Enum {
    fn assert_has_value(&self, t: &str) -> &dml::EnumValue {
        let pred = String::from(t);
        self.values
            .iter()
            .find(|x| x.name == pred)
            .expect(format!("Value {} not found", t).as_str())
    }
}

impl EnumValueAsserts for dml::EnumValue {
    fn assert_has_comment(&self, c: &str) -> &Self {
        assert_eq!(self.documentation, Some(c.to_string()));
        self
    }
}

impl ErrorAsserts for ErrorCollection {
    fn assert_is(&self, error: ValidationError) -> &Self {
        if self.errors.len() == 1 {
            assert_eq!(self.errors[0], error);
        } else {
            panic!("Expected exactly one validation error.");
        }

        return self;
    }

    fn assert_is_at(&self, index: usize, error: ValidationError) -> &Self {
        assert_eq!(self.errors[index], error);
        return self;
    }
}

#[allow(dead_code)] // Not sure why the compiler thinks this is never used.
pub fn parse(datamodel_string: &str) -> datamodel::Datamodel {
    parse_with_plugins(datamodel_string, vec![])
}

pub fn parse_with_plugins(
    datamodel_string: &str,
    source_definitions: Vec<Box<SourceDefinition>>,
) -> datamodel::Datamodel {
    match datamodel::parse_with_plugins(datamodel_string, source_definitions) {
        Ok(s) => s,
        Err(errs) => {
            for err in errs.to_iter() {
                err.pretty_print(&mut std::io::stderr().lock(), "", datamodel_string)
                    .unwrap();
            }
            panic!("Datamodel parsing failed. Please see error above.")
        }
    }
}

#[allow(dead_code)] // Not sure why the compiler thinks this is never used.
pub fn parse_error(datamodel_string: &str) -> ErrorCollection {
    parse_with_plugins_error(datamodel_string, vec![])
}

pub fn parse_with_plugins_error(
    datamodel_string: &str,
    source_definitions: Vec<Box<SourceDefinition>>,
) -> ErrorCollection {
    match datamodel::parse_with_plugins(datamodel_string, source_definitions) {
        Ok(_) => panic!("Expected an error when parsing schema."),
        Err(errs) => errs,
    }
}
