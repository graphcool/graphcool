use crate::*;
use datamodel::dml;
use itertools::Itertools;

pub struct DatamodelConverter<'a> {
    datamodel: &'a dml::Datamodel,
    relations: Vec<TempRelationHolder>,
}

#[allow(unused)]
impl<'a> DatamodelConverter<'a> {
    pub fn convert_string(datamodel: String) -> InternalDataModelTemplate {
        let datamodel = datamodel::parse(&datamodel).unwrap();
        Self::convert(&datamodel)
    }

    pub fn convert(datamodel: &dml::Datamodel) -> InternalDataModelTemplate {
        DatamodelConverter::new(datamodel).convert_internal()
    }

    fn new(datamodel: &dml::Datamodel) -> DatamodelConverter {
        DatamodelConverter {
            datamodel,
            relations: Self::calculate_relations(datamodel),
        }
    }

    fn convert_internal(&self) -> InternalDataModelTemplate {
        InternalDataModelTemplate {
            models: self.convert_models(),
            relations: self.convert_relations(),
            enums: self.convert_enums(),
            version: Some("v2".to_string()),
        }
    }

    fn convert_enums(&self) -> Vec<InternalEnum> {
        self.datamodel
            .enums()
            .map(|e| InternalEnum {
                name: e.name.clone(),
                values: e.values.iter().map(|v| v.name.clone()).collect(),
            })
            .collect()
    }

    fn convert_models(&self) -> Vec<ModelTemplate> {
        self.datamodel
            .models()
            .map(|model| ModelTemplate {
                name: model.name.clone(),
                stable_identifier: "".to_string(),
                is_embedded: model.is_embedded,
                fields: self.convert_fields(model),
                manifestation: model.database_name.clone().map(|n| ModelManifestation { db_name: n }),
            })
            .collect()
    }

    fn convert_fields(&self, model: &dml::Model) -> Vec<FieldTemplate> {
        model
            .fields()
            .map(|field| match field.type_identifier() {
                TypeIdentifier::Relation => {
                    let relation = self
                        .relations
                        .iter()
                        .find(|r| r.is_for_model_and_field(model, field))
                        .expect(&format!(
                            "Did not find a relation for those for model {} and field {}",
                            model.name, field.name
                        ));

                    FieldTemplate::Relation(RelationFieldTemplate {
                        name: field.name.clone(),
                        type_identifier: field.type_identifier(),
                        is_required: field.is_required(),
                        is_list: field.is_list(),
                        is_unique: field.is_unique(),
                        is_hidden: false,
                        is_auto_generated: field.is_auto_generated(),
                        manifestation: field.manifestation(),
                        relation_name: relation.name(),
                        relation_side: relation.relation_side(field),
                    })
                }
                ti => FieldTemplate::Scalar(ScalarFieldTemplate {
                    name: field.name.clone(),
                    type_identifier: field.type_identifier(),
                    is_required: field.is_required(),
                    is_list: field.is_list(),
                    is_unique: field.is_unique(),
                    is_hidden: false,
                    is_auto_generated: field.is_auto_generated(),
                    manifestation: field.manifestation(),
                    behaviour: field.behaviour(),
                    default_value: field.default_value(),
                    internal_enum: field.internal_enum(self.datamodel),
                }),
            })
            .collect()
    }

    fn convert_relations(&self) -> Vec<RelationTemplate> {
        self.relations
            .iter()
            .map(|r| RelationTemplate {
                name: r.name(),
                model_a_on_delete: OnDelete::SetNull,
                model_b_on_delete: OnDelete::SetNull,
                manifestation: Some(r.manifestation()),
                model_a_name: r.model_a.name.clone(),
                model_b_name: r.model_b.name.clone(),
            })
            .collect()
    }

    pub fn calculate_relations(datamodel: &dml::Datamodel) -> Vec<TempRelationHolder> {
        let mut result = Vec::new();
        for model in datamodel.models() {
            for field in model.fields() {
                match &field.field_type {
                    dml::FieldType::Relation(relation_info) => {
                        let dml::RelationInfo {
                            to,
                            to_fields,
                            name,
                            on_delete: _,
                        } = relation_info;
                        let related_model = datamodel
                            .find_model(&to)
                            .expect(&format!("Related model {} not found", to));

                        let related_field = related_model
                            .fields()
                            .find(|f| match f.field_type {
                                dml::FieldType::Relation(ref rel_info) => {
                                    // TODO: i probably don't need to check the the `to`. The name of the relation should be enough. The parser must guarantee that the relation info is set right.
                                    if &model.name == &related_model.name {
                                        // SELF RELATIONS
                                        &rel_info.to == &model.name && &rel_info.name == name && f.name != field.name
                                    } else {
                                        // In a normal relation the related field could be named the same hence we omit the last condition from above.
                                        &rel_info.to == &model.name && &rel_info.name == name
                                    }
                                }
                                _ => false,
                            })
                            .expect(&format!(
                                "Related model for model {} and field {} not found",
                                model.name, field.name
                            ))
                            .clone();

                        let related_field_info = match &related_field.field_type {
                            dml::FieldType::Relation(info) => info,
                            _ => panic!("this was not a relation field"),
                        };

                        let (model_a, model_b, field_a, field_b) = match () {
                            _ if &model.name < &related_model.name => (
                                model.clone(),
                                related_model.clone(),
                                field.clone(),
                                related_field.clone(),
                            ),
                            _ if &related_model.name < &model.name => (
                                related_model.clone(),
                                model.clone(),
                                related_field.clone(),
                                field.clone(),
                            ),
                            _ => (
                                model.clone(),
                                related_model.clone(),
                                field.clone(),
                                related_field.clone(),
                            ),
                        };
                        let inline_on_model_a = TempManifestationHolder::Inline {
                            in_table_of_model: model_a.name.clone(),
                            column: field_a.final_db_name(),
                        };
                        let inline_on_model_b = TempManifestationHolder::Inline {
                            in_table_of_model: model_b.name.clone(),
                            column: field_b.final_db_name(),
                        };
                        let inline_on_this_model = TempManifestationHolder::Inline {
                            in_table_of_model: model.name.clone(),
                            column: field.final_db_name(),
                        };
                        let inline_on_related_model = TempManifestationHolder::Inline {
                            in_table_of_model: related_model.name.clone(),
                            column: related_field.final_db_name(),
                        };

                        let manifestation = match (field_a.is_list(), field_b.is_list()) {
                            (true, true) => TempManifestationHolder::Table,
                            (false, true) => inline_on_model_a,
                            (true, false) => inline_on_model_b,
                            // TODO: to_fields is now a list, please fix this line.
                            (false, false) => match (to_fields.first(), &related_field_info.to_fields.first()) {
                                (Some(_), None) => inline_on_this_model,
                                (None, Some(_)) => inline_on_related_model,
                                (None, None) => {
                                    if model_a.name < model_b.name {
                                        inline_on_model_a
                                    } else {
                                        inline_on_model_b
                                    }
                                }
                                (Some(_), Some(_)) => {
                                    panic!("It's not allowed that both sides of a relation specify the inline policy. The field was {} on model {}. The related field was {} on model {}.", field.name, model.name, related_field.name, related_model.name)
                                }
                            },
                        };

                        result.push(TempRelationHolder {
                            name: name.clone(),
                            model_a: model_a,
                            model_b: model_b,
                            field_a: field_a,
                            field_b: field_b,
                            manifestation,
                        })
                    }
                    _ => {}
                }
            }
        }
        result.into_iter().unique_by(|rel| rel.name()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct TempRelationHolder {
    pub name: String,
    pub model_a: dml::Model,
    pub model_b: dml::Model,
    pub field_a: dml::Field,
    pub field_b: dml::Field,
    pub manifestation: TempManifestationHolder,
}

#[derive(PartialEq, Debug, Clone)]
pub enum TempManifestationHolder {
    Inline { in_table_of_model: String, column: String },
    Table,
}

#[allow(unused)]
impl TempRelationHolder {
    fn name(&self) -> String {
        // TODO: must replicate behaviour of `generateRelationName` from `SchemaInferrer`
        match &self.name as &str {
            "" => format!("{}To{}", &self.model_a.name, &self.model_b.name),
            _ => self.name.clone(),
        }
    }

    pub fn table_name(&self) -> String {
        format!("_{}", self.name())
    }

    pub fn model_a_column(&self) -> String {
        "A".to_string()
    }

    pub fn model_b_column(&self) -> String {
        "B".to_string()
    }

    fn is_many_to_many(&self) -> bool {
        self.field_a.is_list() && self.field_b.is_list()
    }

    fn is_for_model_and_field(&self, model: &dml::Model, field: &dml::Field) -> bool {
        (&self.model_a == model && &self.field_a == field) || (&self.model_b == model && &self.field_b == field)
    }

    fn relation_side(&self, field: &dml::Field) -> RelationSide {
        if field == &self.field_a {
            RelationSide::A
        } else if field == &self.field_b {
            RelationSide::B
        } else {
            panic!("this field is not part of hte relations")
        }
    }

    fn manifestation(&self) -> RelationLinkManifestation {
        match &self.manifestation {
            // TODO: relation table columns must get renamed: lowercased type names instead of A and B
            TempManifestationHolder::Table => RelationLinkManifestation::RelationTable(RelationTable {
                table: self.table_name(),
                model_a_column: self.model_a_column(),
                model_b_column: self.model_b_column(),
                id_column: None,
            }),
            TempManifestationHolder::Inline {
                in_table_of_model,
                column,
            } => RelationLinkManifestation::Inline(InlineRelation {
                in_table_of_model_name: in_table_of_model.to_string(),
                referencing_column: column.to_string(),
            }),
        }
    }
}

trait DatamodelFieldExtensions {
    fn type_identifier(&self) -> TypeIdentifier;
    fn is_required(&self) -> bool;
    fn is_list(&self) -> bool;
    fn is_unique(&self) -> bool;
    fn is_auto_generated(&self) -> bool;
    fn manifestation(&self) -> Option<FieldManifestation>;
    fn behaviour(&self) -> Option<FieldBehaviour>;
    fn final_db_name(&self) -> String;
    fn internal_enum(&self, datamodel: &dml::Datamodel) -> Option<InternalEnum>;
    fn default_value(&self) -> Option<PrismaValue>;
}

impl DatamodelFieldExtensions for dml::Field {
    fn type_identifier(&self) -> TypeIdentifier {
        // todo: add support for CUID and UUID
        match self.field_type {
            dml::FieldType::Enum(_) => TypeIdentifier::Enum,
            dml::FieldType::Relation(_) => TypeIdentifier::Relation,
            dml::FieldType::Base(scalar) => match scalar {
                dml::ScalarType::Boolean => TypeIdentifier::Boolean,
                dml::ScalarType::DateTime => TypeIdentifier::DateTime,
                dml::ScalarType::Decimal => TypeIdentifier::Float,
                dml::ScalarType::Float => TypeIdentifier::Float,
                dml::ScalarType::Int => TypeIdentifier::Int,
                dml::ScalarType::String => match self.default_value {
                    Some(datamodel::common::PrismaValue::Expression(ref expr, _, _)) if expr == "cuid" => {
                        TypeIdentifier::GraphQLID
                    }
                    Some(datamodel::common::PrismaValue::Expression(ref expr, _, _)) if expr == "uuid" => {
                        TypeIdentifier::UUID
                    }
                    _ => TypeIdentifier::String,
                },
            },
            dml::FieldType::ConnectorSpecific {
                base_type: _,
                connector_type: _,
            } => unimplemented!("Connector Specific types are not supported here yet"),
        }
    }

    fn is_required(&self) -> bool {
        self.arity == dml::FieldArity::Required
    }
    fn is_list(&self) -> bool {
        self.arity == dml::FieldArity::List
    }
    fn is_unique(&self) -> bool {
        self.is_unique
    }
    fn is_auto_generated(&self) -> bool {
        let has_auto_generating_behaviour = self
            .id_info
            .as_ref()
            .filter(|id| id.strategy == dml::IdStrategy::Auto)
            .is_some();
        let is_an_int = self.type_identifier() == TypeIdentifier::Int;
        has_auto_generating_behaviour && is_an_int
    }

    fn manifestation(&self) -> Option<FieldManifestation> {
        self.database_name.clone().map(|n| FieldManifestation { db_name: n })
    }

    fn behaviour(&self) -> Option<FieldBehaviour> {
        // TODO: implement this properly once this is specced for the datamodel
        self.id_info
            .as_ref()
            .map(|id_info| {
                let strategy = match id_info.strategy {
                    dml::IdStrategy::Auto => IdStrategy::Auto,
                    dml::IdStrategy::None => IdStrategy::None,
                };
                FieldBehaviour::Id {
                    strategy: strategy,
                    sequence: None, // the sequence was just used by the migration engine. Now those models are only used by the query engine. Hence we don't need it anyway.
                }
            })
            // case: @default(now())
            .or_else(|| match self.default_value {
                Some(datamodel::common::PrismaValue::Expression(ref expr, _, _)) if expr == "now" => {
                    Some(FieldBehaviour::CreatedAt)
                }
                _ => None,
            })
            .or_else(|| {
                if self.is_updated_at {
                    Some(FieldBehaviour::UpdatedAt)
                } else {
                    None
                }
            })
            .or_else(|| {
                self.scalar_list_strategy.map(|sls| match sls {
                    datamodel::ScalarListStrategy::Embedded => FieldBehaviour::ScalarList {
                        strategy: ScalarListStrategy::Embedded,
                    },
                    datamodel::ScalarListStrategy::Relation => FieldBehaviour::ScalarList {
                        strategy: ScalarListStrategy::Relation,
                    },
                })
            })
    }

    fn final_db_name(&self) -> String {
        self.database_name.clone().unwrap_or_else(|| self.name.clone())
    }

    fn internal_enum(&self, datamodel: &dml::Datamodel) -> Option<InternalEnum> {
        match self.field_type {
            dml::FieldType::Enum(ref name) => {
                datamodel
                    .enums()
                    .find(|e| e.name == name.clone())
                    .map(|e| InternalEnum {
                        name: e.name.clone(),
                        values: e.values.iter().map(|v| v.name.clone()).collect(),
                    })
            }
            _ => None,
        }
    }

    fn default_value(&self) -> Option<PrismaValue> {
        self.default_value.as_ref().and_then(|v| match v {
            datamodel::common::PrismaValue::Boolean(x) => Some(PrismaValue::Boolean(*x)),
            datamodel::common::PrismaValue::Int(x) => Some(PrismaValue::Int(*x as i64)),
            datamodel::common::PrismaValue::Float(x) => Some(PrismaValue::Float(*x as f64)),
            datamodel::common::PrismaValue::String(x) => Some(PrismaValue::String(x.clone())),
            datamodel::common::PrismaValue::DateTime(x) => Some(PrismaValue::DateTime(*x)),
            datamodel::common::PrismaValue::Decimal(x) => Some(PrismaValue::Float(*x as f64)), // TODO: not sure if this mapping is correct
            datamodel::common::PrismaValue::ConstantLiteral(x) => Some(PrismaValue::Enum(x.clone())),
            datamodel::common::PrismaValue::Expression(_, _, _) => None, // expressions are handled in the behaviour function right now
        })
    }
}
