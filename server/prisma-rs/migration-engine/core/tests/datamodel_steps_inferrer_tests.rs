#![allow(non_snake_case)]
mod test_harness;

use datamodel::dml::*;
use migration_connector::steps::*;
use migration_core::migration::datamodel_migration_steps_inferrer::*;
use test_harness::parse;

#[test]
fn infer_CreateModel_if_it_does_not_exist_yet() {
    let dm1 = Datamodel::empty();
    let dm2 = parse(
        r#"
        model Test {
            id Int @id
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![
        MigrationStep::CreateModel(CreateModel {
            name: "Test".to_string(),
            db_name: None,
            embedded: false,
        }),
        MigrationStep::CreateField(CreateField {
            model: "Test".to_string(),
            name: "id".to_string(),
            tpe: FieldType::Base(ScalarType::Int),
            arity: FieldArity::Required,
            db_name: None,
            is_created_at: None,
            is_updated_at: None,
            is_unique: false,
            id: Some(IdInfo {
                strategy: IdStrategy::Auto,
                sequence: None,
            }),
            default: None,
            scalar_list: None,
        }),
    ];
    assert_eq!(steps, expected);
}

#[test]
fn infer_DeleteModel() {
    let dm1 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
        }
    "#,
    );
    let dm2 = Datamodel::empty();

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::DeleteModel(DeleteModel {
        name: "Test".to_string(),
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_UpdateModel() {
    // TODO: add tests for other properties as well
    let dm1 = parse(
        r#"
        model Post {
            id String @id @default(cuid())
        }
    "#,
    );
    let dm2 = parse(
        r#"
        model Post{
            id String @id @default(cuid())
            
            @@embedded
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::UpdateModel(UpdateModel {
        name: "Post".to_string(),
        new_name: None,
        db_name: None,
        embedded: Some(true),
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_CreateField_if_it_does_not_exist_yet() {
    let dm1 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
        }
    "#,
    );
    let dm2 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
            field Int?
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::CreateField(CreateField {
        model: "Test".to_string(),
        name: "field".to_string(),
        tpe: FieldType::Base(ScalarType::Int),
        arity: FieldArity::Optional,
        db_name: None,
        is_created_at: None,
        is_updated_at: None,
        is_unique: false,
        id: None,
        default: None,
        scalar_list: None,
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_CreateField_if_relation_field_does_not_exist_yet() {
    let dm1 = parse(
        r#"
        model Blog {
            id String @id @default(cuid())
        }
        model Post {
            id String @id @default(cuid())
        }
    "#,
    );
    let dm2 = parse(
        r#"
        model Blog {
            id String @id @default(cuid())
            posts Post[]
        }
        model Post {
            id String @id @default(cuid())
            blog Blog?
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![
        MigrationStep::CreateField(CreateField {
            model: "Blog".to_string(),
            name: "posts".to_string(),
            tpe: FieldType::Relation(RelationInfo {
                to: "Post".to_string(),
                to_fields: vec![],
                name: String::from("BlogToPost"),
                on_delete: OnDeleteStrategy::None,
            }),
            arity: FieldArity::List,
            db_name: None,
            is_created_at: None,
            is_updated_at: None,
            is_unique: false,
            id: None,
            default: None,
            scalar_list: None,
        }),
        MigrationStep::CreateField(CreateField {
            model: "Post".to_string(),
            name: "blog".to_string(),
            tpe: FieldType::Relation(RelationInfo {
                to: "Blog".to_string(),
                to_fields: vec![String::from("id")],
                name: String::from("BlogToPost"),
                on_delete: OnDeleteStrategy::None,
            }),
            arity: FieldArity::Optional,
            db_name: None,
            is_created_at: None,
            is_updated_at: None,
            is_unique: false,
            id: None,
            default: None,
            scalar_list: None,
        }),
    ];
    assert_eq!(steps, expected);
}

#[test]
fn infer_DeleteField() {
    let dm1 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
            field Int?
        }
    "#,
    );
    let dm2 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::DeleteField(DeleteField {
        model: "Test".to_string(),
        name: "field".to_string(),
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_UpdateField_simple() {
    let dm1 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
            field Int?
        }
    "#,
    );
    let dm2 = parse(
        r#"
        model Test {
            id String @id @default(cuid())
            field Boolean @default(false) @unique
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::UpdateField(UpdateField {
        model: "Test".to_string(),
        name: "field".to_string(),
        new_name: None,
        tpe: Some(FieldType::Base(ScalarType::Boolean)),
        arity: Some(FieldArity::Required),
        db_name: None,
        is_created_at: None,
        is_updated_at: None,
        is_unique: Some(true),
        id_info: None,
        default: Some(Some(Value::Boolean(false))),
        scalar_list: None,
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_CreateEnum() {
    let dm1 = Datamodel::empty();
    let dm2 = parse(
        r#"
        enum Test {
            A
            B
        }
    "#,
    );

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::CreateEnum(CreateEnum {
        name: "Test".to_string(),
        db_name: None,
        values: vec![
            EnumValue {
                name: "A".to_string(),
                documentation: None,
            },
            EnumValue {
                name: "B".to_string(),
                documentation: None,
            },
        ],
    })];
    assert_eq!(steps, expected);
}

#[test]
fn infer_DeleteEnum() {
    let dm1 = parse(
        r#"
        enum Test {
            A
            B
        }
    "#,
    );
    let dm2 = Datamodel::empty();

    let steps = infer(&dm1, &dm2);
    let expected = vec![MigrationStep::DeleteEnum(DeleteEnum {
        name: "Test".to_string(),
    })];
    assert_eq!(steps, expected);
}

fn infer(dm1: &Datamodel, dm2: &Datamodel) -> Vec<MigrationStep> {
    let inferrer = DataModelMigrationStepsInferrerImplWrapper {};
    inferrer.infer(&dm1, &dm2)
}
