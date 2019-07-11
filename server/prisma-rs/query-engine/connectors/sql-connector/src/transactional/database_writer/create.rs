use crate::{
    error::SqlError,
    write_query::{NestedActions, WriteQueryBuilder},
    TransactionExt,
};
use prisma_models::{GraphqlId, ModelRef, PrismaArgs, PrismaListValue, RelationFieldRef};
use std::sync::Arc;

/// Creates a new root record and any associated list records to the database.
pub fn execute<S>(
    conn: &mut TransactionExt,
    model: ModelRef,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<GraphqlId>
where
    S: AsRef<str>,
{
    let (insert, returned_id) = WriteQueryBuilder::create_record(Arc::clone(&model), non_list_args.clone());

    let last_id = match conn.insert(insert) {
        Ok(id) => id,
        Err(SqlError::UniqueConstraintViolation { field_name }) => {
            if field_name == "PRIMARY" {
                return Err(SqlError::UniqueConstraintViolation {
                    field_name: format!("{}.{}", model.name, model.fields().id().name),
                });
            } else {
                return Err(SqlError::UniqueConstraintViolation {
                    field_name: format!("{}.{}", model.name, field_name),
                });
            }
        }
        Err(SqlError::NullConstraintViolation { field_name }) => {
            if field_name == "PRIMARY" {
                return Err(SqlError::NullConstraintViolation {
                    field_name: format!("{}.{}", model.name, model.fields().id().name),
                });
            } else {
                return Err(SqlError::NullConstraintViolation {
                    field_name: format!("{}.{}", model.name, field_name),
                });
            }
        }
        Err(e) => return Err(e),
    };

    let id = match returned_id {
        Some(id) => id,
        None => GraphqlId::from(last_id.unwrap()),
    };

    for (field_name, list_value) in list_args {
        let field = model.fields().find_from_scalar(field_name.as_ref()).unwrap();
        let table = field.scalar_list_table();

        if let Some(insert) = WriteQueryBuilder::create_scalar_list_value(table.table(), &list_value, &id) {
            conn.insert(insert)?;
        }
    }

    Ok(id)
}

/// Creates a new nested item related to a parent, including any associated
/// list values, and is connected with the `parent_id` to the parent record.
pub fn execute_nested<S>(
    conn: &mut TransactionExt,
    parent_id: &GraphqlId,
    actions: &NestedActions,
    relation_field: RelationFieldRef,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<GraphqlId>
where
    S: AsRef<str>,
{
    if let Some((select, check)) = actions.required_check(parent_id)? {
        let ids = conn.select_ids(select)?;
        check(ids.into_iter().next().is_some())?
    };

    if let Some(query) = actions.parent_removal(parent_id) {
        conn.execute(query)?;
    }

    let related_field = relation_field.related_field();

    if related_field.relation_is_inlined_in_parent() {
        let mut prisma_args = non_list_args.clone();
        prisma_args.insert(related_field.name.clone(), parent_id.clone());

        execute(conn, relation_field.related_model(), &prisma_args, list_args)
    } else {
        let id = execute(conn, relation_field.related_model(), non_list_args, list_args)?;
        let relation_query = WriteQueryBuilder::create_relation(relation_field, parent_id, &id);

        conn.execute(relation_query)?;

        Ok(id)
    }
}
