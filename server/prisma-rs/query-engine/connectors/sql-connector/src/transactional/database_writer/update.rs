use crate::{write_query::WriteQueryBuilder, TransactionExt};
use connector::filter::RecordFinder;
use prisma_models::{GraphqlId, ModelRef, PrismaArgs, PrismaListValue, RelationFieldRef};
use std::sync::Arc;

/// Updates one record and any associated list record in the database.
pub fn execute<S>(
    conn: &mut TransactionExt,
    record_finder: &RecordFinder,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<GraphqlId>
where
    S: AsRef<str>,
{
    let model = record_finder.field.model();
    let id = conn.find_id(record_finder)?;

    if let Some(update) = WriteQueryBuilder::update_one(Arc::clone(&model), &id, non_list_args)? {
        conn.update(update)?;
    }

    update_list_args(conn, &[id.clone()], Arc::clone(&model), list_args)?;

    Ok(id)
}

/// Updates a nested item related to the parent, including any associated
/// list values.
pub fn execute_nested<S>(
    conn: &mut TransactionExt,
    parent_id: &GraphqlId,
    record_finder: &Option<RecordFinder>,
    relation_field: RelationFieldRef,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<GraphqlId>
where
    S: AsRef<str>,
{
    if let Some(ref record_finder) = record_finder {
        conn.find_id(record_finder)?;
    };

    let id = conn.find_id_by_parent(Arc::clone(&relation_field), parent_id, record_finder)?;
    let record_finder = RecordFinder::from((relation_field.related_model().fields().id(), id));

    execute(conn, &record_finder, non_list_args, list_args)
}

/// Updates list args related to the given records.
pub fn update_list_args<S>(
    conn: &mut TransactionExt,
    ids: &[GraphqlId],
    model: ModelRef,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<()>
where
    S: AsRef<str>,
{
    for (field_name, list_value) in list_args {
        let field = model.fields().find_from_scalar(field_name.as_ref()).unwrap();
        let table = field.scalar_list_table();
        let (deletes, inserts) = WriteQueryBuilder::update_scalar_list_values(&table, &list_value, ids.to_vec());

        for delete in deletes {
            conn.delete(delete)?;
        }

        for insert in inserts {
            conn.insert(insert)?;
        }
    }

    Ok(())
}
