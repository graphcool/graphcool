use super::update;
use crate::{write_query::WriteQueryBuilder, TransactionExt};
use connector::filter::Filter;
use prisma_models::{GraphqlId, ModelRef, PrismaArgs, PrismaListValue, RelationFieldRef};
use std::sync::Arc;

/// Updates every record and any associated list records in the database
/// matching the `Filter`.
///
/// Returns the number of updated items, if successful.
pub fn execute<S>(
    conn: &mut TransactionExt,
    model: ModelRef,
    filter: &Filter,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<usize>
where
    S: AsRef<str>,
{
    let ids = conn.filter_ids(Arc::clone(&model), filter.clone())?;
    let count = ids.len();

    if count == 0 {
        return Ok(count);
    }

    let updates = {
        let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
        WriteQueryBuilder::update_many(Arc::clone(&model), ids.as_slice(), non_list_args)?
    };

    for update in updates {
        conn.update(update)?;
    }

    update::update_list_args(conn, ids.as_slice(), Arc::clone(&model), list_args)?;

    Ok(count)
}

/// Updates nested items matching to filter, or if no filter is given, all
/// nested items related to the given `parent_id`.
pub fn execute_nested<S>(
    conn: &mut TransactionExt,
    parent_id: &GraphqlId,
    filter: &Option<Filter>,
    relation_field: RelationFieldRef,
    non_list_args: &PrismaArgs,
    list_args: &[(S, PrismaListValue)],
) -> crate::Result<usize>
where
    S: AsRef<str>,
{
    let ids = conn.filter_ids_by_parents(Arc::clone(&relation_field), vec![parent_id], filter.clone())?;
    let count = ids.len();

    if count == 0 {
        return Ok(count);
    }

    let updates = {
        let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
        WriteQueryBuilder::update_many(relation_field.related_model(), ids.as_slice(), non_list_args)?
    };

    for update in updates {
        conn.update(update)?;
    }

    update::update_list_args(conn, ids.as_slice(), relation_field.model(), list_args)?;

    Ok(count)
}
