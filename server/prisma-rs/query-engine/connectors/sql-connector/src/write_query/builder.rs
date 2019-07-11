use crate::error::SqlError;
use prisma_models::prelude::*;
use prisma_query::ast::*;

pub struct WriteQueryBuilder;

impl WriteQueryBuilder {
    const PARAMETER_LIMIT: usize = 10000;

    pub fn create_record(model: ModelRef, mut args: PrismaArgs) -> (Insert<'static>, Option<GraphqlId>) {
        let model_id = model.fields().id();

        let return_id = match args.get_field_value(&model_id.name) {
            _ if model_id.is_auto_generated => None,
            Some(PrismaValue::Null) | None => {
                let id = model.generate_id();
                args.insert(model_id.name.as_str(), id.clone());
                Some(id)
            }
            Some(PrismaValue::GraphqlId(id)) => Some(id.clone()),
            _ => None,
        };

        let fields: Vec<&Field> = model
            .fields()
            .all
            .iter()
            .filter(|field| args.has_arg_for(&field.name()))
            .collect();

        let fields = fields
            .iter()
            .map(|field| (field.db_name(), args.take_field_value(field.name()).unwrap()));

        let base = Insert::single_into(model.table());

        let insert = fields
            .into_iter()
            .fold(base, |acc, (name, value)| acc.value(name.into_owned(), value));

        (Insert::from(insert).returning(vec![model_id.as_column()]), return_id)
    }

    pub fn create_relation(field: RelationFieldRef, parent_id: &GraphqlId, child_id: &GraphqlId) -> Query<'static> {
        let relation = field.relation();

        match relation.inline_relation_column() {
            Some(column) => {
                let referencing_column = column.name.to_string();

                let (update_id, link_id) = match field.relation_is_inlined_in_parent() {
                    true => (parent_id, child_id),
                    false => (child_id, parent_id),
                };

                let update_condition = match field.relation_is_inlined_in_parent() {
                    true => field.model().fields().id().as_column().equals(update_id),
                    false => field.related_model().fields().id().as_column().equals(update_id),
                };

                Update::table(relation.relation_table())
                    .set(referencing_column, link_id.clone())
                    .so_that(update_condition)
                    .into()
            }
            None => {
                let relation = field.relation();
                let parent_column = field.relation_column();
                let child_column = field.opposite_column();

                let insert = Insert::single_into(relation.relation_table())
                    .value(parent_column.name.to_string(), parent_id.clone())
                    .value(child_column.name.to_string(), child_id.clone());

                let insert: Insert = match relation.id_column() {
                    Some(id_column) => insert.value(id_column, cuid::cuid().unwrap()).into(),
                    None => insert.into(),
                };

                insert.on_conflict(OnConflict::DoNothing).into()
            }
        }
    }

    pub fn create_scalar_list_value(
        scalar_list_table: Table<'static>,
        list_value: &PrismaListValue,
        id: &GraphqlId,
    ) -> Option<Insert<'static>> {
        let list_value = match list_value {
            Some(l) if l.is_empty() => return None,
            None => return None,
            Some(l) => l,
        };

        let positions = (1..=list_value.len()).map(|v| (v * 1000) as i64);
        let values = list_value.iter().zip(positions);

        let columns = vec![
            ScalarListTable::POSITION_FIELD_NAME,
            ScalarListTable::VALUE_FIELD_NAME,
            ScalarListTable::NODE_ID_FIELD_NAME,
        ];

        let insert = Insert::multi_into(scalar_list_table, columns);

        let result = values
            .fold(insert, |acc, (value, position)| {
                acc.values((position, value.clone(), id.clone()))
            })
            .into();

        Some(result)
    }

    pub fn update_one(model: ModelRef, id: &GraphqlId, args: &PrismaArgs) -> crate::Result<Option<Update<'static>>> {
        Self::update_many(model, &[id; 1], args).map(|updates| updates.into_iter().next())
    }

    pub fn update_many(model: ModelRef, ids: &[&GraphqlId], args: &PrismaArgs) -> crate::Result<Vec<Update<'static>>> {
        if args.args.is_empty() || ids.is_empty() {
            return Ok(Vec::new());
        }

        let fields = model.fields();
        let mut query = Update::table(model.table());

        for (name, value) in args.args.iter() {
            let field = fields.find_from_scalar(&name).unwrap();

            if field.is_required && value.is_null() {
                return Err(SqlError::FieldCannotBeNull {
                    field: field.name.clone(),
                });
            }

            query = query.set(field.db_name().to_string(), value.clone());
        }

        let result: Vec<Update> = ids
            .chunks(Self::PARAMETER_LIMIT)
            .into_iter()
            .map(|ids| {
                query
                    .clone()
                    .so_that(fields.id().as_column().in_selection(ids.to_vec()))
            })
            .collect();

        Ok(result)
    }

    pub fn delete_many(model: ModelRef, ids: &[&GraphqlId]) -> Vec<Delete<'static>> {
        let mut deletes = Vec::new();

        for chunk in ids.chunks(Self::PARAMETER_LIMIT).into_iter() {
            for lf in model.fields().scalar_list() {
                let scalar_list_table = lf.scalar_list_table();
                let condition = scalar_list_table.node_id_column().in_selection(chunk.to_vec());
                deletes.push(Delete::from_table(scalar_list_table.table()).so_that(condition));
            }

            let condition = model.fields().id().as_column().in_selection(chunk.to_vec());
            deletes.push(Delete::from_table(model.table()).so_that(condition));
        }

        deletes
    }

    pub fn update_scalar_list_values(
        scalar_list_table: &ScalarListTable,
        list_value: &PrismaListValue,
        ids: Vec<GraphqlId>,
    ) -> (Vec<Delete<'static>>, Vec<Insert<'static>>) {
        if ids.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let deletes = {
            let ids: Vec<&GraphqlId> = ids.iter().map(|id| &*id).collect();
            Self::delete_scalar_list_values(scalar_list_table, ids.as_slice())
        };

        let inserts = match list_value {
            Some(l) if l.is_empty() => Vec::new(),
            _ => ids
                .iter()
                .flat_map(|id| Self::create_scalar_list_value(scalar_list_table.table(), list_value, id))
                .collect(),
        };

        (deletes, inserts)
    }

    pub fn delete_scalar_list_values(scalar_list_table: &ScalarListTable, ids: &[&GraphqlId]) -> Vec<Delete<'static>> {
        Self::delete_in_chunks(scalar_list_table.table(), ids, |chunk| {
            ScalarListTable::NODE_ID_FIELD_NAME.in_selection(chunk.to_vec())
        })
    }

    pub fn truncate_tables(internal_data_model: InternalDataModelRef) -> Vec<Table<'static>> {
        let models = internal_data_model.models();
        let mut tables = Vec::new();

        tables = internal_data_model
            .relations()
            .iter()
            .map(|r| r.relation_table())
            .fold(tables, |mut acc, table| {
                acc.push(table);
                acc
            });

        tables = models.iter().map(|m| m.table()).fold(tables, |mut acc, table| {
            acc.push(table);
            acc
        });

        tables = models
            .iter()
            .flat_map(|m| {
                let tables: Vec<Table> = m
                    .fields()
                    .scalar_list()
                    .iter()
                    .map(|slf| slf.scalar_list_table().table())
                    .collect();

                tables
            })
            .fold(tables, |mut acc, table| {
                acc.push(table);
                acc
            });

        tables
    }

    fn delete_in_chunks<F>(table: Table<'static>, ids: &[&GraphqlId], conditions: F) -> Vec<Delete<'static>>
    where
        F: Fn(&[&GraphqlId]) -> Compare<'static>,
    {
        ids.chunks(Self::PARAMETER_LIMIT)
            .into_iter()
            .map(|chunk| Delete::from_table(table.clone()).so_that(conditions(chunk)))
            .collect()
    }
}
