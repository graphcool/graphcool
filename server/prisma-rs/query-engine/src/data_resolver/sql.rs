use super::DataResolver;
use crate::{
    database_executor::{DatabaseExecutor, Parseable},
    node_selector::NodeSelector,
    protobuf::prelude::*,
    query_builder::QueryBuilder,
};
use chrono::{DateTime, Utc};
use prisma_common::PrismaResult;
use prisma_models::prelude::*;

pub struct SqlResolver<T>
where
    T: DatabaseExecutor,
{
    database_executor: T,
}

impl<T> DataResolver for SqlResolver<T>
where
    T: DatabaseExecutor,
{
    fn get_node_by_where(
        &self,
        node_selector: NodeSelector,
        selected_fields: SelectedFields,
    ) -> PrismaResult<Option<SingleNode>> {
        let (db_name, query) = QueryBuilder::get_node_by_where(node_selector, &selected_fields);
        let scalar_fields = selected_fields.scalar_non_list();
        let field_names = scalar_fields.iter().map(|f| f.name.clone()).collect();

        let nodes = self
            .database_executor
            .with_rows(query, db_name, |row| Self::read_row(row, &selected_fields))?;

        let result = nodes.into_iter().next().map(|node| SingleNode { node, field_names });

        Ok(result)
    }

    fn get_nodes(
        &self,
        model: ModelRef,
        query_arguments: QueryArguments,
        selected_fields: SelectedFields,
    ) -> PrismaResult<ManyNodes> {
        let scalar_fields = selected_fields.scalar_non_list();
        let field_names = scalar_fields.iter().map(|f| f.name.clone()).collect();
        let (db_name, query) = QueryBuilder::get_nodes(model, query_arguments, &selected_fields);

        let nodes = self
            .database_executor
            .with_rows(query, db_name, |row| Self::read_row(row, &selected_fields))?;

        let result = ManyNodes { nodes, field_names };

        Ok(result)
    }

    fn get_related_nodes(
        &self,
        from_field: RelationFieldRef,
        from_node_ids: Vec<GraphqlId>,
        query_arguments: QueryArguments,
        selected_fields: SelectedFields,
    ) -> PrismaResult<ManyNodes> {
        let scalar_fields = selected_fields.scalar_non_list();
        let field_names = scalar_fields.iter().map(|f| f.name.clone()).collect();
        let (db_name, query) =
            QueryBuilder::get_related_nodes(from_field, from_node_ids, query_arguments, &selected_fields);

        let nodes = self.database_executor.with_rows(query, db_name, |row| {
            let mut node = Self::read_row(row, &selected_fields);
            let position = scalar_fields.len();

            //node.add_related_id(row.get(position));
            //node.add_parent_id(row.get(position + 1));

            node
        })?;

        let result = ManyNodes { nodes, field_names };

        Ok(result)
    }
}

impl<T> SqlResolver<T>
where
    T: DatabaseExecutor,
{
    pub fn new(database_executor: T) -> Self {
        Self { database_executor }
    }

    fn read_row(row: Box<dyn Parseable>, selected_fields: &SelectedFields) -> Node {
        let fields = selected_fields
            .scalar_non_list()
            .iter()
            .enumerate()
            .map(|(i, sf)| row.parse_at(sf.type_identifier, i))
            .collect();

        Node::new(fields)
    }
}
