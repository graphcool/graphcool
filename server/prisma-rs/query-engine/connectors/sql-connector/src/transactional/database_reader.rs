use crate::{
    database::{SqlCapabilities, SqlDatabase},
    error::SqlError,
    query_builder::{ManyRelatedRecordsBaseQuery, ManyRelatedRecordsQueryBuilder, QueryBuilder},
    TransactionalExt,
};
use connector::{self, error::ConnectorError, filter::RecordFinder, *};
use itertools::Itertools;
use prisma_models::*;
use std::convert::TryFrom;

struct ScalarListElement {
    record_id: GraphqlId,
    value: PrismaValue,
}

impl<T> DatabaseReader for SqlDatabase<T>
where
    T: TransactionalExt + SqlCapabilities,
{
    fn get_single_record(
        &self,
        record_finder: &RecordFinder,
        selected_fields: &SelectedFields,
    ) -> connector::Result<Option<SingleRecord>> {
        let db_name = &record_finder.field.model().internal_data_model().db_name;
        let query = QueryBuilder::get_records(record_finder.field.model(), selected_fields, record_finder);
        let field_names = selected_fields.names();
        let idents = selected_fields.type_identifiers();

        let record = self
            .executor
            .with_tx_ext(db_name, |conn| match conn.find(query, idents.as_slice()) {
                Ok(result) => Ok(Some(result)),
                Err(_e @ SqlError::RecordNotFoundForWhere(_)) => Ok(None),
                Err(e) => Err(e),
            })?
            .map(Record::from)
            .map(|record| SingleRecord {
                record: record,
                field_names,
            });

        Ok(record)
    }

    fn get_many_records(
        &self,
        model: ModelRef,
        query_arguments: QueryArguments,
        selected_fields: &SelectedFields,
    ) -> connector::Result<ManyRecords> {
        let db_name = &model.internal_data_model().db_name;
        let field_names = selected_fields.names();
        let idents = selected_fields.type_identifiers();
        let query = QueryBuilder::get_records(model, selected_fields, query_arguments);

        let records = self
            .executor
            .with_tx_ext(db_name, |conn| conn.filter(query.into(), idents.as_slice()))?
            .into_iter()
            .map(Record::from)
            .collect();

        Ok(ManyRecords { records, field_names })
    }

    fn get_related_records(
        &self,
        from_field: RelationFieldRef,
        from_record_ids: &[GraphqlId],
        query_arguments: QueryArguments,
        selected_fields: &SelectedFields,
    ) -> connector::Result<ManyRecords> {
        let db_name = &from_field.model().internal_data_model().db_name;
        let idents = selected_fields.type_identifiers();
        let field_names = selected_fields.names();

        let query = {
            let is_with_pagination = query_arguments.is_with_pagination();
            let base = ManyRelatedRecordsBaseQuery::new(from_field, from_record_ids, query_arguments, selected_fields);

            if is_with_pagination {
                T::ManyRelatedRecordsBuilder::with_pagination(base)
            } else {
                T::ManyRelatedRecordsBuilder::without_pagination(base)
            }
        };

        let records: connector::Result<Vec<Record>> = self
            .executor
            .with_tx_ext(db_name, |conn| conn.filter(query, idents.as_slice()))?
            .into_iter()
            .map(|mut row| {
                let parent_id = row.values.pop().ok_or(ConnectorError::ColumnDoesNotExist)?;

                // Relation id is always the second last value. We don't need it
                // here and we don't need it in the record.
                let _ = row.values.pop();

                let mut record = Record::from(row);
                record.add_parent_id(GraphqlId::try_from(parent_id)?);

                Ok(record)
            })
            .collect();

        Ok(ManyRecords {
            records: records?,
            field_names,
        })
    }

    fn count_by_model(&self, model: ModelRef, query_arguments: QueryArguments) -> connector::Result<usize> {
        let db_name = &model.internal_data_model().db_name;
        let query = QueryBuilder::count_by_model(model, query_arguments);

        let result = self
            .executor
            .with_tx_ext(db_name, |conn| conn.find_int(query))
            .map(|count| count as usize)?;

        Ok(result)
    }

    fn count_by_table(&self, database: &str, table: &str) -> connector::Result<usize> {
        let query = QueryBuilder::count_by_table(database, table);

        let result = self
            .executor
            .with_tx_ext(database, |conn| conn.find_int(query))
            .map(|count| count as usize)?;

        Ok(result)
    }

    fn get_scalar_list_values_by_record_ids(
        &self,
        list_field: ScalarFieldRef,
        record_ids: Vec<GraphqlId>,
    ) -> connector::Result<Vec<ScalarListValues>> {
        let db_name = &list_field.model().internal_data_model().db_name;
        let type_identifier = list_field.type_identifier;
        let query = QueryBuilder::get_scalar_list_values_by_record_ids(list_field, record_ids);

        let results: Vec<ScalarListElement> = self.executor.with_tx_ext(db_name, |conn| {
            let rows = conn.filter(query.into(), &[TypeIdentifier::GraphQLID, type_identifier])?;

            rows.into_iter()
                .map(|row| {
                    let mut iter = row.values.into_iter();

                    let record_id = iter.next().ok_or(SqlError::ColumnDoesNotExist)?;
                    let value = iter.next().ok_or(SqlError::ColumnDoesNotExist)?;

                    Ok(ScalarListElement {
                        record_id: GraphqlId::try_from(record_id)?,
                        value: value,
                    })
                })
                .collect()
        })?;

        let mut list_values = Vec::new();

        for (record_id, elements) in &results.into_iter().group_by(|ele| ele.record_id.clone()) {
            let values = ScalarListValues {
                record_id,
                values: elements.into_iter().map(|e| e.value).collect(),
            };
            list_values.push(values);
        }

        Ok(list_values)
    }
}
