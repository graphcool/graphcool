mod create;
mod delete;
mod delete_many;
mod nested;
mod relation;
mod update;
mod update_many;

use crate::{
    database::{SqlCapabilities, SqlDatabase},
    error::SqlError,
    write_query::WriteQueryBuilder,
    RawQuery, TransactionExt, TransactionalExt,
};
use connector::{self, write_query::*, DatabaseWriter};
use serde_json::Value;
use std::sync::Arc;

impl<T> DatabaseWriter for SqlDatabase<T>
where
    T: TransactionalExt + SqlCapabilities,
{
    fn execute(&self, db_name: String, write_query: RootWriteQuery) -> connector::Result<WriteQueryResult> {
        let result = self.executor.with_tx_ext(&db_name, |conn: &mut TransactionExt| {
            fn create(conn: &mut TransactionExt, cn: &CreateRecord) -> crate::Result<WriteQueryResult> {
                let parent_id = create::execute(conn, Arc::clone(&cn.model), &cn.non_list_args, &cn.list_args)?;
                nested::execute(conn, &cn.nested_writes, &parent_id)?;

                Ok(WriteQueryResult {
                    identifier: Identifier::Id(parent_id),
                    typ: WriteQueryResultType::Create,
                })
            }

            fn update(conn: &mut TransactionExt, un: &UpdateRecord) -> crate::Result<WriteQueryResult> {
                let parent_id = update::execute(conn, &un.where_, &un.non_list_args, &un.list_args)?;
                nested::execute(conn, &un.nested_writes, &parent_id)?;

                Ok(WriteQueryResult {
                    identifier: Identifier::Id(parent_id),
                    typ: WriteQueryResultType::Update,
                })
            }

            match write_query {
                RootWriteQuery::CreateRecord(ref cn) => Ok(create(conn, cn)?),
                RootWriteQuery::UpdateRecord(ref un) => Ok(update(conn, un)?),
                RootWriteQuery::UpsertRecord(ref ups) => match conn.find_id(&ups.where_) {
                    Err(_e @ SqlError::RecordNotFoundForWhere { .. }) => Ok(create(conn, &ups.create)?),
                    Err(e) => return Err(e.into()),
                    Ok(_) => Ok(update(conn, &ups.update)?),
                },
                RootWriteQuery::UpdateManyRecords(ref uns) => {
                    let count = update_many::execute(
                        conn,
                        Arc::clone(&uns.model),
                        &uns.filter,
                        &uns.non_list_args,
                        &uns.list_args,
                    )?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Count(count),
                        typ: WriteQueryResultType::Many,
                    })
                }
                RootWriteQuery::DeleteRecord(ref dn) => {
                    let record = delete::execute(conn, &dn.where_)?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Record(record),
                        typ: WriteQueryResultType::Delete,
                    })
                }
                RootWriteQuery::DeleteManyRecords(ref dns) => {
                    let count = delete_many::execute(conn, Arc::clone(&dns.model), &dns.filter)?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::Count(count),
                        typ: WriteQueryResultType::Many,
                    })
                }
                RootWriteQuery::ResetData(ref rd) => {
                    let tables = WriteQueryBuilder::truncate_tables(Arc::clone(&rd.internal_data_model));
                    conn.empty_tables(tables)?;

                    Ok(WriteQueryResult {
                        identifier: Identifier::None,
                        typ: WriteQueryResultType::Unit,
                    })
                }
            }
        })?;

        Ok(result)
    }

    fn execute_raw(&self, db_name: String, query: String) -> connector::Result<Value> {
        let result = self.executor.with_tx_ext(&db_name, |conn: &mut TransactionExt| {
            conn.raw_json(RawQuery::from(query))
        })?;

        Ok(result)
    }
}
