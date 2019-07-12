use crate::{query_builder::*, TransactionalExt};
use prisma_query::connector::{Mysql, PostgreSql, Sqlite};

pub trait SqlCapabilities {
    /// This we use to differentiate between databases with or without
    /// `ROW_NUMBER` function for related records pagination.
    type ManyRelatedRecordsBuilder: ManyRelatedRecordsQueryBuilder;
}

/// A wrapper for relational databases due to trait restrictions. Implements the
/// needed traits.
pub struct SqlDatabase<T>
where
    T: TransactionalExt + SqlCapabilities,
{
    pub executor: T,
}

impl<T> SqlDatabase<T>
where
    T: TransactionalExt + SqlCapabilities,
{
    pub fn new(executor: T) -> Self {
        Self { executor }
    }
}

impl SqlCapabilities for Sqlite {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl SqlCapabilities for PostgreSql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl SqlCapabilities for Mysql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithUnionAll;
}
