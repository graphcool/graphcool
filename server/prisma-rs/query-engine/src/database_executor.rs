mod sqlite;

use prisma_common::PrismaResult;
use prisma_models::prelude::*;
use prisma_query::ast::Select;
use rusqlite::Row;

pub use sqlite::Sqlite;

pub trait Parseable {
    fn parse_at(&self, typ: TypeIdentifier, index: usize) -> PrismaValue;
}

pub trait DatabaseExecutor {
    fn with_rows<F>(&self, query: Select, db_name: String, mut f: F) -> PrismaResult<Vec<Node>>
    where
        F: FnMut(Box<dyn Parseable>) -> Node;
}
