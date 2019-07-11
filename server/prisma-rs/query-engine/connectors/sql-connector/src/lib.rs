//! # The SQL Connector interface
//!
//! The public interface to outside is split into separate traits:
//!
//! - [DatabaseReader](../connector/trait.DatabaseReader.html) to fetch data.
//! - [DatabaseWriter](../connector/trait.DatabaseWriter.html) to write
//!   data.

mod cursor_condition;
mod database;
mod error;
mod filter_conversion;
mod ordering;
mod query_builder;
mod raw_query;
mod row;
mod transactional;
mod write_query;

use filter_conversion::*;
use raw_query::*;
use row::*;

pub use database::*;
pub use error::SqlError;
pub use transactional::*;

type Result<T> = std::result::Result<T, error::SqlError>;
