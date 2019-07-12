use crate::error::SqlError;
use chrono::{DateTime, Utc};
use prisma_models::{GraphqlId, PrismaValue, Record, TypeIdentifier};
use prisma_query::{
    ast::{DatabaseValue, ParameterizedValue},
    connector::ResultRow,
};
use std::{borrow::Borrow, io};
use uuid::Uuid;

/// An allocated representation of a `Row` returned from the database.
#[derive(Debug, Clone, Default)]
pub struct SqlRow {
    pub values: Vec<PrismaValue>,
}

impl From<SqlRow> for Record {
    fn from(row: SqlRow) -> Record {
        Record::new(row.values)
    }
}

pub trait ToSqlRow {
    /// Conversion from a database specific row to an allocated `SqlRow`. To
    /// help deciding the right types, the provided `TypeIdentifier`s should map
    /// to the returned columns in the right order.
    fn to_sql_row<'b>(self, idents: &[TypeIdentifier]) -> crate::Result<SqlRow>;
}

impl ToSqlRow for ResultRow {
    fn to_sql_row<'b>(self, idents: &[TypeIdentifier]) -> crate::Result<SqlRow> {
        let mut row = SqlRow::default();

        for (i, p_value) in self.into_iter().enumerate() {
            let pv = match idents[i] {
                TypeIdentifier::GraphQLID | TypeIdentifier::Relation => match p_value {
                    ParameterizedValue::Text(s) => {
                        let id = Uuid::parse_str(s.borrow())
                            .map(|uuid| GraphqlId::UUID(uuid))
                            .unwrap_or_else(|_| GraphqlId::String(s.into_owned()));

                        PrismaValue::GraphqlId(id)
                    }
                    ParameterizedValue::Integer(i) => PrismaValue::GraphqlId(GraphqlId::Int(i as usize)),
                    ParameterizedValue::Uuid(u) => PrismaValue::GraphqlId(GraphqlId::UUID(u)),
                    _ => {
                        let error =
                            io::Error::new(io::ErrorKind::InvalidData, "ID value not stored as string, int or uuid");
                        return Err(SqlError::ConversionError(error.into()));
                    }
                },
                TypeIdentifier::Enum => match p_value {
                    ParameterizedValue::Text(cow) => PrismaValue::Enum(cow.into_owned()),
                    _ => {
                        let error = io::Error::new(io::ErrorKind::InvalidData, "Enum value not stored as text");
                        return Err(SqlError::ConversionError(error.into()));
                    }
                },
                TypeIdentifier::Json => match p_value {
                    ParameterizedValue::Text(json) => PrismaValue::Json(serde_json::from_str(json.borrow())?),
                    ParameterizedValue::Json(json) => PrismaValue::Json(json),
                    _ => {
                        let error = io::Error::new(io::ErrorKind::InvalidData, "Json value not stored as text or json");
                        return Err(SqlError::ConversionError(error.into()));
                    }
                },
                TypeIdentifier::UUID => match p_value {
                    ParameterizedValue::Text(uuid) => PrismaValue::Uuid(Uuid::parse_str(&uuid)?),
                    ParameterizedValue::Uuid(uuid) => PrismaValue::Uuid(uuid),
                    _ => {
                        let error = io::Error::new(io::ErrorKind::InvalidData, "Uuid value not stored as text or uuid");
                        return Err(SqlError::ConversionError(error.into()));
                    }
                },
                TypeIdentifier::DateTime => match p_value {
                    ParameterizedValue::DateTime(dt) => PrismaValue::DateTime(dt),
                    ParameterizedValue::Integer(ts) => {
                        let nsecs = ((ts % 1000) * 1_000_000) as u32;
                        let secs = (ts / 1000) as i64;
                        let naive = chrono::NaiveDateTime::from_timestamp(secs, nsecs);
                        let datetime: DateTime<Utc> = DateTime::from_utc(naive, Utc);

                        PrismaValue::DateTime(datetime)
                    }
                    ParameterizedValue::Text(dt_string) => {
                        let dt = DateTime::parse_from_rfc3339(dt_string.borrow())
                            .or_else(|_| DateTime::parse_from_rfc2822(dt_string.borrow()))
                            .expect(&format!("Could not parse stored DateTime string: {}", dt_string));

                        PrismaValue::DateTime(dt.with_timezone(&Utc))
                    }
                    _ => {
                        let error = io::Error::new(
                            io::ErrorKind::InvalidData,
                            "DateTime value not stored as datetime, int or text",
                        );
                        return Err(SqlError::ConversionError(error.into()));
                    }
                },
                _ => PrismaValue::from(p_value),
            };

            row.values.push(pv);
        }

        Ok(row)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum SqlId {
    String(String),
    Int(usize),
    UUID(Uuid),
}

impl From<SqlId> for GraphqlId {
    fn from(sql_id: SqlId) -> Self {
        match sql_id {
            SqlId::String(s) => GraphqlId::String(s),
            SqlId::Int(i) => GraphqlId::Int(i),
            SqlId::UUID(u) => GraphqlId::UUID(u),
        }
    }
}

impl From<SqlId> for DatabaseValue<'static> {
    fn from(id: SqlId) -> Self {
        match id {
            SqlId::String(s) => s.into(),
            SqlId::Int(i) => (i as i64).into(),
            SqlId::UUID(u) => u.into(),
        }
    }
}

impl From<&SqlId> for DatabaseValue<'static> {
    fn from(id: &SqlId) -> Self {
        id.clone().into()
    }
}
