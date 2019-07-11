use crate::{DomainError, DomainResult, ScalarFieldRef, TypeIdentifier};
use chrono::prelude::*;
use graphql_parser::query::{Number, Value as GraphqlValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{convert::TryFrom, fmt, string::FromUtf8Error};
use uuid::Uuid;

#[cfg(feature = "sql")]
use prisma_query::ast::*;

pub type PrismaListValue = Option<Vec<PrismaValue>>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone)]
pub enum GraphqlId {
    String(String),
    Int(usize),
    UUID(Uuid),
}

#[cfg(feature = "sql")]
impl From<Id> for GraphqlId {
    fn from(id: Id) -> Self {
        match id {
            Id::String(s) => GraphqlId::String(s),
            Id::Int(i) => GraphqlId::Int(i),
            Id::UUID(u) => GraphqlId::UUID(u),
        }
    }
}

impl GraphqlId {
    pub fn to_value(&self) -> GraphqlValue {
        match self {
            GraphqlId::String(s) => GraphqlValue::String(s.clone()),
            GraphqlId::Int(i) => GraphqlValue::Int(Number::from((*i) as i32)), // This could cause issues!
            GraphqlId::UUID(u) => GraphqlValue::String(u.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag = "gcValueType", content = "value")]
pub enum PrismaValue {
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "float")]
    Float(f64),
    #[serde(rename = "bool")]
    Boolean(bool),
    #[serde(rename = "datetime")]
    DateTime(DateTime<Utc>),
    #[serde(rename = "enum")]
    Enum(String),
    #[serde(rename = "json")]
    Json(Value),
    #[serde(rename = "int")]
    Int(i64),
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "uuid")]
    Uuid(Uuid),
    #[serde(rename = "graphQlId")]
    GraphqlId(GraphqlId),
    #[serde(rename = "list")]
    List(PrismaListValue),
}

impl PrismaValue {
    pub fn is_null(&self) -> bool {
        match self {
            PrismaValue::Null => true,
            _ => false,
        }
    }

    // this function is broken because it does not operate on the type identifier of a field. It is just guessing.
    pub fn from_value_broken(v: &GraphqlValue) -> Self {
        match v {
            GraphqlValue::Boolean(b) => PrismaValue::Boolean(b.clone()),
            GraphqlValue::Enum(e) => PrismaValue::Enum(e.clone()),
            GraphqlValue::Float(f) => PrismaValue::Float(f.clone()),
            GraphqlValue::Int(i) => PrismaValue::Int(i.as_i64().unwrap()),
            GraphqlValue::Null => PrismaValue::Null,
            GraphqlValue::String(s) => Self::str_as_json(s)
                .or_else(|| Self::str_as_datetime(s))
                .unwrap_or(PrismaValue::String(s.clone())),
            GraphqlValue::List(l) => PrismaValue::List(Some(l.iter().map(|i| Self::from_value_broken(i)).collect())),
            GraphqlValue::Object(obj) if obj.contains_key("set") => Self::from_value_broken(obj.get("set").unwrap()),
            value => panic!(format!("Unable to make {:?} to PrismaValue", value)),
        }
    }

    pub fn from_value(value: &GraphqlValue, field: &ScalarFieldRef) -> Self {
        match (value, field.type_identifier) {
            (GraphqlValue::Boolean(b), TypeIdentifier::Boolean) => PrismaValue::Boolean(b.clone()),
            (GraphqlValue::Enum(e), TypeIdentifier::Enum) => PrismaValue::Enum(e.clone()),
            (GraphqlValue::Float(f), TypeIdentifier::Float) => PrismaValue::Float(f.clone()),
            (GraphqlValue::Int(i), TypeIdentifier::Int) => PrismaValue::Int(i.as_i64().unwrap()),
            (GraphqlValue::Null, _) => PrismaValue::Null,
            (GraphqlValue::String(s), TypeIdentifier::String) => PrismaValue::String(s.clone()),
            (GraphqlValue::String(s), TypeIdentifier::GraphQLID) => {
                PrismaValue::GraphqlId(GraphqlId::String(s.clone()))
            }
            //            (GraphqlValue::Int(s), TypeIdentifier::GraphQLID) => PrismaValue::GraphqlId(GraphqlId::Int(s)),
            (GraphqlValue::String(s), TypeIdentifier::Json) => Self::str_as_json(s).expect("received invalid json"),
            (GraphqlValue::String(s), TypeIdentifier::DateTime) => {
                Self::str_as_datetime(s).expect("received invalid datetime")
            }
            (GraphqlValue::List(l), _) => {
                PrismaValue::List(Some(l.iter().map(|i| Self::from_value(i, &field)).collect()))
            }
            (GraphqlValue::Object(obj), _) if obj.contains_key("set") => {
                Self::from_value_broken(obj.get("set").unwrap())
            }
            (value, _) => panic!(format!(
                "Unable to make {:?} to PrismaValue. Field was {} with type identifier {:?}",
                value, field.name, field.type_identifier
            )),
        }
    }

    fn str_as_json(s: &str) -> Option<PrismaValue> {
        serde_json::from_str(s).ok().map(|j| PrismaValue::Json(j))
    }

    // If you look at this and think: "What's up with Z?" then you're asking the right question.
    // Feel free to try and fix it for cases with AND without Z.
    fn str_as_datetime(s: &str) -> Option<PrismaValue> {
        let fmt = "%Y-%m-%dT%H:%M:%S%.3f";
        Utc.datetime_from_str(s.trim_end_matches("Z"), fmt)
            .ok()
            .map(|dt| PrismaValue::DateTime(DateTime::<Utc>::from_utc(dt.naive_utc(), Utc)))
    }
}

impl fmt::Display for PrismaValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PrismaValue::String(x) => x.fmt(f),
            PrismaValue::Float(x) => x.fmt(f),
            PrismaValue::Boolean(x) => x.fmt(f),
            PrismaValue::DateTime(x) => x.fmt(f),
            PrismaValue::Enum(x) => x.fmt(f),
            PrismaValue::Json(x) => x.fmt(f),
            PrismaValue::Int(x) => x.fmt(f),
            PrismaValue::Null => "null".fmt(f),
            PrismaValue::Uuid(x) => x.fmt(f),
            PrismaValue::GraphqlId(x) => match x {
                GraphqlId::String(x) => x.fmt(f),
                GraphqlId::Int(x) => x.fmt(f),
                GraphqlId::UUID(x) => x.fmt(f),
            },
            PrismaValue::List(x) => {
                let as_string = format!("{:?}", x);
                as_string.fmt(f)
            }
        }
    }
}

impl From<&str> for PrismaValue {
    fn from(s: &str) -> Self {
        PrismaValue::from(s.to_string())
    }
}

impl From<String> for PrismaValue {
    fn from(s: String) -> Self {
        PrismaValue::String(s)
    }
}

impl From<f64> for PrismaValue {
    fn from(s: f64) -> Self {
        PrismaValue::Float(s)
    }
}

impl From<f32> for PrismaValue {
    fn from(s: f32) -> Self {
        PrismaValue::Float(s as f64)
    }
}

impl From<bool> for PrismaValue {
    fn from(s: bool) -> Self {
        PrismaValue::Boolean(s)
    }
}

impl From<i32> for PrismaValue {
    fn from(s: i32) -> Self {
        PrismaValue::Int(s as i64)
    }
}

impl From<i64> for PrismaValue {
    fn from(s: i64) -> Self {
        PrismaValue::Int(s)
    }
}

impl From<Uuid> for PrismaValue {
    fn from(s: Uuid) -> Self {
        PrismaValue::Uuid(s)
    }
}

impl From<PrismaListValue> for PrismaValue {
    fn from(s: PrismaListValue) -> Self {
        PrismaValue::List(s)
    }
}

impl From<GraphqlId> for PrismaValue {
    fn from(id: GraphqlId) -> PrismaValue {
        PrismaValue::GraphqlId(id)
    }
}

impl From<&GraphqlId> for PrismaValue {
    fn from(id: &GraphqlId) -> PrismaValue {
        PrismaValue::GraphqlId(id.clone())
    }
}

impl TryFrom<PrismaValue> for PrismaListValue {
    type Error = DomainError;

    fn try_from(s: PrismaValue) -> DomainResult<PrismaListValue> {
        match s {
            PrismaValue::List(l) => Ok(l),
            PrismaValue::Null => Ok(None),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "PrismaListValue")),
        }
    }
}

impl TryFrom<PrismaValue> for GraphqlId {
    type Error = DomainError;

    fn try_from(value: PrismaValue) -> DomainResult<GraphqlId> {
        match value {
            PrismaValue::GraphqlId(id) => Ok(id),
            PrismaValue::Int(i) => Ok(GraphqlId::from(i)),
            PrismaValue::String(s) => Ok(GraphqlId::from(s)),
            PrismaValue::Uuid(u) => Ok(GraphqlId::from(u)),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "GraphqlId")),
        }
    }
}

impl TryFrom<&PrismaValue> for GraphqlId {
    type Error = DomainError;

    fn try_from(value: &PrismaValue) -> DomainResult<GraphqlId> {
        match value {
            PrismaValue::GraphqlId(id) => Ok(id.clone()),
            PrismaValue::Int(i) => Ok(GraphqlId::from(*i)),
            PrismaValue::String(s) => Ok(GraphqlId::from(s.clone())),
            PrismaValue::Uuid(u) => Ok(GraphqlId::from(u.clone())),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "GraphqlId")),
        }
    }
}

impl TryFrom<PrismaValue> for i64 {
    type Error = DomainError;

    fn try_from(value: PrismaValue) -> DomainResult<i64> {
        match value {
            PrismaValue::Int(i) => Ok(i),
            _ => Err(DomainError::ConversionFailure("PrismaValue", "i64")),
        }
    }
}

#[cfg(feature = "sql")]
impl<'a> From<GraphqlId> for DatabaseValue<'a> {
    fn from(id: GraphqlId) -> Self {
        match id {
            GraphqlId::String(s) => s.into(),
            GraphqlId::Int(i) => (i as i64).into(),
            GraphqlId::UUID(u) => u.into(),
        }
    }
}

#[cfg(feature = "sql")]
impl<'a> From<&GraphqlId> for DatabaseValue<'a> {
    fn from(id: &GraphqlId) -> Self {
        id.clone().into()
    }
}

#[cfg(feature = "sql")]
impl<'a> From<PrismaValue> for DatabaseValue<'a> {
    fn from(pv: PrismaValue) -> Self {
        match pv {
            PrismaValue::String(s) => s.into(),
            PrismaValue::Float(f) => (f as f64).into(),
            PrismaValue::Boolean(b) => b.into(),
            PrismaValue::DateTime(d) => d.into(),
            PrismaValue::Enum(e) => e.into(),
            PrismaValue::Json(j) => j.to_string().into(),
            PrismaValue::Int(i) => (i as i64).into(),
            PrismaValue::Null => DatabaseValue::Parameterized(ParameterizedValue::Null),
            PrismaValue::Uuid(u) => u.into(),
            PrismaValue::GraphqlId(id) => id.into(),
            PrismaValue::List(Some(l)) => l.into(),
            PrismaValue::List(_) => panic!("List values are not supported here"),
        }
    }
}

#[cfg(feature = "sql")]
impl<'a> From<ParameterizedValue<'a>> for PrismaValue {
    fn from(pv: ParameterizedValue<'a>) -> Self {
        match pv {
            ParameterizedValue::Null => PrismaValue::Null,
            ParameterizedValue::Integer(i) => PrismaValue::Int(i),
            ParameterizedValue::Real(f) => PrismaValue::Float(f),
            ParameterizedValue::Text(s) => PrismaValue::String(s.into_owned()),
            ParameterizedValue::Boolean(b) => PrismaValue::Boolean(b),
            ParameterizedValue::Array(v) => {
                let lst = v.into_iter().map(PrismaValue::from).collect();
                PrismaValue::List(Some(lst))
            }
            ParameterizedValue::Json(val) => PrismaValue::Json(val),
            ParameterizedValue::Uuid(uuid) => PrismaValue::Uuid(uuid),
            ParameterizedValue::DateTime(dt) => PrismaValue::DateTime(dt),
        }
    }
}

impl From<&str> for GraphqlId {
    fn from(s: &str) -> Self {
        GraphqlId::from(s.to_string())
    }
}

impl From<String> for GraphqlId {
    fn from(s: String) -> Self {
        GraphqlId::String(s)
    }
}

impl TryFrom<Vec<u8>> for GraphqlId {
    type Error = FromUtf8Error;

    fn try_from(v: Vec<u8>) -> Result<GraphqlId, Self::Error> {
        Ok(GraphqlId::String(String::from_utf8(v)?))
    }
}

impl From<usize> for GraphqlId {
    fn from(id: usize) -> Self {
        GraphqlId::Int(id)
    }
}

impl From<i64> for GraphqlId {
    fn from(id: i64) -> Self {
        GraphqlId::Int(id as usize)
    }
}

impl From<u64> for GraphqlId {
    fn from(id: u64) -> Self {
        GraphqlId::Int(id as usize)
    }
}

impl From<Uuid> for GraphqlId {
    fn from(uuid: Uuid) -> Self {
        GraphqlId::UUID(uuid)
    }
}
