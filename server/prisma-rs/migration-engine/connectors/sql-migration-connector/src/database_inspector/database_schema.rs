use datamodel::Value;

#[derive(Debug, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub tables: Vec<Table>,
}

impl DatabaseSchema {
    pub fn table(&self, name: &str) -> Result<&Table, String> {
        match self.tables.iter().find(|t| t.name == name) {
            Some(t) => Ok(t),
            None => Err(format!("Table {} not found", name)),
        }
    }

    pub fn table_bang(&self, name: &str) -> &Table {
        self.table(&name).unwrap()
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.table(name).is_ok()
    }

    pub fn empty() -> DatabaseSchema {
        DatabaseSchema { tables: Vec::new() }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub primary_key_columns: Vec<String>,
}

impl Table {
    pub fn column_bang(&self, name: &str) -> &Column {
        self.column(name)
            .expect(&format!("Column {} not found in Table {}", name, self.name))
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.column(name).is_some()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub tpe: ColumnType,
    pub is_required: bool,
    pub foreign_key: Option<ForeignKey>,
    pub sequence: Option<Sequence>,
    pub default: Option<Value>,
}

impl Column {
    pub fn differs_in_something_except_default(&self, other: &Column) -> bool {
        self.name != other.name
            || self.tpe != other.tpe
            || self.is_required != other.is_required
            || self.foreign_key != other.foreign_key
            || self.sequence != other.sequence
    }
}

impl Column {
    pub fn new(name: String, tpe: ColumnType, is_required: bool) -> Column {
        Column {
            name,
            tpe,
            is_required,
            foreign_key: None,
            sequence: None,
            default: None,
        }
    }

    pub fn with_foreign_key(name: String, tpe: ColumnType, is_required: bool, foreign_key: ForeignKey) -> Column {
        Column {
            name,
            tpe,
            is_required,
            foreign_key: Some(foreign_key),
            sequence: None,
            default: None,
        }
    }
}

#[derive(Debug, Copy, PartialEq, Clone)]
pub enum ColumnType {
    Int,
    Float,
    Boolean,
    String,
    DateTime,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub name: Option<String>,
    pub table: String,
    pub column: String,
}

impl PartialEq for ForeignKey {
    fn eq(&self, other: &ForeignKey) -> bool {
        self.table == other.table && self.column == other.column
    }
}

impl ForeignKey {
    pub fn new(table: String, column: String) -> ForeignKey {
        ForeignKey {
            name: None,
            table,
            column,
        }
    }

    pub fn with_foreign_key(name: String, table: String, column: String) -> ForeignKey {
        ForeignKey {
            name: Some(name),
            table,
            column,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Sequence {
    pub name: String,
    pub current: u32,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub tpe: IndexType,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IndexType {
    // can later add fulltext or custom ones
    Unique,
    Normal,
}
