pub mod parser;
pub mod reformat;
pub mod renderer;
pub mod string_builder;
pub mod table;

/// AST representation of a prisma datamodel
///
/// This module is used internally to represent an AST. The AST's nodes can be used
/// during validation of a schema, especially when implementing custom directives.
///
/// The AST is not validated, also fields and directives are not resolved. Every node is
/// annotated with it's location in the text representation.
/// Basically, the AST is an object oriented representation of the datamodel's text.

/// Represents a location in a datamodel's text representation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Span {
        Span { start, end }
    }

    // Creates a new empty span.
    pub fn empty() -> Span {
        Span { start: 0, end: 0 }
    }
    /// Creates a new ast::Span from a pest::Span.
    pub fn from_pest(s: &pest::Span) -> Span {
        Span {
            start: s.start(),
            end: s.end(),
        }
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{} - {}]", self.start, self.end)
    }
}

pub trait WithSpan {
    fn span(&self) -> &Span;
}

trait WithKeyValueConfig {
    fn properties(&self) -> &Vec<Argument>;
}

#[derive(Debug, Clone)]
pub struct Identifier {
    pub name: String,
    pub span: Span,
}

impl Identifier {
    pub fn new(name: &str) -> Identifier {
        Identifier {
            name: String::from(name),
            span: Span::empty(),
        }
    }
}

impl WithSpan for Identifier {
    fn span(&self) -> &Span {
        &self.span
    }
}

pub trait WithIdentifier {
    fn identifier(&self) -> &Identifier;
}

pub trait WithName {
    fn name(&self) -> &str;
}

impl<T> WithName for T
where
    T: WithIdentifier,
{
    fn name(&self) -> &str {
        &self.identifier().name
    }
}

/// The arity of a field.
#[derive(Debug)]
pub enum FieldArity {
    Required,
    Optional,
    List,
}

/// A comment. Currently unimplemented.
#[derive(Debug, Clone)]
pub struct Comment {
    /// The comment text
    pub text: String,
}

/// An argument, either for directives, or for keys in source blocks.
#[derive(Debug, Clone)]
pub struct Argument {
    /// Name of the argument.
    pub name: Identifier,
    /// Argument value.
    pub value: Value,
    /// Location of the argument in the text representation.
    pub span: Span,
}

impl WithIdentifier for Argument {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl Argument {
    pub fn new_string(name: &str, value: &str) -> Argument {
        Argument {
            name: Identifier::new(name),
            value: Value::StringValue(String::from(value), Span::empty()),
            span: Span::empty(),
        }
    }

    pub fn new_constant(name: &str, value: &str) -> Argument {
        Argument {
            name: Identifier::new(name),
            value: Value::ConstantValue(String::from(value), Span::empty()),
            span: Span::empty(),
        }
    }

    pub fn new_array(name: &str, value: Vec<Value>) -> Argument {
        Argument {
            name: Identifier::new(name),
            value: Value::Array(value, Span::empty()),
            span: Span::empty(),
        }
    }

    pub fn new(name: &str, value: Value) -> Argument {
        Argument {
            name: Identifier::new(name),
            value: value,
            span: Span::empty(),
        }
    }
}

// TODO: Rename to expression.
/// Represents arbitrary, even nested, expressions.
#[derive(Debug, Clone)]
pub enum Value {
    /// Any numeric value e.g. floats or ints.
    NumericValue(String, Span),
    /// Any boolean value.
    BooleanValue(String, Span),
    /// Any string value.
    StringValue(String, Span),
    /// A ducktyped string value, used as function return values which can be ducktyped.
    /// Canbe any scalar type, array or function is not possible.
    Any(String, Span),
    /// Any literal constant, basically a string which was not inside "...".
    /// This is used for representing builtin enums.
    ConstantValue(String, Span),
    /// A function with a name and arguments, which is evaluated at client side.
    Function(String, Vec<Value>, Span),
    /// An array of other values.
    Array(Vec<Value>, Span),
}

impl Value {
    pub fn with_lifted_span(&self, offset: usize) -> Value {
        match self {
            Value::NumericValue(v, s) => Value::NumericValue(v.clone(), lift_span(&s, offset)),
            Value::BooleanValue(v, s) => Value::BooleanValue(v.clone(), lift_span(&s, offset)),
            Value::StringValue(v, s) => Value::StringValue(v.clone(), lift_span(&s, offset)),
            Value::ConstantValue(v, s) => Value::ConstantValue(v.clone(), lift_span(&s, offset)),
            Value::Function(v, a, s) => Value::Function(
                v.clone(),
                a.iter().map(|elem| elem.with_lifted_span(offset)).collect(),
                lift_span(&s, offset),
            ),
            Value::Array(v, s) => Value::Array(
                v.iter().map(|elem| elem.with_lifted_span(offset)).collect(),
                lift_span(&s, offset),
            ),
            Value::Any(v, s) => Value::Any(v.clone(), lift_span(&s, offset)),
        }
    }
}

/// Adds an offset to a span.
pub fn lift_span(span: &Span, offset: usize) -> Span {
    Span {
        start: offset + span.start,
        end: offset + span.end,
    }
}

/// Creates a friendly readable representation for a value's type.
pub fn describe_value_type(val: &Value) -> &'static str {
    match val {
        Value::NumericValue(_, _) => "numeric",
        Value::BooleanValue(_, _) => "boolean",
        Value::StringValue(_, _) => "string",
        Value::ConstantValue(_, _) => "literal",
        Value::Function(_, _, _) => "functional",
        Value::Array(_, _) => "array",
        Value::Any(_, _) => "any",
    }
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::StringValue(x, _) => x.clone(),
            Value::NumericValue(x, _) => x.clone(),
            Value::BooleanValue(x, _) => x.clone(),
            Value::ConstantValue(x, _) => x.clone(),
            Value::Function(x, _, _) => x.clone(),
            Value::Array(_, _) => String::from("(array)"),
            Value::Any(x, _) => x.clone(),
        }
    }
}

/// A directive.
#[derive(Debug, Clone)]
pub struct Directive {
    pub name: Identifier,
    pub arguments: Vec<Argument>,
    pub span: Span,
}

impl Directive {
    pub fn new(name: &str, arguments: Vec<Argument>) -> Directive {
        Directive {
            name: Identifier::new(name),
            arguments: arguments,
            span: Span::empty(),
        }
    }
}

impl WithIdentifier for Directive {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithSpan for Directive {
    fn span(&self) -> &Span {
        &self.span
    }
}

/// Trait for an AST node which can have directives.
pub trait WithDirectives {
    fn directives(&self) -> &Vec<Directive>;
}

/// Trait for an AST node which can have comments.
pub trait WithDocumentation {
    fn documentation(&self) -> &Option<Comment>;
}

/// A field declaration.
#[derive(Debug)]
pub struct Field {
    /// The field's type.
    pub field_type: Identifier,
    /// The name of the field.
    pub name: Identifier,
    /// The aritiy of the field.
    pub arity: FieldArity,
    /// The default value of the field.
    pub default_value: Option<Value>,
    /// The directives of this field.
    pub directives: Vec<Directive>,
    /// The comments for this field.
    pub documentation: Option<Comment>,
    /// The location of this field in the text representation.
    pub span: Span,
}

impl WithIdentifier for Field {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithSpan for Field {
    fn span(&self) -> &Span {
        &self.span
    }
}

impl WithDirectives for Field {
    fn directives(&self) -> &Vec<Directive> {
        &self.directives
    }
}

impl WithDocumentation for Field {
    fn documentation(&self) -> &Option<Comment> {
        &self.documentation
    }
}

/// An enum declaration.
#[derive(Debug)]
pub struct Enum {
    /// The name of the enum.
    pub name: Identifier,
    /// The values of the enum.
    pub values: Vec<EnumValue>,
    /// The directives of this enum.
    pub directives: Vec<Directive>,
    /// The comments for this enum.
    pub documentation: Option<Comment>,
    /// The location of this enum in the text representation.
    pub span: Span,
}

impl WithIdentifier for Enum {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithSpan for Enum {
    fn span(&self) -> &Span {
        &self.span
    }
}

impl WithDirectives for Enum {
    fn directives(&self) -> &Vec<Directive> {
        &self.directives
    }
}

impl WithDocumentation for Enum {
    fn documentation(&self) -> &Option<Comment> {
        &self.documentation
    }
}

/// An enum value definition.
#[derive(Debug)]
pub struct EnumValue {
    /// The name of the enum value.
    pub name: String,
    /// The location of this enum value in the text representation.
    pub span: Span,
    pub documentation: Option<Comment>,
}

impl WithName for EnumValue {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithSpan for EnumValue {
    fn span(&self) -> &Span {
        &self.span
    }
}

/// A model declaration.
#[derive(Debug)]
pub struct Model {
    /// The name of the model.
    pub name: Identifier,
    /// The fields of the model.
    pub fields: Vec<Field>,
    /// The directives of this model.
    pub directives: Vec<Directive>,
    /// The documentation for this model.
    pub documentation: Option<Comment>,
    /// The location of this model in the text representation.
    pub span: Span,
}

impl WithIdentifier for Model {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithSpan for Model {
    fn span(&self) -> &Span {
        &self.span
    }
}

impl WithDirectives for Model {
    fn directives(&self) -> &Vec<Directive> {
        &self.directives
    }
}

impl WithDocumentation for Model {
    fn documentation(&self) -> &Option<Comment> {
        &self.documentation
    }
}

/// A source block declaration.
#[derive(Debug)]
pub struct SourceConfig {
    /// Name of this source.
    pub name: Identifier,
    /// Top-level configuration properties for this source.
    pub properties: Vec<Argument>,
    /// The comments for this source block.
    pub documentation: Option<Comment>,
    /// The location of this source block in the text representation.
    pub span: Span,
}

impl WithIdentifier for SourceConfig {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithKeyValueConfig for SourceConfig {
    fn properties(&self) -> &Vec<Argument> {
        &self.properties
    }
}

impl WithSpan for SourceConfig {
    fn span(&self) -> &Span {
        &self.span
    }
}

impl WithDocumentation for SourceConfig {
    fn documentation(&self) -> &Option<Comment> {
        &self.documentation
    }
}

/// A Generator block declaration.
#[derive(Debug)]
pub struct GeneratorConfig {
    /// Name of this generator.
    pub name: Identifier,
    /// Top-level configuration properties for this generator.
    pub properties: Vec<Argument>,
    /// The comments for this generator block.
    pub documentation: Option<Comment>,
    /// The location of this generator block in the text representation.
    pub span: Span,
}

impl WithIdentifier for GeneratorConfig {
    fn identifier(&self) -> &Identifier {
        &self.name
    }
}

impl WithKeyValueConfig for GeneratorConfig {
    fn properties(&self) -> &Vec<Argument> {
        &self.properties
    }
}

impl WithSpan for GeneratorConfig {
    fn span(&self) -> &Span {
        &self.span
    }
}

impl WithDocumentation for GeneratorConfig {
    fn documentation(&self) -> &Option<Comment> {
        &self.documentation
    }
}

/// Enum for distinguishing between top-level nodes
/// Enum, Model and SourceConfig.
#[derive(Debug)]
pub enum Top {
    Enum(Enum),
    Model(Model),
    Source(SourceConfig),
    Generator(GeneratorConfig),
    Type(Field),
}

impl WithIdentifier for Top {
    fn identifier(&self) -> &Identifier {
        match self {
            Top::Enum(x) => x.identifier(),
            Top::Model(x) => x.identifier(),
            Top::Source(x) => x.identifier(),
            Top::Generator(x) => x.identifier(),
            Top::Type(x) => x.identifier(),
        }
    }
}

impl WithSpan for Top {
    fn span(&self) -> &Span {
        match self {
            Top::Enum(x) => x.span(),
            Top::Model(x) => x.span(),
            Top::Source(x) => x.span(),
            Top::Generator(x) => x.span(),
            Top::Type(x) => x.span(),
        }
    }
}

impl Top {
    pub fn get_type(&self) -> &str {
        match self {
            Top::Enum(_) => "enum",
            Top::Model(_) => "model",
            Top::Source(_) => "source",
            Top::Generator(_) => "generator",
            Top::Type(_) => "type",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Top::Enum(x) => &x.name.name,
            Top::Model(x) => &x.name.name,
            Top::Source(x) => &x.name.name,
            Top::Generator(x) => &x.name.name,
            Top::Type(x) => &x.name.name,
        }
    }
}

/// A prisma datamodel.
#[derive(Debug)]
pub struct Datamodel {
    /// All models, enums, or source config blocks.
    pub models: Vec<Top>,
}
