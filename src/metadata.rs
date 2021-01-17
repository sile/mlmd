//! Metadata definitions.
//!
//! Please see [metadata_store.proto] for the detail of each component.
//!
//! [metadata_store.proto]: https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/proto/metadata_store.proto
use sqlx::Row as _;
use std::collections::BTreeMap;
use std::time::Duration;

/// Type kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum TypeKind {
    Execution = 0,
    Artifact = 1,
    Context = 2,
}

impl TypeKind {
    pub(crate) fn item_table_name(&self) -> &'static str {
        match self {
            Self::Execution => "Execution",
            Self::Artifact => "Artifact",
            Self::Context => "Context",
        }
    }
}

impl std::fmt::Display for TypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Execution => write!(f, "execution"),
            Self::Artifact => write!(f, "artifact"),
            Self::Context => write!(f, "context"),
        }
    }
}

/// Type identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeId(i32);

impl TypeId {
    /// Makes a new identifier.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// Gets the value of this identifier.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for TypeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Artifact identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactId(i32);

impl ArtifactId {
    /// Makes a new identifier.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// Gets the value of this identifier.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Execution identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionId(i32);

impl ExecutionId {
    /// Makes a new identifier.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// Gets the value of this identifier.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Context identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextId(i32);

impl ContextId {
    /// Makes a new identifier.
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    /// Gets the value of this identifier.
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifier of artifact, execution or context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(missing_docs)]
pub enum Id {
    Artifact(ArtifactId),
    Execution(ExecutionId),
    Context(ContextId),
}

impl Id {
    /// Gets the value of this identifier.
    pub fn get(self) -> i32 {
        match self {
            Self::Artifact(x) => x.get(),
            Self::Execution(x) => x.get(),
            Self::Context(x) => x.get(),
        }
    }

    /// Gets the type kind of this identifier.
    pub fn kind(self) -> TypeKind {
        match self {
            Self::Artifact(_) => TypeKind::Artifact,
            Self::Execution(_) => TypeKind::Execution,
            Self::Context(_) => TypeKind::Context,
        }
    }

    pub(crate) fn from_kind(id: i32, kind: TypeKind) -> Self {
        match kind {
            TypeKind::Artifact => Self::Artifact(ArtifactId::new(id)),
            TypeKind::Execution => Self::Execution(ExecutionId::new(id)),
            TypeKind::Context => Self::Context(ContextId::new(id)),
        }
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {}", self.kind(), self.get())
    }
}

/// Property types.
pub type PropertyTypes = BTreeMap<String, PropertyType>;

/// Property values.
pub type PropertyValues = BTreeMap<String, PropertyValue>;

/// Property type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum PropertyType {
    Int = 1,
    Double = 2,
    String = 3,
}

impl PropertyType {
    pub(crate) fn from_i32(value: i32) -> Result<Self, sqlx::Error> {
        match value {
            1 => Ok(Self::Int),
            2 => Ok(Self::Double),
            3 => Ok(Self::String),
            _ => Err(sqlx::Error::Decode(
                anyhow::anyhow!("property type {} is undefined", value).into(),
            )),
        }
    }
}

impl std::fmt::Display for PropertyType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Int => write!(f, "int"),
            Self::Double => write!(f, "double"),
            Self::String => write!(f, "string"),
        }
    }
}

/// Artifact type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub struct ArtifactType {
    pub id: TypeId,
    pub name: String,
    pub properties: PropertyTypes,
}

/// Execution type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub struct ExecutionType {
    pub id: TypeId,
    pub name: String,
    pub properties: PropertyTypes,
}

/// Context type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub struct ContextType {
    pub id: TypeId,
    pub name: String,
    pub properties: PropertyTypes,
}

/// Property value.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum PropertyValue {
    Int(i32),
    Double(f64),
    String(String),
}

impl PropertyValue {
    /// Gets the type of this property.
    pub fn ty(&self) -> PropertyType {
        match self {
            Self::Int(_) => PropertyType::Int,
            Self::Double(_) => PropertyType::Double,
            Self::String(_) => PropertyType::String,
        }
    }

    /// Gets the value of this property as [`i32`].
    ///
    /// If this is not a [`PropertyValue::Int`], [`None`] is returned .
    pub fn as_int(&self) -> Option<i32> {
        if let Self::Int(v) = &self {
            Some(*v)
        } else {
            None
        }
    }

    /// Gets the value of this property as [`f64`].
    ///
    /// If this is not a [`PropertyValue::Double`], [`None`] is returned .
    pub fn as_double(&self) -> Option<f64> {
        if let Self::Double(v) = &self {
            Some(*v)
        } else {
            None
        }
    }

    /// Gets the value of this property as [`String`].
    ///
    /// If this is not a [`PropertyValue::String`], [`None`] is returned .
    pub fn as_string(&self) -> Option<&String> {
        if let Self::String(v) = &self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i32> for PropertyValue {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl<'a> From<&'a str> for PropertyValue {
    fn from(v: &'a str) -> Self {
        Self::String(v.to_owned())
    }
}

/// Artifact.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub struct Artifact {
    pub id: ArtifactId,
    pub type_id: TypeId,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub properties: PropertyValues,
    pub custom_properties: PropertyValues,
    pub state: ArtifactState,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

impl crate::query::InsertProperty for Artifact {
    fn insert_property(&mut self, is_custom: bool, name: String, value: PropertyValue) {
        if is_custom {
            self.custom_properties.insert(name, value);
        } else {
            self.properties.insert(name, value);
        }
    }
}

impl<'a> sqlx::FromRow<'a, sqlx::any::AnyRow> for Artifact {
    fn from_row(row: &'a sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: ArtifactId::new(row.try_get("id")?),
            type_id: TypeId::new(row.try_get("type_id")?),
            name: none_if_empty(row.try_get("name")?),
            uri: none_if_empty(row.try_get("uri")?),
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            state: ArtifactState::from_i32(row.try_get("state")?)?,
            create_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("create_time_since_epoch")? as u64,
            ),
            last_update_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("last_update_time_since_epoch")? as u64,
            ),
        })
    }
}

/// Artifact state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactState {
    /// Unknown state (default).
    Unknown = 0,

    /// A state indicating that the artifact may exist.
    Pending = 1,

    /// A state indicating that the artifact should exist, unless something external to the system deletes it.
    Live = 2,

    /// A state indicating that the artifact should be deleted.
    MarkedForDeletion = 3,

    /// A state indicating that the artifact has been deleted.
    Deleted = 4,
}

impl ArtifactState {
    pub(crate) fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Pending),
            2 => Ok(Self::Live),
            3 => Ok(Self::MarkedForDeletion),
            4 => Ok(Self::Deleted),
            _ => Err(sqlx::Error::Decode(
                anyhow::anyhow!("artifact state {} is undefined", v).into(),
            )),
        }
    }
}

impl Default for ArtifactState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub struct Execution {
    pub id: ExecutionId,
    pub type_id: TypeId,
    pub name: Option<String>,
    pub last_known_state: ExecutionState,
    pub properties: PropertyValues,
    pub custom_properties: PropertyValues,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

impl crate::query::InsertProperty for Execution {
    fn insert_property(&mut self, is_custom: bool, name: String, value: PropertyValue) {
        if is_custom {
            self.custom_properties.insert(name, value);
        } else {
            self.properties.insert(name, value);
        }
    }
}

impl<'a> sqlx::FromRow<'a, sqlx::any::AnyRow> for Execution {
    fn from_row(row: &'a sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: ExecutionId::new(row.try_get("id")?),
            type_id: TypeId::new(row.try_get("type_id")?),
            name: none_if_empty(row.try_get("name")?),
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            last_known_state: ExecutionState::from_i32(row.try_get("last_known_state")?)?,
            create_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("create_time_since_epoch")? as u64,
            ),
            last_update_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("last_update_time_since_epoch")? as u64,
            ),
        })
    }
}

/// Execution state.
///
/// The state transitions are `New -> Running -> Complete | Cached | Failed | Canceled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionState {
    /// Unknown state (default).
    Unknown = 0,

    /// The execution is created but not started yet.
    New = 1,

    /// The execution is running.
    Running = 2,

    /// The execution complete.
    Complete = 3,

    /// The execution failed.
    Failed = 4,

    /// The execution is skipped due to cached results.
    Cached = 5,

    /// The execution is skipped due to precondition not met.
    ///
    /// It is different from [`Cached`](Self::Cached) in that a [`Canceled`](Self::Canceled)
    /// execution will not have any event associated with it.
    /// It is different from [`Failed`](Self::Failed) in that there is no
    /// unexpected error happened and it is regarded as a normal state.
    Canceled = 6,
}

impl ExecutionState {
    pub(crate) fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::New),
            2 => Ok(Self::Running),
            3 => Ok(Self::Complete),
            4 => Ok(Self::Failed),
            5 => Ok(Self::Cached),
            6 => Ok(Self::Canceled),
            _ => Err(sqlx::Error::Decode(
                anyhow::anyhow!("execution state {} is undefined", v).into(),
            )),
        }
    }
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Context.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub struct Context {
    pub id: ContextId,
    pub type_id: TypeId,
    pub name: String,
    pub properties: PropertyValues,
    pub custom_properties: PropertyValues,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

impl crate::query::InsertProperty for Context {
    fn insert_property(&mut self, is_custom: bool, name: String, value: PropertyValue) {
        if is_custom {
            self.custom_properties.insert(name, value);
        } else {
            self.properties.insert(name, value);
        }
    }
}

impl<'a> sqlx::FromRow<'a, sqlx::any::AnyRow> for Context {
    fn from_row(row: &'a sqlx::any::AnyRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: ContextId::new(row.try_get("id")?),
            type_id: TypeId::new(row.try_get("type_id")?),
            name: row.try_get("name")?,
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            create_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("create_time_since_epoch")? as u64,
            ),
            last_update_time_since_epoch: Duration::from_millis(
                row.try_get::<i64, _>("last_update_time_since_epoch")? as u64,
            ),
        })
    }
}

/// Event type.
///
/// Events distinguish between an artifact that is written by the execution
/// (possibly as a cache), versus artifacts that are part of the declared
/// output of the execution.
///
/// For more information on what `Declared` and `Iternal` mean, see [the comment on the original repo][comment].
///
/// [comment]: https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/proto/metadata_store.proto#L94-L161
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum EventType {
    Unknown = 0,
    DeclaredOutput = 1,
    DeclaredInput = 2,
    Input = 3,
    Output = 4,
    InternalInput = 5,
    InternalOutput = 6,
}

impl EventType {
    pub(crate) fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::DeclaredOutput),
            2 => Ok(Self::DeclaredInput),
            3 => Ok(Self::Input),
            4 => Ok(Self::Output),
            5 => Ok(Self::InternalInput),
            6 => Ok(Self::InternalOutput),
            _ => Err(sqlx::Error::Decode(
                anyhow::anyhow!("event type {} is undefined", v).into(),
            )),
        }
    }
}

impl Default for EventType {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Event step.
///
/// A path (i.e., a vector of event steps) can name an artifact in the context of an execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum EventStep {
    Index(i32),
    Key(String),
}

/// Event.
///
/// An event represents a relationship between an artifact and an execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub struct Event {
    pub artifact_id: ArtifactId,
    pub execution_id: ExecutionId,
    pub path: Vec<EventStep>,
    pub ty: EventType,
    pub create_time_since_epoch: Duration,
}

fn none_if_empty(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
