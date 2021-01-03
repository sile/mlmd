use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i32),
    Double(f64),
    String(String),
}

impl Value {
    pub fn as_int(&self) -> Option<i32> {
        if let Self::Int(v) = &self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        if let Self::Double(v) = &self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        if let Self::String(v) = &self {
            Some(v)
        } else {
            None
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(v: &'a str) -> Self {
        Self::String(v.to_owned())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyType {
    Unknown = 0,
    Int = 1,
    Double = 2,
    String = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactTypeId(i32);

impl ArtifactTypeId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct ArtifactType {
    pub id: ArtifactTypeId,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

#[derive(Debug, Clone)]
pub struct NewArtifactType {
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl NewArtifactType {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("artifact state {value} is undefined")]
    UndefinedArtifactState { value: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactState {
    Unknown = 0,
    Pending = 1,
    Live = 2,
    MarkedForDeletion = 3,
    Deleted = 4,
}

impl ArtifactState {
    pub fn from_i32(v: i32) -> Result<Self, ConvertError> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Pending),
            2 => Ok(Self::Live),
            3 => Ok(Self::MarkedForDeletion),
            4 => Ok(Self::Deleted),
            _ => Err(ConvertError::UndefinedArtifactState { value: v }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArtifactId(i32);

impl ArtifactId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Artifact {
    pub ty: ArtifactType,
    pub id: ArtifactId,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, Value>,
    pub state: ArtifactState,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

#[derive(Debug, Clone)]
pub struct NewArtifact {
    pub ty: ArtifactType,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, Value>,
    pub state: ArtifactState,
    // TODO: create_time_since_epoch, last_update_time_since_epoch
}

impl NewArtifact {
    pub fn new(ty: ArtifactType) -> Self {
        Self {
            ty,
            name: None,
            uri: None,
            properties: BTreeMap::new(),
            state: ArtifactState::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionTypeId(i32);

impl ExecutionTypeId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionType {
    pub id: ExecutionTypeId,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
    pub input_type: Option<ArtifactStructType>,
    pub output_type: Option<ArtifactStructType>,
}

#[derive(Debug, Clone)]
pub struct NewExecutionType {
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
    pub input_type: Option<ArtifactStructType>,
    pub output_type: Option<ArtifactStructType>,
}

impl NewExecutionType {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            properties: BTreeMap::new(),
            input_type: None,
            output_type: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExecutionId(i32);

impl ExecutionId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Execution {
    pub id: ExecutionId,
    pub name: Option<String>,
    pub ty: ExecutionType,
    pub last_known_state: ExecutionState,
    pub properties: BTreeMap<String, Value>,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

#[derive(Debug, Clone)]
pub struct NewExecution {
    pub name: Option<String>,
    pub ty: ExecutionType,
    pub last_known_state: ExecutionState,
    pub properties: BTreeMap<String, Value>,
}

impl NewExecution {
    pub fn new(ty: ExecutionType) -> Self {
        Self {
            ty,
            name: None,
            last_known_state: ExecutionState::Unknown,
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionState {
    Unknown = 0,
    New = 1,
    Running = 2,
    Complete = 3,
    Failed = 4,
    Cached = 5,
    Canceled = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Unknown = 0,
    DeclaredOutput = 1,
    DeclaredInput = 2,
    Input = 3,
    Output = 4,
    InternalInput = 5,
    InternalOutput = 6,
}

#[derive(Debug, Clone)]
pub enum EventStep {
    Index(i64),
    Key(String),
}

#[derive(Debug, Clone)]
pub struct EventPath {
    pub steps: Vec<EventStep>,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub artifact_id: ArtifactId,
    pub execution_id: ExecutionId,
    pub path: Option<EventPath>,
    pub ty: EventType,
    pub create_time_since_epoch: Duration,
}

impl Event {
    pub fn new(ty: EventType, artifact_id: ArtifactId, execution_id: ExecutionId) -> Self {
        Self {
            ty,
            artifact_id,
            execution_id,
            path: None,
            create_time_since_epoch: UNIX_EPOCH
                .elapsed()
                .unwrap_or_else(|_| Duration::from_secs(0)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextTypeId(i32);

impl ContextTypeId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct ContextType {
    pub id: ContextTypeId,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

#[derive(Debug, Clone)]
pub struct NewContextType {
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl NewContextType {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            properties: BTreeMap::new(),
        }
    }

    // TODO: fn instantiate(&self) -> NewContext
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextId(i32);

impl ContextId {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Context {
    pub ty: ContextType,
    pub id: ContextId,
    pub name: String,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, Value>,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

#[derive(Debug, Clone)]
pub struct NewContext {
    pub ty: ContextType,
    pub name: String,
    pub properties: BTreeMap<String, Value>,
}

impl NewContext {
    pub fn new(ty: ContextType, name: &str) -> Self {
        Self {
            ty,
            name: name.to_owned(),
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Attribution {
    pub artifact_id: ArtifactId,
    pub context_id: ContextId,
}

#[derive(Debug, Clone)]
pub struct Association {
    pub execution_id: ExecutionId,
    pub context_id: ContextId,
}

#[derive(Debug, Clone)]
pub struct ParentContext {
    pub child_id: ContextId,
    pub parent_id: ContextId,
}

#[derive(Debug, Clone)]
pub enum ArtifactStructType {
    Simple(ArtifactType),
    Union(Vec<Self>),
    Intersection(Vec<Self>),
    List(Box<Self>),
    None,
    Any,
    Tuple(Vec<Self>),
    Dict(BTreeMap<String, Self>),
}
