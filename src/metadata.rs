use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(i32);

impl Id {
    pub const fn new(id: i32) -> Self {
        Self(id)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyType {
    Unknown = 0,
    Int = 1,
    Double = 2,
    String = 3,
}

impl PropertyType {
    pub fn from_i32(value: i32) -> Result<Self, ConvertError> {
        match value {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Int),
            2 => Ok(Self::Double),
            3 => Ok(Self::String),
            _ => Err(ConvertError::UndefinedPropertyType { value }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("artifact state {value} is undefined")]
    UndefinedArtifactState { value: i32 },

    #[error("execution state {value} is undefined")]
    UndefinedExecutionState { value: i32 },

    #[error("property type {value} is undefined")]
    UndefinedPropertyType { value: i32 },

    #[error("wrong property value")]
    WrongPropertyValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArtifactType {
    pub id: Id,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionType {
    pub id: Id,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
    // TODO: input_type, output_type
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContextType {
    pub id: Id,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

// TODO: PropertyValue
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i32),
    Double(f64),
    String(String),
}

impl Value {
    pub fn ty(&self) -> PropertyType {
        match self {
            Self::Int(_) => PropertyType::Int,
            Self::Double(_) => PropertyType::Double,
            Self::String(_) => PropertyType::String,
        }
    }

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

#[derive(Debug, Clone, PartialEq)]
pub struct Artifact {
    pub id: Id,
    pub type_id: Id,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, Value>,
    pub custom_properties: BTreeMap<String, Value>,
    pub state: ArtifactState,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
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

#[derive(Debug, Clone, PartialEq)]
pub struct Execution {
    pub id: Id,
    pub type_id: Id,
    pub name: Option<String>,
    pub last_known_state: ExecutionState,
    pub properties: BTreeMap<String, Value>,
    pub custom_properties: BTreeMap<String, Value>,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
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

impl ExecutionState {
    pub fn from_i32(v: i32) -> Result<Self, ConvertError> {
        match v {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::New),
            2 => Ok(Self::Running),
            3 => Ok(Self::Complete),
            4 => Ok(Self::Failed),
            5 => Ok(Self::Cached),
            6 => Ok(Self::Canceled),
            _ => Err(ConvertError::UndefinedExecutionState { value: v }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Context {
    pub id: Id,
    pub type_id: Id,
    pub name: String,
    pub properties: BTreeMap<String, Value>,
    pub custom_properties: BTreeMap<String, Value>,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub enum EventType {
//     Unknown = 0,
//     DeclaredOutput = 1,
//     DeclaredInput = 2,
//     Input = 3,
//     Output = 4,
//     InternalInput = 5,
//     InternalOutput = 6,
// }

// #[derive(Debug, Clone)]
// pub enum EventStep {
//     Index(i64),
//     Key(String),
// }

// #[derive(Debug, Clone)]
// pub struct EventPath {
//     pub steps: Vec<EventStep>,
// }

// #[derive(Debug, Clone)]
// pub struct Event {
//     pub artifact_id: ArtifactId,
//     pub execution_id: ExecutionId,
//     pub path: Option<EventPath>,
//     pub ty: EventType,
//     pub create_time_since_epoch: Duration,
// }

// impl Event {
//     pub fn new(ty: EventType, artifact_id: ArtifactId, execution_id: ExecutionId) -> Self {
//         Self {
//             ty,
//             artifact_id,
//             execution_id,
//             path: None,
//             create_time_since_epoch: UNIX_EPOCH
//                 .elapsed()
//                 .unwrap_or_else(|_| Duration::from_secs(0)),
//         }
//     }
// }

// #[derive(Debug, Clone)]
// pub struct ParentContext {
//     pub child_id: ContextId,
//     pub parent_id: ContextId,
// }

// #[derive(Debug, Clone)]
// pub enum ArtifactStructType {
//     Simple(ArtifactType),
//     Union(Vec<Self>),
//     Intersection(Vec<Self>),
//     List(Box<Self>),
//     None,
//     Any,
//     Tuple(Vec<Self>),
//     Dict(BTreeMap<String, Self>),
// }
