use sqlx::Row as _;
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

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// TODO: Add `PropertyTypes` and `PropertyValues`

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PropertyType {
    // TODO: Unknown = 0,
    Int = 1,
    Double = 2,
    String = 3,
}

impl PropertyType {
    pub(crate) fn from_i32(value: i32) -> Result<Self, sqlx::Error> {
        match value {
            // 0 => Ok(Self::Unknown),
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

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Int(i32),
    Double(f64),
    String(String),
}

impl PropertyValue {
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

#[derive(Debug, Clone, PartialEq)]
pub struct Artifact {
    pub id: Id,
    pub type_id: Id,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
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
            id: Id::new(row.try_get("id")?),
            type_id: Id::new(row.try_get("type_id")?),
            name: row.try_get("name")?,
            uri: row.try_get("uri")?,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactState {
    Unknown = 0,
    Pending = 1,
    Live = 2,
    MarkedForDeletion = 3,
    Deleted = 4,
}

impl ArtifactState {
    pub fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct Execution {
    pub id: Id,
    pub type_id: Id,
    pub name: Option<String>,
    pub last_known_state: ExecutionState,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
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
            id: Id::new(row.try_get("id")?),
            type_id: Id::new(row.try_get("type_id")?),
            name: row.try_get("name")?,
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
    pub fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct Context {
    pub id: Id,
    pub type_id: Id,
    pub name: String,
    pub properties: BTreeMap<String, PropertyValue>,
    pub custom_properties: BTreeMap<String, PropertyValue>,
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
            id: Id::new(row.try_get("id")?),
            type_id: Id::new(row.try_get("type_id")?),
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

impl EventType {
    pub fn from_i32(v: i32) -> Result<Self, sqlx::Error> {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventStep {
    Index(i32),
    Key(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Event {
    pub artifact_id: Id,
    pub execution_id: Id,
    pub path: Vec<EventStep>,
    pub ty: EventType,
    pub create_time_since_epoch: Duration,
}
