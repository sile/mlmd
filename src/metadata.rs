use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Double(f64),
    String(String),
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PropertyType {
    Unknown = 0,
    Int = 1,
    Double = 2,
    String = 3,
}

#[derive(Debug, Clone)]
pub struct ArtifactType {
    pub id: i32,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl ArtifactType {
    // pub fn instantiate(&self, id: i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ArtifactState {
    Unknown = 0,
    Pending = 1,
    Live = 2,
    MarkedForDeletion = 3,
    Deleted = 4,
}

#[derive(Debug, Clone)]
pub struct Artifact {
    pub id: i32,
    pub name: Option<String>,
    pub ty: Arc<ArtifactType>,
    pub uri: Option<String>,
    pub properties: BTreeMap<String, Value>,
    pub state: ArtifactState,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

// impl Artifact {
//     pub fn builder(id: u32, type_id: u32) -> ArtifactBuilder {
//         let elapsed = UNIX_EPOCH
//             .elapsed()
//             .unwrap_or_else(|_| Duration::from_secs(0));
//         ArtifactBuilder(Artifact {
//             id,
//             type_id,
//             name: None,
//             uri: None,
//             properties: BTreeMap::new(),
//             state: ArtifactState::Unknown,
//             create_time_since_epoch: elapsed,
//             last_update_time_since_epoch: elapsed,
//         })
//     }
// }

// #[derive(Debug, Clone)]
// pub struct ArtifactBuilder(Artifact);

// impl ArtifactBuilder {
//     pub fn name(mut self, name: &str) -> Self {
//         self.0.name = Some(name.to_owned());
//         self
//     }

//     pub fn uri(mut self, uri: &str) -> Self {
//         self.0.uri = Some(uri.to_owned());
//         self
//     }

//     pub fn property<T>(mut self, key: &str, value: T) -> Self
//     where
//         T: Into<Value>,
//     {
//         self.0.properties.insert(key.to_owned(), value.into());
//         self
//     }

//     pub fn state(mut self, state: ArtifactState) -> Self {
//         self.0.state = state;
//         self
//     }

//     pub fn build(self) -> Artifact {
//         self.0
//     }
// }
