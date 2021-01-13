use crate::metadata::{
    ArtifactState, EventStep, EventType, ExecutionState, Id, PropertyType, Value,
};
use crate::query::QueryValue;
use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Debug, Default, Clone)]
pub struct GetTypesOptions {
    pub name: Option<String>,
    pub ids: Vec<Id>,
}

impl GetTypesOptions {
    pub fn by_id(id: Id) -> Self {
        Self {
            name: None,
            ids: vec![id],
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PutTypeOptions {
    pub can_add_fields: bool,
    pub can_omit_fields: bool,
    pub properties: BTreeMap<String, PropertyType>,
}

#[derive(Debug, Clone)]
pub struct PostArtifactOptions {
    pub(crate) name: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) custom_properties: BTreeMap<String, Value>,
    pub(crate) state: ArtifactState,
    pub(crate) create_time_since_epoch: Duration,
    pub(crate) last_update_time_since_epoch: Duration,
}

impl Default for PostArtifactOptions {
    fn default() -> Self {
        Self {
            name: None,
            uri: None,
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            state: ArtifactState::Unknown,
            create_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
            last_update_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct GetArtifactsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) artifact_name: Option<String>,
    pub(crate) artifact_ids: Vec<Id>,
    pub(crate) uri: Option<String>,
    pub(crate) context_id: Option<Id>,
}

impl GetArtifactsOptions {
    pub fn ty(mut self, type_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, artifact_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self.artifact_name = Some(artifact_name.to_owned());
        self
    }

    pub fn ids(mut self, artifact_ids: &[Id]) -> Self {
        self.artifact_ids = Vec::from(artifact_ids);
        self
    }

    pub fn uri(mut self, uri: &str) -> Self {
        self.uri = Some(uri.to_owned());
        self
    }

    pub fn context(mut self, context_id: Id) -> Self {
        self.context_id = Some(context_id);
        self
    }

    pub(crate) fn values(&self) -> Vec<QueryValue> {
        let mut values = Vec::new();
        if let Some(v) = &self.type_name {
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = &self.artifact_name {
            values.push(QueryValue::Str(v));
        }
        for v in &self.artifact_ids {
            values.push(QueryValue::Int(v.get()));
        }
        if let Some(v) = &self.uri {
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = self.context_id {
            values.push(QueryValue::Int(v.get()));
        }
        values
    }
}

#[derive(Debug, Default, Clone)]
pub struct GetExecutionsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) execution_name: Option<String>,
    pub(crate) execution_ids: Vec<Id>,
    pub(crate) context_id: Option<Id>,
}

impl GetExecutionsOptions {
    pub fn ty(mut self, type_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, execution_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self.execution_name = Some(execution_name.to_owned());
        self
    }

    pub fn ids(mut self, execution_ids: &[Id]) -> Self {
        self.execution_ids = Vec::from(execution_ids);
        self
    }

    pub fn context(mut self, context_id: Id) -> Self {
        self.context_id = Some(context_id);
        self
    }

    pub(crate) fn values(&self) -> Vec<QueryValue> {
        let mut values = Vec::new();
        if let Some(v) = &self.type_name {
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = &self.execution_name {
            values.push(QueryValue::Str(v));
        }
        for v in &self.execution_ids {
            values.push(QueryValue::Int(v.get()));
        }
        if let Some(v) = self.context_id {
            values.push(QueryValue::Int(v.get()));
        }
        values
    }
}

#[derive(Debug, Clone)]
pub struct PostExecutionOptions {
    pub(crate) name: Option<String>,
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) custom_properties: BTreeMap<String, Value>,
    pub(crate) last_known_state: ExecutionState,
    pub(crate) create_time_since_epoch: Duration,
    pub(crate) last_update_time_since_epoch: Duration,
}

impl Default for PostExecutionOptions {
    fn default() -> Self {
        Self {
            name: None,
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            last_known_state: ExecutionState::Unknown,
            create_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
            last_update_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
        }
    }
}

impl PostExecutionOptions {
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.custom_properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn last_known_state(mut self, state: ExecutionState) -> Self {
        self.last_known_state = state;
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.create_time_since_epoch = time;
        self
    }

    pub fn last_update_time_since_epoch(mut self, time: Duration) -> Self {
        self.last_update_time_since_epoch = time;
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct GetContextsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) context_name: Option<String>,
    pub(crate) context_ids: Vec<Id>,
    pub(crate) artifact_id: Option<Id>,
    pub(crate) execution_id: Option<Id>,
}

impl GetContextsOptions {
    pub fn ty(mut self, type_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, context_name: &str) -> Self {
        self.type_name = Some(type_name.to_owned());
        self.context_name = Some(context_name.to_owned());
        self
    }

    pub fn ids(mut self, context_ids: &[Id]) -> Self {
        self.context_ids = Vec::from(context_ids);
        self
    }

    pub fn artifact(mut self, artifact_id: Id) -> Self {
        self.artifact_id = Some(artifact_id);
        self
    }

    pub fn execution(mut self, execution_id: Id) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    pub(crate) fn values(&self) -> Vec<QueryValue> {
        let mut values = Vec::new();
        if let Some(v) = &self.type_name {
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = &self.context_name {
            values.push(QueryValue::Str(v));
        }
        for v in &self.context_ids {
            values.push(QueryValue::Int(v.get()));
        }
        if let Some(v) = self.artifact_id {
            values.push(QueryValue::Int(v.get()));
        }
        if let Some(v) = self.execution_id {
            values.push(QueryValue::Int(v.get()));
        }
        values
    }
}

#[derive(Debug, Clone)]
pub struct PostContextOptions {
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) custom_properties: BTreeMap<String, Value>,
    pub(crate) create_time_since_epoch: Duration,
    pub(crate) last_update_time_since_epoch: Duration,
}

impl Default for PostContextOptions {
    fn default() -> Self {
        Self {
            properties: BTreeMap::new(),
            custom_properties: BTreeMap::new(),
            create_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
            last_update_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
        }
    }
}

impl PostContextOptions {
    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.custom_properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.create_time_since_epoch = time;
        self
    }

    pub fn last_update_time_since_epoch(mut self, time: Duration) -> Self {
        self.last_update_time_since_epoch = time;
        self
    }
}

#[derive(Debug, Clone)]
pub struct PutEventOptions {
    pub(crate) event_type: EventType,
    pub(crate) path: Vec<EventStep>,
    pub(crate) create_time_since_epoch: Duration,
}

impl Default for PutEventOptions {
    fn default() -> Self {
        Self {
            event_type: EventType::Unknown,
            path: Vec::new(),
            create_time_since_epoch: UNIX_EPOCH.elapsed().unwrap_or_default(),
        }
    }
}

impl PutEventOptions {
    pub fn path(mut self, path: Vec<EventStep>) -> Self {
        self.path = path;
        self
    }

    pub fn step(mut self, step: EventStep) -> Self {
        self.path.push(step);
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.create_time_since_epoch = time;
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct GetEventsOptions {
    pub(crate) artifact_ids: Vec<Id>,
    pub(crate) execution_ids: Vec<Id>,
}

impl GetEventsOptions {
    pub fn artifact_ids(mut self, ids: &[Id]) -> Self {
        self.artifact_ids = Vec::from(ids);
        self
    }

    pub fn execution_ids(mut self, ids: &[Id]) -> Self {
        self.execution_ids = Vec::from(ids);
        self
    }
}
