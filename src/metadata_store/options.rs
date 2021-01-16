use crate::metadata::{
    ArtifactId, ArtifactState, ContextId, EventStep, EventType, ExecutionId, ExecutionState,
    PropertyType, PropertyValue, TypeId,
};
use crate::query::QueryValue;
use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Debug, Default, Clone)]
pub struct GetTypesOptions {
    pub name: Option<String>,
    pub ids: Vec<TypeId>,
}

impl GetTypesOptions {
    pub fn by_id(id: TypeId) -> Self {
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

#[derive(Debug, Clone, Default)]
pub struct ArtifactOptions {
    pub(crate) name: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) properties: BTreeMap<String, PropertyValue>,
    pub(crate) custom_properties: BTreeMap<String, PropertyValue>,
    pub(crate) state: Option<ArtifactState>,
}

#[derive(Debug, Default, Clone)]
pub struct GetArtifactsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) artifact_name: Option<String>,
    pub(crate) artifact_ids: Vec<ArtifactId>,
    pub(crate) uri: Option<String>,
    pub(crate) context_id: Option<ContextId>,
}

impl GetArtifactsOptions {
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
    pub(crate) execution_ids: Vec<ExecutionId>,
    pub(crate) context_id: Option<ContextId>,
}

impl GetExecutionsOptions {
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

#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    pub(crate) name: Option<String>,
    pub(crate) properties: BTreeMap<String, PropertyValue>,
    pub(crate) custom_properties: BTreeMap<String, PropertyValue>,
    pub(crate) last_known_state: Option<ExecutionState>,
}

#[derive(Debug, Default, Clone)]
pub struct GetContextsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) context_name: Option<String>,
    pub(crate) context_ids: Vec<ContextId>,
    pub(crate) artifact_id: Option<ArtifactId>,
    pub(crate) execution_id: Option<ExecutionId>,
}

impl GetContextsOptions {
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

#[derive(Debug, Clone, Default)]
pub struct ContextOptions {
    pub(crate) name: Option<String>,
    pub(crate) properties: BTreeMap<String, PropertyValue>,
    pub(crate) custom_properties: BTreeMap<String, PropertyValue>,
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

#[derive(Debug, Default, Clone)]
pub struct GetEventsOptions {
    pub(crate) artifact_ids: Vec<ArtifactId>,
    pub(crate) execution_ids: Vec<ExecutionId>,
}
