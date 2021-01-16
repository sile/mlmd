use crate::metadata::{
    ArtifactId, ArtifactState, ContextId, EventStep, EventType, ExecutionId, ExecutionState,
    PropertyTypes, PropertyValues, TypeId,
};
use crate::query::QueryValue;
use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct GetTypesOptions {
    pub name: Option<String>,
    pub ids: BTreeSet<TypeId>,
}

impl GetTypesOptions {
    pub fn by_id(id: TypeId) -> Self {
        Self {
            name: None,
            ids: vec![id].into_iter().collect(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PutTypeOptions {
    pub can_add_fields: bool,
    pub can_omit_fields: bool,
    pub properties: PropertyTypes,
}

#[derive(Debug, Clone, Default)]
pub struct ArtifactOptions {
    pub(crate) name: Option<String>,
    pub(crate) uri: Option<String>,
    pub(crate) properties: PropertyValues,
    pub(crate) custom_properties: PropertyValues,
    pub(crate) state: Option<ArtifactState>,
}

#[derive(Debug, Default, Clone)]
pub struct GetArtifactsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) artifact_name: Option<String>,
    pub(crate) artifact_ids: BTreeSet<ArtifactId>,
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
    pub(crate) execution_ids: BTreeSet<ExecutionId>,
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
    pub(crate) properties: PropertyValues,
    pub(crate) custom_properties: PropertyValues,
    pub(crate) last_known_state: Option<ExecutionState>,
}

#[derive(Debug, Default, Clone)]
pub struct GetContextsOptions {
    pub(crate) type_name: Option<String>,
    pub(crate) context_name: Option<String>,
    pub(crate) context_ids: BTreeSet<ContextId>,
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
    pub(crate) properties: PropertyValues,
    pub(crate) custom_properties: PropertyValues,
}

#[derive(Debug, Clone)]
pub struct PutEventOptions {
    pub(crate) event_type: EventType,
    pub(crate) path: Vec<EventStep>,
}

impl Default for PutEventOptions {
    fn default() -> Self {
        Self {
            event_type: EventType::Unknown,
            path: Vec::new(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct GetEventsOptions {
    pub(crate) artifact_ids: BTreeSet<ArtifactId>,
    pub(crate) execution_ids: BTreeSet<ExecutionId>,
}
