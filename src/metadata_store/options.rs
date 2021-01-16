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

#[derive(Debug, Clone)]
pub enum ItemOptions {
    Artifact(ArtifactOptions),
    Execution(ExecutionOptions),
    Context(ContextOptions),
}

impl ItemOptions {
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Artifact(x) => x.name.as_ref().map(|n| n.as_str()),
            Self::Execution(x) => x.name.as_ref().map(|n| n.as_str()),
            Self::Context(x) => x.name.as_ref().map(|n| n.as_str()),
        }
    }

    pub fn properties(&self) -> &PropertyValues {
        match self {
            Self::Artifact(x) => &x.properties,
            Self::Execution(x) => &x.properties,
            Self::Context(x) => &x.properties,
        }
    }

    pub fn custom_properties(&self) -> &PropertyValues {
        match self {
            Self::Artifact(x) => &x.custom_properties,
            Self::Execution(x) => &x.custom_properties,
            Self::Context(x) => &x.custom_properties,
        }
    }

    pub fn extra_fields(&self) -> Vec<(&'static str, QueryValue)> {
        let mut fields = Vec::new();
        match self {
            Self::Artifact(x) => {
                if let Some(uri) = &x.uri {
                    fields.push(("uri", QueryValue::Str(uri)));
                }
                if let Some(state) = x.state {
                    fields.push(("state", QueryValue::Int(state as i32)));
                }
            }
            Self::Execution(x) => {
                if let Some(state) = x.last_known_state {
                    fields.push(("last_known_state", QueryValue::Int(state as i32)));
                }
            }
            Self::Context(_) => {}
        }
        fields
    }
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
