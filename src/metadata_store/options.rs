use crate::metadata::{Id, PropertyType};
use crate::query::QueryValue;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct PutTypeOptions {
    pub(crate) can_add_fields: bool,
    pub(crate) can_omit_fields: bool,
    pub(crate) properties: BTreeMap<String, PropertyType>,
}

impl PutTypeOptions {
    pub fn can_add_fields(mut self) -> Self {
        self.can_add_fields = true;
        self
    }

    pub fn can_omit_fields(mut self) -> Self {
        self.can_omit_fields = true;
        self
    }

    pub fn property(mut self, key: &str, value_type: PropertyType) -> Self {
        self.properties.insert(key.to_owned(), value_type);
        self
    }

    pub fn property_int(self, key: &str) -> Self {
        self.property(key, PropertyType::Int)
    }

    pub fn property_double(self, key: &str) -> Self {
        self.property(key, PropertyType::Double)
    }

    pub fn property_string(self, key: &str) -> Self {
        self.property(key, PropertyType::String)
    }
}

#[derive(Debug, Default, Clone)]
pub struct PostArtifactOptions {}

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
