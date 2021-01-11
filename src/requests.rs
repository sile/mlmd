use crate::metadata::{ArtifactType, ContextType, ExecutionType, Id, PropertyType};
use crate::metadata_store::{errors, options, MetadataStore};
use crate::query;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct PutArtifactTypeRequest<'a> {
    store: &'a mut MetadataStore,
    type_name: String,
    options: options::PutTypeOptions,
}

impl<'a> PutArtifactTypeRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_name: &str) -> Self {
        Self {
            store,
            type_name: type_name.to_owned(),
            options: options::PutTypeOptions::default(),
        }
    }

    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    pub fn properties(mut self, properties: BTreeMap<String, PropertyType>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PutTypeError> {
        self.store
            .put_type(query::TypeKind::Artifact, &self.type_name, self.options)
            .await
    }
}

#[derive(Debug)]
pub struct GetArtifactTypesRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetTypesOptions,
}

impl<'a> GetArtifactTypesRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: options::GetTypesOptions::default(),
        }
    }

    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    pub fn id(mut self, type_id: Id) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[Id]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ArtifactType>, errors::GetError> {
        self.store
            .get_types(
                query::TypeKind::Artifact,
                |id, name, properties| ArtifactType {
                    id,
                    name,
                    properties,
                },
                self.options,
            )
            .await
    }
}

#[derive(Debug)]
pub struct PutExecutionTypeRequest<'a> {
    store: &'a mut MetadataStore,
    type_name: String,
    options: options::PutTypeOptions,
}

impl<'a> PutExecutionTypeRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_name: &str) -> Self {
        Self {
            store,
            type_name: type_name.to_owned(),
            options: options::PutTypeOptions::default(),
        }
    }

    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    pub fn properties(mut self, properties: BTreeMap<String, PropertyType>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PutTypeError> {
        self.store
            .put_type(query::TypeKind::Execution, &self.type_name, self.options)
            .await
    }
}

#[derive(Debug)]
pub struct GetExecutionTypesRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetTypesOptions,
}

impl<'a> GetExecutionTypesRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: options::GetTypesOptions::default(),
        }
    }

    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    pub fn id(mut self, type_id: Id) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[Id]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ExecutionType>, errors::GetError> {
        self.store
            .get_types(
                query::TypeKind::Execution,
                |id, name, properties| ExecutionType {
                    id,
                    name,
                    properties,
                },
                self.options,
            )
            .await
    }
}

#[derive(Debug)]
pub struct PutContextTypeRequest<'a> {
    store: &'a mut MetadataStore,
    type_name: String,
    options: options::PutTypeOptions,
}

impl<'a> PutContextTypeRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_name: &str) -> Self {
        Self {
            store,
            type_name: type_name.to_owned(),
            options: options::PutTypeOptions::default(),
        }
    }

    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    pub fn properties(mut self, properties: BTreeMap<String, PropertyType>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PutTypeError> {
        self.store
            .put_type(query::TypeKind::Context, &self.type_name, self.options)
            .await
    }
}

#[derive(Debug)]
pub struct GetContextTypesRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetTypesOptions,
}

impl<'a> GetContextTypesRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: options::GetTypesOptions::default(),
        }
    }

    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    pub fn id(mut self, type_id: Id) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[Id]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ContextType>, errors::GetError> {
        self.store
            .get_types(
                query::TypeKind::Context,
                |id, name, properties| ContextType {
                    id,
                    name,
                    properties,
                },
                self.options,
            )
            .await
    }
}
