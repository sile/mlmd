use crate::errors;
use crate::metadata::{
    Artifact, ArtifactId, ArtifactState, ArtifactType, Context, ContextId, ContextType, Event,
    EventStep, EventType, Execution, ExecutionId, ExecutionState, ExecutionType, Id, PropertyType,
    PropertyTypes, PropertyValue, PropertyValues, TypeId, TypeKind,
};
use crate::metadata_store::{options, MetadataStore};
use crate::query;
use std::time::Duration;

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

    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<TypeId, errors::PutError> {
        self.store
            .put_type(TypeKind::Artifact, &self.type_name, self.options)
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

    pub fn id(mut self, type_id: TypeId) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[TypeId]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ArtifactType>, errors::GetError> {
        self.store
            .get_types(
                TypeKind::Artifact,
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

    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<TypeId, errors::PutError> {
        self.store
            .put_type(TypeKind::Execution, &self.type_name, self.options)
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

    pub fn id(mut self, type_id: TypeId) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[TypeId]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ExecutionType>, errors::GetError> {
        self.store
            .get_types(
                TypeKind::Execution,
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

    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<TypeId, errors::PutError> {
        self.store
            .put_type(TypeKind::Context, &self.type_name, self.options)
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

    pub fn id(mut self, type_id: TypeId) -> Self {
        self.options.ids.push(type_id);
        self
    }

    pub fn ids(mut self, type_ids: &[TypeId]) -> Self {
        self.options.ids.extend(type_ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<ContextType>, errors::GetError> {
        self.store
            .get_types(
                TypeKind::Context,
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

#[derive(Debug)]
pub struct GetArtifactsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetArtifactsOptions,
}

impl<'a> GetArtifactsRequest<'a> {
    pub fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, artifact_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.artifact_name = Some(artifact_name.to_owned());
        self
    }

    pub fn id(mut self, artifact_id: ArtifactId) -> Self {
        self.options.artifact_ids.push(artifact_id);
        self
    }

    pub fn ids(mut self, artifact_ids: &[ArtifactId]) -> Self {
        self.options.artifact_ids.extend(artifact_ids);
        self
    }

    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    pub fn context(mut self, context_id: ContextId) -> Self {
        self.options.context_id = Some(context_id);
        self
    }

    pub async fn execute(self) -> Result<Vec<Artifact>, errors::GetError> {
        let generator = query::GetArtifactsQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.get_items(generator).await
    }
}

#[derive(Debug)]
pub struct GetExecutionsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetExecutionsOptions,
}

impl<'a> GetExecutionsRequest<'a> {
    pub fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, execution_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.execution_name = Some(execution_name.to_owned());
        self
    }

    pub fn id(mut self, execution_id: ExecutionId) -> Self {
        self.options.execution_ids.push(execution_id);
        self
    }

    pub fn ids(mut self, execution_ids: &[ExecutionId]) -> Self {
        self.options.execution_ids.extend(execution_ids);
        self
    }

    pub fn context(mut self, context_id: ContextId) -> Self {
        self.options.context_id = Some(context_id);
        self
    }

    pub async fn execute(self) -> Result<Vec<Execution>, errors::GetError> {
        let generator = query::GetExecutionsQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.get_items(generator).await
    }
}

#[derive(Debug)]
pub struct GetContextsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetContextsOptions,
}

impl<'a> GetContextsRequest<'a> {
    pub fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    pub fn type_and_name(mut self, type_name: &str, context_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.context_name = Some(context_name.to_owned());
        self
    }

    pub fn id(mut self, context_id: ContextId) -> Self {
        self.options.context_ids.push(context_id);
        self
    }

    pub fn ids(mut self, context_ids: &[ContextId]) -> Self {
        self.options.context_ids.extend(context_ids);
        self
    }

    pub fn artifact(mut self, artifact_id: ArtifactId) -> Self {
        self.options.artifact_id = Some(artifact_id);
        self
    }

    pub fn execution(mut self, execution_id: ExecutionId) -> Self {
        self.options.execution_id = Some(execution_id);
        self
    }

    pub async fn execute(self) -> Result<Vec<Context>, errors::GetError> {
        let generator = query::GetContextsQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.get_items(generator).await
    }
}

#[derive(Debug)]
pub struct PostArtifactRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: TypeId,
    options: options::ArtifactOptions,
}

impl<'a> PostArtifactRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: TypeId) -> Self {
        Self {
            store,
            type_id,
            options: Default::default(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn state(mut self, state: ArtifactState) -> Self {
        self.options.state = Some(state);
        self
    }

    pub async fn execute(self) -> Result<ArtifactId, errors::PostError> {
        let generator = query::PostArtifactQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            options: self.options,
        };
        self.store
            .post_item(self.type_id, generator)
            .await
            .map(ArtifactId::new)
    }
}

#[derive(Debug)]
pub struct PostExecutionRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: TypeId,
    options: options::ExecutionOptions,
}

impl<'a> PostExecutionRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: TypeId) -> Self {
        Self {
            store,
            type_id,
            options: Default::default(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn last_known_state(mut self, state: ExecutionState) -> Self {
        self.options.last_known_state = Some(state);
        self
    }

    pub async fn execute(self) -> Result<ExecutionId, errors::PostError> {
        let generator = query::PostExecutionQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            options: self.options,
        };
        self.store
            .post_item(self.type_id, generator)
            .await
            .map(ExecutionId::new)
    }
}

#[derive(Debug)]
pub struct PostContextRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: TypeId,
    name: String,
    options: options::ContextOptions,
}

impl<'a> PostContextRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: TypeId, context_name: &str) -> Self {
        Self {
            store,
            type_id,
            name: context_name.to_owned(),
            options: Default::default(),
        }
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub async fn execute(self) -> Result<ContextId, errors::PostError> {
        let generator = query::PostContextQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            name: self.name,
            options: self.options,
        };
        self.store
            .post_item(self.type_id, generator)
            .await
            .map(ContextId::new)
    }
}

#[derive(Debug)]
pub struct PutArtifactRequest<'a> {
    store: &'a mut MetadataStore,
    id: ArtifactId,
    options: options::ArtifactOptions,
}

impl<'a> PutArtifactRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, id: ArtifactId) -> Self {
        Self {
            store,
            id,
            options: Default::default(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn state(mut self, state: ArtifactState) -> Self {
        self.options.state = Some(state);
        self
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutArtifactQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.put_item(Id::Artifact(self.id), generator).await
    }
}

#[derive(Debug)]
pub struct PutExecutionRequest<'a> {
    store: &'a mut MetadataStore,
    id: ExecutionId,
    options: options::ExecutionOptions,
}

impl<'a> PutExecutionRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, id: ExecutionId) -> Self {
        Self {
            store,
            id,
            options: Default::default(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn last_known_state(mut self, state: ExecutionState) -> Self {
        self.options.last_known_state = Some(state);
        self
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutExecutionQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.put_item(Id::Execution(self.id), generator).await
    }
}

#[derive(Debug)]
pub struct PutContextRequest<'a> {
    store: &'a mut MetadataStore,
    id: ContextId,
    options: options::ContextOptions,
}

impl<'a> PutContextRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, id: ContextId) -> Self {
        Self {
            store,
            id,
            options: Default::default(),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutContextQueryGenerator {
            query: self.store.query.clone(),
            options: self.options,
        };
        self.store.put_item(Id::Context(self.id), generator).await
    }
}

#[derive(Debug)]
pub struct PutAttributionRequest<'a> {
    store: &'a mut MetadataStore,
    context_id: ContextId,
    artifact_id: ArtifactId,
}

impl<'a> PutAttributionRequest<'a> {
    pub(crate) fn new(
        store: &'a mut MetadataStore,
        context_id: ContextId,
        artifact_id: ArtifactId,
    ) -> Self {
        Self {
            store,
            context_id,
            artifact_id,
        }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        self.store
            .put_relation(self.context_id, Id::Artifact(self.artifact_id))
            .await
    }
}

#[derive(Debug)]
pub struct PutAssociationRequest<'a> {
    store: &'a mut MetadataStore,
    context_id: ContextId,
    execution_id: ExecutionId,
}

impl<'a> PutAssociationRequest<'a> {
    pub(crate) fn new(
        store: &'a mut MetadataStore,
        context_id: ContextId,
        execution_id: ExecutionId,
    ) -> Self {
        Self {
            store,
            context_id,
            execution_id,
        }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        self.store
            .put_relation(self.context_id, Id::Execution(self.execution_id))
            .await
    }
}

#[derive(Debug)]
pub struct PutEventRequest<'a> {
    store: &'a mut MetadataStore,
    execution_id: ExecutionId,
    artifact_id: ArtifactId,
    options: options::PutEventOptions,
}

impl<'a> PutEventRequest<'a> {
    pub(crate) fn new(
        store: &'a mut MetadataStore,
        execution_id: ExecutionId,
        artifact_id: ArtifactId,
    ) -> Self {
        Self {
            store,
            execution_id,
            artifact_id,
            options: Default::default(),
        }
    }

    pub fn ty(mut self, event_type: EventType) -> Self {
        self.options.event_type = event_type;
        self
    }

    pub fn path(mut self, path: &[EventStep]) -> Self {
        self.options.path.extend_from_slice(path);
        self
    }

    pub fn step(mut self, step: EventStep) -> Self {
        self.options.path.push(step);
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.create_time_since_epoch = time;
        self
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        self.store
            .execute_put_event(self.execution_id, self.artifact_id, self.options)
            .await
    }
}

#[derive(Debug)]
pub struct GetEventsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetEventsOptions,
}

impl<'a> GetEventsRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    pub fn execution(mut self, id: ExecutionId) -> Self {
        self.options.execution_ids.push(id);
        self
    }

    pub fn executions(mut self, ids: &[ExecutionId]) -> Self {
        self.options.execution_ids.extend(ids);
        self
    }

    pub fn artifact(mut self, id: ArtifactId) -> Self {
        self.options.artifact_ids.push(id);
        self
    }

    pub fn artifacts(mut self, ids: &[ArtifactId]) -> Self {
        self.options.artifact_ids.extend(ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<Event>, errors::GetError> {
        self.store.execute_get_events(self.options).await
    }
}
