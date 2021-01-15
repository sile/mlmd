use crate::errors;
use crate::metadata::{
    Artifact, ArtifactState, ArtifactType, Context, ContextType, Event, EventStep, EventType,
    Execution, ExecutionState, ExecutionType, Id, PropertyType, Value,
};
use crate::metadata_store::{options, MetadataStore};
use crate::query;
use std::collections::BTreeMap;
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

    pub fn properties(mut self, properties: BTreeMap<String, PropertyType>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PutError> {
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

    pub async fn execute(self) -> Result<Id, errors::PutError> {
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

    pub async fn execute(self) -> Result<Id, errors::PutError> {
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

    pub fn id(mut self, artifact_id: Id) -> Self {
        self.options.artifact_ids.push(artifact_id);
        self
    }

    pub fn ids(mut self, artifact_ids: &[Id]) -> Self {
        self.options.artifact_ids.extend(artifact_ids);
        self
    }

    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    pub fn context(mut self, context_id: Id) -> Self {
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

    pub fn id(mut self, execution_id: Id) -> Self {
        self.options.execution_ids.push(execution_id);
        self
    }

    pub fn ids(mut self, execution_ids: &[Id]) -> Self {
        self.options.execution_ids.extend(execution_ids);
        self
    }

    pub fn context(mut self, context_id: Id) -> Self {
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

    pub fn id(mut self, context_id: Id) -> Self {
        self.options.context_ids.push(context_id);
        self
    }

    pub fn ids(mut self, context_ids: &[Id]) -> Self {
        self.options.context_ids.extend(context_ids);
        self
    }

    pub fn artifact(mut self, artifact_id: Id) -> Self {
        self.options.artifact_id = Some(artifact_id);
        self
    }

    pub fn execution(mut self, execution_id: Id) -> Self {
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
    type_id: Id,
    options: options::PostArtifactOptions,
}

impl<'a> PostArtifactRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: Id) -> Self {
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

    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn state(mut self, state: ArtifactState) -> Self {
        self.options.state = state;
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.create_time_since_epoch = time;
        self
    }

    pub fn last_update_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.last_update_time_since_epoch = time;
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PostError> {
        let generator = query::PostArtifactQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            options: self.options,
        };
        self.store.post_item(self.type_id, generator).await
    }
}

#[derive(Debug)]
pub struct PostExecutionRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: Id,
    options: options::PostExecutionOptions,
}

impl<'a> PostExecutionRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: Id) -> Self {
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

    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn last_known_state(mut self, state: ExecutionState) -> Self {
        self.options.last_known_state = state;
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.create_time_since_epoch = time;
        self
    }

    pub fn last_update_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.last_update_time_since_epoch = time;
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PostError> {
        let generator = query::PostExecutionQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            options: self.options,
        };
        self.store.post_item(self.type_id, generator).await
    }
}

#[derive(Debug)]
pub struct PostContextRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: Id,
    name: String,
    options: options::PostContextOptions,
}

impl<'a> PostContextRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: Id, context_name: &str) -> Self {
        Self {
            store,
            type_id,
            name: context_name.to_owned(),
            options: Default::default(),
        }
    }

    pub fn properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.properties = properties;
        self
    }

    pub fn custom_properties(mut self, properties: BTreeMap<String, Value>) -> Self {
        self.options.custom_properties = properties;
        self
    }

    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    pub fn create_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.create_time_since_epoch = time;
        self
    }

    pub fn last_update_time_since_epoch(mut self, time: Duration) -> Self {
        self.options.last_update_time_since_epoch = time;
        self
    }

    pub async fn execute(self) -> Result<Id, errors::PostError> {
        let generator = query::PostContextQueryGenerator {
            query: self.store.query.clone(),
            type_id: self.type_id,
            name: self.name,
            options: self.options,
        };
        self.store.post_item(self.type_id, generator).await
    }
}

#[derive(Debug)]
pub struct PutArtifactRequest<'a> {
    store: &'a mut MetadataStore,
    item: Artifact,
}

impl<'a> PutArtifactRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, item: Artifact) -> Self {
        Self { store, item }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutArtifactQueryGenerator {
            query: self.store.query.clone(),
            item: self.item,
        };
        self.store.put_item(generator).await
    }
}

#[derive(Debug)]
pub struct PutExecutionRequest<'a> {
    store: &'a mut MetadataStore,
    item: Execution,
}

impl<'a> PutExecutionRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, item: Execution) -> Self {
        Self { store, item }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutExecutionQueryGenerator {
            query: self.store.query.clone(),
            item: self.item,
        };
        self.store.put_item(generator).await
    }
}

#[derive(Debug)]
pub struct PutContextRequest<'a> {
    store: &'a mut MetadataStore,
    item: Context,
}

impl<'a> PutContextRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, item: Context) -> Self {
        Self { store, item }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        let generator = query::PutContextQueryGenerator {
            query: self.store.query.clone(),
            item: self.item,
        };
        self.store.put_item(generator).await
    }
}

#[derive(Debug)]
pub struct PutAttributionRequest<'a> {
    store: &'a mut MetadataStore,
    context_id: Id,
    artifact_id: Id,
}

impl<'a> PutAttributionRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, context_id: Id, artifact_id: Id) -> Self {
        Self {
            store,
            context_id,
            artifact_id,
        }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        self.store
            .put_relation(self.context_id, self.artifact_id, true)
            .await
    }
}

#[derive(Debug)]
pub struct PutAssociationRequest<'a> {
    store: &'a mut MetadataStore,
    context_id: Id,
    execution_id: Id,
}

impl<'a> PutAssociationRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, context_id: Id, execution_id: Id) -> Self {
        Self {
            store,
            context_id,
            execution_id,
        }
    }

    pub async fn execute(self) -> Result<(), errors::PutError> {
        self.store
            .put_relation(self.context_id, self.execution_id, false)
            .await
    }
}

#[derive(Debug)]
pub struct PutEventRequest<'a> {
    store: &'a mut MetadataStore,
    execution_id: Id,
    artifact_id: Id,
    options: options::PutEventOptions,
}

impl<'a> PutEventRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, execution_id: Id, artifact_id: Id) -> Self {
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

    pub fn execution(mut self, id: Id) -> Self {
        self.options.execution_ids.push(id);
        self
    }

    pub fn executions(mut self, ids: &[Id]) -> Self {
        self.options.execution_ids.extend(ids);
        self
    }

    pub fn artifact(mut self, id: Id) -> Self {
        self.options.artifact_ids.push(id);
        self
    }

    pub fn artifacts(mut self, ids: &[Id]) -> Self {
        self.options.artifact_ids.extend(ids);
        self
    }

    pub async fn execute(self) -> Result<Vec<Event>, errors::GetError> {
        self.store.execute_get_events(self.options).await
    }
}
