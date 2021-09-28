//! Builders of GET, PUT and POST requests that will be issued via [`MetadataStore`].
use crate::errors::{GetError, PostError, PutError};
use crate::metadata::{
    Artifact, ArtifactId, ArtifactState, ArtifactType, Context, ContextId, ContextType, Event,
    EventStep, EventType, Execution, ExecutionId, ExecutionState, ExecutionType, Id, PropertyType,
    PropertyTypes, PropertyValue, PropertyValues, TypeId, TypeKind,
};
use crate::metadata_store::{options, MetadataStore};
use std::iter;
use std::ops::{Bound, Range, RangeBounds};
use std::time::Duration;

/// Possible values for [`GetArtifactsRequest::order_by`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ArtifactOrderByField {
    Id,
    Name,
    Uri,
    CreateTime,
    UpdateTime,
}

impl ArtifactOrderByField {
    pub(crate) fn field_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::Uri => "uri",
            Self::CreateTime => "create_time_since_epoch",
            Self::UpdateTime => "last_update_time_since_epoch",
        }
    }
}

/// Possible values for [`GetExecutionsRequest::order_by`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ExecutionOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ExecutionOrderByField {
    pub(crate) fn field_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::CreateTime => "create_time_since_epoch",
            Self::UpdateTime => "last_update_time_since_epoch",
        }
    }
}

/// Possible values for [`GetContextsRequest::order_by`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ContextOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ContextOrderByField {
    pub(crate) fn field_name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
            Self::CreateTime => "create_time_since_epoch",
            Self::UpdateTime => "last_update_time_since_epoch",
        }
    }
}

/// Possible values for [`GetEventsRequest::order_by`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum EventOrderByField {
    CreateTime,
}

impl EventOrderByField {
    pub(crate) fn field_name(self) -> &'static str {
        match self {
            Self::CreateTime => "milliseconds_since_epoch",
        }
    }
}

/// Request builder for [`MetadataStore::put_artifact_type`].
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

    /// When specified, stored properties can be omitted in the request type.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the stored type has properties not in the request type.
    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    /// When specified, new properties can be added.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the request type has properties that are not in the stored type.
    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    /// Adds properties to the type.
    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties.extend(properties);
        self
    }

    /// Adds a property to the type.
    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    /// Inserts or updates an artifact type and returns the identifier of that.
    ///
    /// See [the official API doc](https://www.tensorflow.org/tfx/ml_metadata/api_docs/python/mlmd/metadata_store/MetadataStore#put_artifact_type) for the details.
    pub async fn execute(self) -> Result<TypeId, PutError> {
        self.store
            .execute_put_type(TypeKind::Artifact, &self.type_name, self.options)
            .await
    }
}

/// Request builder for [`MetadataStore::get_artifact_types`].
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

    /// Specifies the type name of the target types.
    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    /// Specifies the ID of the target type.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, type_id: TypeId) -> Self {
        self.ids(iter::once(type_id))
    }

    /// Specifies the ID set of the target types.
    pub fn ids(mut self, type_ids: impl Iterator<Item = TypeId>) -> Self {
        self.options.ids = type_ids.collect();
        self
    }

    /// Gets specified artifact types.
    ///
    /// If multiple conditions are specified, types which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<ArtifactType>, GetError> {
        self.store
            .execute_get_types(
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

/// Request builder for [`MetadataStore::put_execution_type`].
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

    /// When specified, stored properties can be omitted in the request type.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the stored type has properties not in the request type.
    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    /// When specified, new properties can be added.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the request type has properties that are not in the stored type.
    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    /// Adds properties to the type.
    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds a property to the type.
    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    /// Inserts or updates an execution type and returns the identifier of that.
    ///
    /// See [the official API doc](https://www.tensorflow.org/tfx/ml_metadata/api_docs/python/mlmd/metadata_store/MetadataStore#put_execution_type) for the details.
    pub async fn execute(self) -> Result<TypeId, PutError> {
        self.store
            .execute_put_type(TypeKind::Execution, &self.type_name, self.options)
            .await
    }
}

/// Request builder for [`MetadataStore::get_execution_types`].
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

    /// Specifies the type name of the target types.
    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    /// Specifies the ID of the target type.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, type_id: TypeId) -> Self {
        self.ids(iter::once(type_id))
    }

    /// Specifies the ID set of the target types.
    pub fn ids(mut self, type_ids: impl Iterator<Item = TypeId>) -> Self {
        self.options.ids = type_ids.collect();
        self
    }

    /// Gets specified execution types.
    ///
    /// If multiple conditions are specified, types which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<ExecutionType>, GetError> {
        self.store
            .execute_get_types(
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

/// Request builder for [`MetadataStore::put_context_type`].
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

    /// When specified, stored properties can be omitted in the request type.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the stored type has properties not in the request type.
    pub fn can_omit_fields(mut self) -> Self {
        self.options.can_omit_fields = true;
        self
    }

    /// When specified, new properties can be added.
    ///
    /// Otherwise, returns [`PutError::TypeAlreadyExists`]
    /// if the request type has properties that are not in the stored type.
    pub fn can_add_fields(mut self) -> Self {
        self.options.can_add_fields = true;
        self
    }

    /// Adds properties to the type.
    pub fn properties(mut self, properties: PropertyTypes) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds a property to the type.
    pub fn property(mut self, name: &str, ty: PropertyType) -> Self {
        self.options.properties.insert(name.to_owned(), ty);
        self
    }

    /// Inserts or updates a context type and returns the identifier of that.
    ///
    /// See [the official API doc](https://www.tensorflow.org/tfx/ml_metadata/api_docs/python/mlmd/metadata_store/MetadataStore#put_context_type) for the details.
    pub async fn execute(self) -> Result<TypeId, PutError> {
        self.store
            .execute_put_type(TypeKind::Context, &self.type_name, self.options)
            .await
    }
}

/// Request builder for [`MetadataStore::get_context_types`].
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

    /// Specifies the type name of the target types.
    pub fn name(mut self, type_name: &str) -> Self {
        self.options.name = Some(type_name.to_owned());
        self
    }

    /// Specifies the ID of the target type.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, type_id: TypeId) -> Self {
        self.ids(iter::once(type_id))
    }

    /// Specifies the ID set of the target types.
    pub fn ids(mut self, type_ids: impl Iterator<Item = TypeId>) -> Self {
        self.options.ids = type_ids.collect();
        self
    }

    /// Gets specified context types.
    ///
    /// If multiple conditions are specified, types which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<ContextType>, GetError> {
        self.store
            .execute_get_types(
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

/// Request builder for [`MetadataStore::get_artifacts`].
#[derive(Debug)]
pub struct GetArtifactsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetArtifactsOptions,
}

impl<'a> GetArtifactsRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    /// Specifies the type of the target artifacts.
    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    /// Specifies the type and name of the target artifact.
    pub fn type_and_name(mut self, type_name: &str, artifact_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.artifact_name = Some(artifact_name.to_owned());
        self.options.artifact_name_pattern = None;
        self
    }

    /// Specifies the type and name pattern of the target artifacts.
    ///
    /// `artifact_name_pattern` can contain wildcard characters for the SQL LIKE statement.
    pub fn type_and_name_pattern(mut self, type_name: &str, artifact_name_pattern: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.artifact_name_pattern = Some(artifact_name_pattern.to_owned());
        self.options.artifact_name = None;
        self
    }

    /// Specifies the ID of the target artifact.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, artifact_id: ArtifactId) -> Self {
        self.ids(iter::once(artifact_id))
    }

    /// Specifies the ID set of the target artifacts.
    pub fn ids(mut self, artifact_ids: impl Iterator<Item = ArtifactId>) -> Self {
        self.options.artifact_ids = artifact_ids.collect();
        self
    }

    /// Specifies the URI of the target artifacts.
    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    /// Specifies the context to which the target artifacts belong.
    pub fn context(mut self, context_id: ContextId) -> Self {
        self.options.context_id = Some(context_id);
        self
    }

    /// Specifies how to order the result.
    pub fn order_by(mut self, field: ArtifactOrderByField, asc: bool) -> Self {
        self.options.order_by = Some(field);
        self.options.desc = !asc;
        self
    }

    /// Specifies the maximum number of the returned artifacts.
    pub fn limit(mut self, n: usize) -> Self {
        self.options.limit = Some(n);
        self
    }

    /// Specifies how many leading artifacts are skipped from the result.
    ///
    /// Note that if `GetArtifactsRequest::limit` is not specified, this option has no effect.
    pub fn offset(mut self, n: usize) -> Self {
        self.options.offset = Some(n);
        self
    }

    /// Specifies creation time range.
    pub fn create_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.create_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Specifies update time range.
    pub fn update_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.update_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Gets specified artifacts.
    ///
    /// If multiple conditions are specified, those which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<Artifact>, GetError> {
        self.store
            .execute_get_items(options::GetItemsOptions::Artifact(self.options))
            .await
    }

    /// Returns the number of artifacts that satisfy the specified conditions.
    ///
    /// This is equivalent to calling `self.execute().await?.len()` but more efficient.
    pub async fn count(self) -> Result<usize, GetError> {
        self.store
            .execute_count_items(options::GetItemsOptions::Artifact(self.options))
            .await
    }
}

fn clone_bound(x: Bound<&Duration>) -> Bound<Duration> {
    match x {
        Bound::Excluded(x) => Bound::Excluded(*x),
        Bound::Included(x) => Bound::Included(*x),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Request builder for [`MetadataStore::get_executions`].
#[derive(Debug)]
pub struct GetExecutionsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetExecutionsOptions,
}

impl<'a> GetExecutionsRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    /// Specifies the type of the target executions.
    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    /// Specifies the type and name of the target execution.
    pub fn type_and_name(mut self, type_name: &str, execution_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.execution_name = Some(execution_name.to_owned());
        self.options.execution_name_pattern = None;
        self
    }

    /// Specifies the type and name pattern of the target executions.
    ///
    /// `execution_name_pattern` can contain wildcard characters for the SQL LIKE statement.
    pub fn type_and_name_pattern(mut self, type_name: &str, execution_name_pattern: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.execution_name_pattern = Some(execution_name_pattern.to_owned());
        self.options.execution_name = None;
        self
    }

    /// Specifies the ID of the target execution.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, execution_id: ExecutionId) -> Self {
        self.ids(iter::once(execution_id))
    }

    /// Specifies the ID set of the target executions.
    pub fn ids(mut self, execution_ids: impl Iterator<Item = ExecutionId>) -> Self {
        self.options.execution_ids = execution_ids.collect();
        self
    }

    /// Specifies the context to which the target executions belong.
    pub fn context(mut self, context_id: ContextId) -> Self {
        self.options.context_id = Some(context_id);
        self
    }

    /// Specifies how to order the result.
    pub fn order_by(mut self, field: ExecutionOrderByField, asc: bool) -> Self {
        self.options.order_by = Some(field);
        self.options.desc = !asc;
        self
    }

    /// Specifies the maximum number of the returned executions.
    pub fn limit(mut self, n: usize) -> Self {
        self.options.limit = Some(n);
        self
    }

    /// Specifies how many leading executions are skipped from the result.
    ///
    /// Note that if `GetExecutionsRequest::limit` is not specified, this option has no effect.
    pub fn offset(mut self, n: usize) -> Self {
        self.options.offset = Some(n);
        self
    }

    /// Specifies creation time range.
    pub fn create_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.create_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Specifies update time range.
    pub fn update_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.update_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Gets specified executions.
    ///
    /// If multiple conditions are specified, those which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<Execution>, GetError> {
        self.store
            .execute_get_items(options::GetItemsOptions::Execution(self.options))
            .await
    }

    /// Returns the number of executions that satisfy the specified conditions.
    ///
    /// This is equivalent to calling `self.execute().await?.len()` but more efficient.
    pub async fn count(self) -> Result<usize, GetError> {
        self.store
            .execute_count_items(options::GetItemsOptions::Execution(self.options))
            .await
    }
}

/// Request builder for [`MetadataStore::get_contexts`].
#[derive(Debug)]
pub struct GetContextsRequest<'a> {
    store: &'a mut MetadataStore,
    options: options::GetContextsOptions,
}

impl<'a> GetContextsRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore) -> Self {
        Self {
            store,
            options: Default::default(),
        }
    }

    /// Specifies the type of the target contexts.
    pub fn ty(mut self, type_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self
    }

    /// Specifies the type and name of the target context.
    pub fn type_and_name(mut self, type_name: &str, context_name: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.context_name = Some(context_name.to_owned());
        self.options.context_name_pattern = None;
        self
    }

    /// Specifies the type and name pattern of the target contexts.
    ///
    /// `context_name_pattern` can contain wildcard characters for the SQL LIKE statement.
    pub fn type_and_name_pattern(mut self, type_name: &str, context_name_pattern: &str) -> Self {
        self.options.type_name = Some(type_name.to_owned());
        self.options.context_name_pattern = Some(context_name_pattern.to_owned());
        self.options.context_name = None;
        self
    }

    /// Specifies the ID of the target context.
    ///
    /// If you need to specify multiple IDs, please use [`ids`](Self::ids) instead.
    pub fn id(self, context_id: ContextId) -> Self {
        self.ids(iter::once(context_id))
    }

    /// Specifies the ID set of the target contexts.
    pub fn ids(mut self, context_ids: impl Iterator<Item = ContextId>) -> Self {
        self.options.context_ids = context_ids.collect();
        self
    }

    /// Specifies the artifact attributed to the target context.
    pub fn artifact(self, artifact_id: ArtifactId) -> Self {
        self.artifacts(iter::once(artifact_id))
    }

    /// Specifies the artifacts attributed to the target context.
    pub fn artifacts(mut self, artifact_ids: impl Iterator<Item = ArtifactId>) -> Self {
        self.options.artifact_ids = artifact_ids.collect();
        self
    }

    /// Specifies the execution associated to the target context.
    pub fn execution(self, execution_id: ExecutionId) -> Self {
        self.executions(iter::once(execution_id))
    }

    /// Specifies the executions associated to the target context.
    pub fn executions(mut self, execution_ids: impl Iterator<Item = ExecutionId>) -> Self {
        self.options.execution_ids = execution_ids.collect();
        self
    }

    /// Specifies how to order the result.
    pub fn order_by(mut self, field: ContextOrderByField, asc: bool) -> Self {
        self.options.order_by = Some(field);
        self.options.desc = !asc;
        self
    }

    /// Specifies the maximum number of the returned contexts.
    pub fn limit(mut self, n: usize) -> Self {
        self.options.limit = Some(n);
        self
    }

    /// Specifies how many leading contexts are skipped from the result.
    ///
    /// Note that if `GetContextsRequest::limit` is not specified, this option has no effect.
    pub fn offset(mut self, n: usize) -> Self {
        self.options.offset = Some(n);
        self
    }

    /// Specifies creation time range.
    pub fn create_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.create_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Specifies update time range.
    pub fn update_time(mut self, range: impl RangeBounds<Duration>) -> Self {
        self.options.update_time = Some(Range {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        });
        self
    }

    /// Gets specified contexts.
    ///
    /// If multiple conditions are specified, those which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<Context>, GetError> {
        self.store
            .execute_get_items(options::GetItemsOptions::Context(self.options))
            .await
    }

    /// Returns the number of contexts that satisfy the specified conditions.
    ///
    /// This is equivalent to calling `self.execute().await?.len()` but more efficient.
    pub async fn count(self) -> Result<usize, GetError> {
        self.store
            .execute_count_items(options::GetItemsOptions::Context(self.options))
            .await
    }
}

/// Request builder for [`MetadataStore::post_artifact`].
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

    /// Sets the name of the artifact.
    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    /// Sets the URI of the artifact.
    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    /// Adds properties to the artifact.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the artifact.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the artifact.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the artifact.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Sets the state of the artifact.
    pub fn state(mut self, state: ArtifactState) -> Self {
        self.options.state = Some(state);
        self
    }

    /// Creates a new artifact and returns the ID.
    pub async fn execute(self) -> Result<ArtifactId, PostError> {
        self.store
            .execute_post_item(self.type_id, options::ItemOptions::Artifact(self.options))
            .await
            .map(ArtifactId::new)
    }
}

/// Request builder for [`MetadataStore::post_execution`].
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

    /// Sets the name of the execution.
    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    /// Adds properties to the execution.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the execution.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the execution.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the execution.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Sets the state of the execution.
    pub fn state(mut self, state: ExecutionState) -> Self {
        self.options.last_known_state = Some(state);
        self
    }

    /// Creates a new execution and returns the ID.
    pub async fn execute(self) -> Result<ExecutionId, PostError> {
        self.store
            .execute_post_item(self.type_id, options::ItemOptions::Execution(self.options))
            .await
            .map(ExecutionId::new)
    }
}

/// Request builder for [`MetadataStore::post_context`].
#[derive(Debug)]
pub struct PostContextRequest<'a> {
    store: &'a mut MetadataStore,
    type_id: TypeId,
    options: options::ContextOptions,
}

impl<'a> PostContextRequest<'a> {
    pub(crate) fn new(store: &'a mut MetadataStore, type_id: TypeId, context_name: &str) -> Self {
        let options = options::ContextOptions {
            name: Some(context_name.to_owned()),
            ..Default::default()
        };
        Self {
            store,
            type_id,
            options,
        }
    }

    /// Adds properties to the context.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the context.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the context.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the context.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Creates a new context and returns the ID.
    pub async fn execute(self) -> Result<ContextId, PostError> {
        self.store
            .execute_post_item(self.type_id, options::ItemOptions::Context(self.options))
            .await
            .map(ContextId::new)
    }
}

/// Request builder for [`MetadataStore::put_artifact`].
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

    /// Sets the name of the artifact.
    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    /// Sets the URI of the artifact.
    pub fn uri(mut self, uri: &str) -> Self {
        self.options.uri = Some(uri.to_owned());
        self
    }

    /// Adds properties to the artifact.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the artifact.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the artifact.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the artifact.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Sets the state of the artifact.
    pub fn state(mut self, state: ArtifactState) -> Self {
        self.options.state = Some(state);
        self
    }

    /// Updates this artifact.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_item(
                Id::Artifact(self.id),
                options::ItemOptions::Artifact(self.options),
            )
            .await
    }
}

/// Request builder for [`MetadataStore::put_execution`].
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

    /// Sets the name of the execution.
    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    /// Adds properties to the execution.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the execution.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the execution.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the execution.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Sets the state of the execution.
    pub fn state(mut self, state: ExecutionState) -> Self {
        self.options.last_known_state = Some(state);
        self
    }

    /// Updates this execution.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_item(
                Id::Execution(self.id),
                options::ItemOptions::Execution(self.options),
            )
            .await
    }
}

/// Request builder for [`MetadataStore::put_context`].
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

    /// Sets the name of the context.
    pub fn name(mut self, name: &str) -> Self {
        self.options.name = Some(name.to_owned());
        self
    }

    /// Adds properties to the context.
    pub fn properties(mut self, properties: PropertyValues) -> Self {
        self.options.properties = properties;
        self
    }

    /// Adds custom properties to the context.
    pub fn custom_properties(mut self, properties: PropertyValues) -> Self {
        self.options.custom_properties = properties;
        self
    }

    /// Adds a property to the context.
    pub fn property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options.properties.insert(key.to_owned(), value.into());
        self
    }

    /// Adds a custom property to the context.
    pub fn custom_property<T>(mut self, key: &str, value: T) -> Self
    where
        T: Into<PropertyValue>,
    {
        self.options
            .custom_properties
            .insert(key.to_owned(), value.into());
        self
    }

    /// Update this context.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_item(
                Id::Context(self.id),
                options::ItemOptions::Context(self.options),
            )
            .await
    }
}

/// Request builder for [`MetadataStore::put_attribution`].
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

    /// Inserts a new attribution.
    ///
    /// If the same entry already exists, this call will be just ignored.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_relation(self.context_id, Id::Artifact(self.artifact_id))
            .await
    }
}

/// Request builder for [`MetadataStore::put_association`].
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

    /// Inserts a new association.
    ///
    /// If the same entry already exists, this call will be just ignored.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_relation(self.context_id, Id::Execution(self.execution_id))
            .await
    }
}

/// Request builder for [`MetadataStore::put_event`].
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

    /// Sets the type of this event.
    pub fn ty(mut self, event_type: EventType) -> Self {
        self.options.event_type = event_type;
        self
    }

    /// Adds a peth (i.e., steps) to this event.
    pub fn path(mut self, path: impl Iterator<Item = EventStep>) -> Self {
        self.options.path.extend(path);
        self
    }

    /// Adds a step to this event.
    pub fn step(mut self, step: EventStep) -> Self {
        self.options.path.push(step);
        self
    }

    /// Inserts a new event.
    pub async fn execute(self) -> Result<(), PutError> {
        self.store
            .execute_put_event(self.execution_id, self.artifact_id, self.options)
            .await
    }
}

/// Request builder for [`MetadataStore::get_events`].
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

    /// Specifies the execution related to the target event.
    ///
    /// If you need to specify multiple executions, please use [`executions`](Self::executions) instead.
    pub fn execution(self, id: ExecutionId) -> Self {
        self.executions(iter::once(id))
    }

    /// Specifies the executions related to the target events.
    pub fn executions(mut self, ids: impl Iterator<Item = ExecutionId>) -> Self {
        self.options.execution_ids = ids.collect();
        self
    }

    /// Specifies the artifact related to the target event.
    ///
    /// If you need to specify multiple artifacts, please use [`artifacts`](Self::artifacts) instead.
    pub fn artifact(self, id: ArtifactId) -> Self {
        self.artifacts(iter::once(id))
    }

    /// Specifies the artifacts related to the target events.
    pub fn artifacts(mut self, ids: impl Iterator<Item = ArtifactId>) -> Self {
        self.options.artifact_ids = ids.collect();
        self
    }

    /// Specifies the maximum number of the returned events.
    pub fn limit(mut self, n: usize) -> Self {
        self.options.limit = Some(n);
        self
    }

    /// Specifies how many leading events are skipped from the result.
    ///
    /// Note that if `GetEventsRequest::limit` is not specified, this option has no effect.
    pub fn offset(mut self, n: usize) -> Self {
        self.options.offset = Some(n);
        self
    }

    /// Specifies how to order the result.
    pub fn order_by(mut self, field: EventOrderByField, asc: bool) -> Self {
        self.options.order_by = Some(field);
        self.options.desc = !asc;
        self
    }

    /// Gets specified events.
    ///
    /// If multiple conditions are specified, those which satisfy all the conditions are returned.
    pub async fn execute(self) -> Result<Vec<Event>, GetError> {
        self.store.execute_get_events(self.options).await
    }

    /// Returns the number of events that satisfy the specified conditions.
    ///
    /// This is equivalent to calling `self.execute().await?.len()` but more efficient.
    pub async fn count(self) -> Result<usize, GetError> {
        self.store.execute_count_events(self.options).await
    }
}
