use self::errors::{GetError, InitError, PostError, PutError, PutTypeError};
use self::options::{
    GetArtifactsOptions, GetContextsOptions, GetEventsOptions, GetExecutionsOptions,
    GetTypesOptions, PostArtifactOptions, PostContextOptions, PostExecutionOptions,
    PutEventOptions, PutTypeOptions,
};
use crate::metadata::{
    Artifact, ArtifactState, Context, Event, EventStep, EventType, Execution, ExecutionState, Id,
    PropertyType, Value,
};
use crate::query::{self, Query, TypeKind};
use crate::requests;
use futures::TryStreamExt as _;
use sqlx::{AnyConnection, Connection as _, Row as _};
use std::collections::BTreeMap;
use std::time::Duration;

pub mod errors;
pub mod options;
#[cfg(test)]
mod tests;

const SCHEMA_VERSION: i32 = 6;

#[derive(Debug)]
pub struct MetadataStore {
    connection: sqlx::AnyConnection,
    query: Query,
}

impl MetadataStore {
    pub async fn new(database_uri: &str) -> Result<Self, InitError> {
        let query = if database_uri.starts_with("sqlite:") {
            Query::sqlite()
        } else if database_uri.starts_with("mysql:") {
            Query::mysql()
        } else {
            return Err(InitError::UnsupportedDatabase);
        };

        let connection = AnyConnection::connect(database_uri).await?;
        let mut this = Self { connection, query };
        this.initialize_database().await?;
        this.check_schema_version().await?;
        Ok(this)
    }

    pub fn put_artifact_type(&mut self, type_name: &str) -> requests::PutArtifactTypeRequest {
        requests::PutArtifactTypeRequest::new(self, type_name)
    }

    pub fn get_artifact_types(&mut self) -> requests::GetArtifactTypesRequest {
        requests::GetArtifactTypesRequest::new(self)
    }

    pub fn put_execution_type(&mut self, type_name: &str) -> requests::PutExecutionTypeRequest {
        requests::PutExecutionTypeRequest::new(self, type_name)
    }

    pub fn get_execution_types(&mut self) -> requests::GetExecutionTypesRequest {
        requests::GetExecutionTypesRequest::new(self)
    }

    pub fn put_context_type(&mut self, type_name: &str) -> requests::PutContextTypeRequest {
        requests::PutContextTypeRequest::new(self, type_name)
    }

    pub fn get_context_types(&mut self) -> requests::GetContextTypesRequest {
        requests::GetContextTypesRequest::new(self)
    }

    pub async fn post_artifact(
        &mut self,
        type_id: Id,
        options: PostArtifactOptions,
    ) -> Result<Id, PostError> {
        let artifact_type = self
            .get_artifact_types()
            .id(type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PostError::TypeNotFound)?;
        for (name, value) in &options.properties {
            if artifact_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PostError::UndefinedProperty);
            }
        }

        let mut connection = self.connection.begin().await?;

        if let Some(name) = &options.name {
            let count: i32 = sqlx::query_scalar(self.query.check_artifact_name())
                .bind(type_id.get())
                .bind(name)
                .bind(-1) // dummy artifact id (TODO)
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PostError::NameConflict);
            }
        }

        let sql = self.query.insert_artifact(&options);
        let mut query = sqlx::query(&sql)
            .bind(type_id.get())
            .bind(options.state as i32)
            .bind(options.create_time_since_epoch.as_millis() as i64)
            .bind(options.last_update_time_since_epoch.as_millis() as i64);
        if let Some(v) = &options.name {
            query = query.bind(v);
        }
        if let Some(v) = &options.uri {
            query = query.bind(v);
        }
        query.execute(&mut connection).await?;

        let artifact_id: i32 = sqlx::query_scalar(self.query.get_last_artifact_id())
            .fetch_one(&mut connection)
            .await?;

        for (name, value, is_custom) in options
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(options.custom_properties.iter().map(|(k, v)| (k, v, true)))
        {
            let sql = self.query.upsert_artifact_property(value);
            let mut query = sqlx::query(&sql)
                .bind(artifact_id)
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(Id::new(artifact_id))
    }

    // TODO: remove redundant code
    pub async fn put_artifact(&mut self, artifact: &Artifact) -> Result<(), PutError> {
        let artifact_type = self
            .get_artifact_types()
            .id(artifact.type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PutError::TypeNotFound)?;
        for (name, value) in &artifact.properties {
            if artifact_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PutError::UndefinedProperty);
            }
        }

        // TODO: check if type id is unchanged
        let old = self
            .get_artifact(artifact.id)
            .await?
            .ok_or_else(|| PutError::NotFound)?;
        if old.type_id != artifact.type_id {
            return Err(PutError::WrongTypeId);
        }

        let mut connection = self.connection.begin().await?;

        if let Some(name) = &artifact.name {
            let count: i32 = sqlx::query_scalar(self.query.check_artifact_name())
                .bind(artifact.type_id.get())
                .bind(name)
                .bind(artifact.id.get())
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PutError::NameConflict);
            }
        }

        let sql = self.query.update_artifact(&artifact);
        let mut query = sqlx::query(&sql)
            .bind(artifact.state as i32)
            .bind(artifact.create_time_since_epoch.as_millis() as i64)
            .bind(artifact.last_update_time_since_epoch.as_millis() as i64);
        if let Some(v) = &artifact.name {
            query = query.bind(v);
        }
        if let Some(v) = &artifact.uri {
            query = query.bind(v);
        }
        query
            .bind(artifact.id.get())
            .execute(&mut connection)
            .await?;

        for (name, value, is_custom) in artifact
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(artifact.custom_properties.iter().map(|(k, v)| (k, v, true)))
        {
            let sql = self.query.upsert_artifact_property(value);
            dbg!(&sql);
            let mut query = sqlx::query(&sql)
                .bind(artifact.id.get())
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(())
    }

    pub async fn get_artifact(&mut self, artifact_id: Id) -> Result<Option<Artifact>, GetError> {
        let artifacts = self
            .get_artifacts(GetArtifactsOptions::default().ids(&[artifact_id]))
            .await?;
        Ok(artifacts.into_iter().nth(0))
    }

    pub async fn get_artifacts(
        &mut self,
        options: GetArtifactsOptions,
    ) -> Result<Vec<Artifact>, GetError> {
        let sql = self.query.get_artifacts(&options);
        let mut query = sqlx::query_as::<_, query::Artifact>(&sql);
        for v in options.values() {
            match v {
                query::QueryValue::Str(v) => {
                    query = query.bind(v);
                }
                query::QueryValue::Int(v) => {
                    query = query.bind(v);
                }
            }
        }

        let mut artifacts = BTreeMap::new();
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            artifacts.insert(
                row.id,
                Artifact {
                    id: Id::new(row.id),
                    type_id: Id::new(row.type_id),
                    name: row.name,
                    uri: row.uri,
                    properties: BTreeMap::new(),
                    custom_properties: BTreeMap::new(),
                    state: ArtifactState::from_i32(row.state)?,
                    create_time_since_epoch: Duration::from_millis(
                        row.create_time_since_epoch as u64,
                    ),
                    last_update_time_since_epoch: Duration::from_millis(
                        row.last_update_time_since_epoch as u64,
                    ),
                },
            );
        }
        std::mem::drop(rows);

        let sql = self.query.get_artifact_properties(artifacts.len());
        let mut query = sqlx::query_as::<_, query::ArtifactProperty>(&sql);
        for id in artifacts.keys() {
            query = query.bind(*id);
        }
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let artifact = artifacts.get_mut(&row.artifact_id).expect("bug");
            let is_custom_property = row.is_custom_property;
            let (name, value) = row.into_name_and_vaue()?;
            if is_custom_property {
                artifact.custom_properties.insert(name, value);
            } else {
                artifact.properties.insert(name, value);
            }
        }

        Ok(artifacts.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn post_execution(
        &mut self,
        type_id: Id,
        options: PostExecutionOptions,
    ) -> Result<Id, PostError> {
        let execution_type = self
            .get_execution_types()
            .id(type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PostError::TypeNotFound)?;
        for (name, value) in &options.properties {
            if execution_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PostError::UndefinedProperty);
            }
        }

        let mut connection = self.connection.begin().await?;

        if let Some(name) = &options.name {
            let count: i32 = sqlx::query_scalar(self.query.check_execution_name())
                .bind(type_id.get())
                .bind(name)
                .bind(-1) // dummy execution id (TODO)
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PostError::NameConflict);
            }
        }

        let sql = self.query.insert_execution(&options);
        let mut query = sqlx::query(&sql)
            .bind(type_id.get())
            .bind(options.last_known_state as i32)
            .bind(options.create_time_since_epoch.as_millis() as i64)
            .bind(options.last_update_time_since_epoch.as_millis() as i64);
        if let Some(v) = &options.name {
            query = query.bind(v);
        }
        query.execute(&mut connection).await?;

        let execution_id: i32 = sqlx::query_scalar(self.query.get_last_execution_id())
            .fetch_one(&mut connection)
            .await?;

        for (name, value, is_custom) in options
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(options.custom_properties.iter().map(|(k, v)| (k, v, true)))
        {
            let sql = self.query.upsert_execution_property(value);
            let mut query = sqlx::query(&sql)
                .bind(execution_id)
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(Id::new(execution_id))
    }

    // TODO: remove redundant code
    pub async fn put_execution(&mut self, execution: &Execution) -> Result<(), PutError> {
        let execution_type = self
            .get_execution_types()
            .id(execution.type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PutError::TypeNotFound)?;
        for (name, value) in &execution.properties {
            if execution_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PutError::UndefinedProperty);
            }
        }

        // TODO: check if type id is unchanged
        let old = self
            .get_execution(execution.id)
            .await?
            .ok_or_else(|| PutError::NotFound)?;
        if old.type_id != execution.type_id {
            return Err(PutError::WrongTypeId);
        }

        let mut connection = self.connection.begin().await?;

        if let Some(name) = &execution.name {
            let count: i32 = sqlx::query_scalar(self.query.check_execution_name())
                .bind(execution.type_id.get())
                .bind(name)
                .bind(execution.id.get())
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PutError::NameConflict);
            }
        }

        let sql = self.query.update_execution(&execution);
        let mut query = sqlx::query(&sql)
            .bind(execution.last_known_state as i32)
            .bind(execution.create_time_since_epoch.as_millis() as i64)
            .bind(execution.last_update_time_since_epoch.as_millis() as i64);
        if let Some(v) = &execution.name {
            query = query.bind(v);
        }
        query
            .bind(execution.id.get())
            .execute(&mut connection)
            .await?;

        for (name, value, is_custom) in execution
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(
                execution
                    .custom_properties
                    .iter()
                    .map(|(k, v)| (k, v, true)),
            )
        {
            let sql = self.query.upsert_execution_property(value);
            dbg!(&sql);
            let mut query = sqlx::query(&sql)
                .bind(execution.id.get())
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(())
    }

    pub async fn get_execution(&mut self, execution_id: Id) -> Result<Option<Execution>, GetError> {
        let executions = self
            .get_executions(GetExecutionsOptions::default().ids(&[execution_id]))
            .await?;
        Ok(executions.into_iter().nth(0))
    }

    pub async fn get_executions(
        &mut self,
        options: GetExecutionsOptions,
    ) -> Result<Vec<Execution>, GetError> {
        let sql = self.query.get_executions(&options);
        let mut query = sqlx::query_as::<_, query::Execution>(&sql);
        for v in options.values() {
            match v {
                query::QueryValue::Str(v) => {
                    query = query.bind(v);
                }
                query::QueryValue::Int(v) => {
                    query = query.bind(v);
                }
            }
        }

        let mut executions = BTreeMap::new();
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            executions.insert(
                row.id,
                Execution {
                    id: Id::new(row.id),
                    type_id: Id::new(row.type_id),
                    name: row.name,
                    properties: BTreeMap::new(),
                    custom_properties: BTreeMap::new(),
                    last_known_state: ExecutionState::from_i32(row.last_known_state)?,
                    create_time_since_epoch: Duration::from_millis(
                        row.create_time_since_epoch as u64,
                    ),
                    last_update_time_since_epoch: Duration::from_millis(
                        row.last_update_time_since_epoch as u64,
                    ),
                },
            );
        }
        std::mem::drop(rows);

        let sql = self.query.get_execution_properties(executions.len());
        let mut query = sqlx::query_as::<_, query::ExecutionProperty>(&sql);
        for id in executions.keys() {
            query = query.bind(*id);
        }
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let execution = executions.get_mut(&row.execution_id).expect("bug");
            let is_custom_property = row.is_custom_property;
            let (name, value) = row.into_name_and_vaue()?;
            if is_custom_property {
                execution.custom_properties.insert(name, value);
            } else {
                execution.properties.insert(name, value);
            }
        }

        Ok(executions.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn post_context(
        &mut self,
        type_id: Id,
        context_name: &str,
        options: PostContextOptions,
    ) -> Result<Id, PostError> {
        let context_type = self
            .get_context_types()
            .id(type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PostError::TypeNotFound)?;
        for (name, value) in &options.properties {
            if context_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PostError::UndefinedProperty);
            }
        }

        let mut connection = self.connection.begin().await?;

        let count: i32 = sqlx::query_scalar(self.query.check_context_name())
            .bind(type_id.get())
            .bind(context_name)
            .bind(-1) // dummy context id (TODO)
            .fetch_one(&mut connection)
            .await?;
        if count > 0 {
            return Err(PostError::NameConflict);
        }

        let sql = self.query.insert_context();
        sqlx::query(&sql)
            .bind(type_id.get())
            .bind(options.create_time_since_epoch.as_millis() as i64)
            .bind(options.last_update_time_since_epoch.as_millis() as i64)
            .bind(context_name)
            .execute(&mut connection)
            .await?;

        let context_id: i32 = sqlx::query_scalar(self.query.get_last_context_id())
            .fetch_one(&mut connection)
            .await?;

        for (name, value, is_custom) in options
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(options.custom_properties.iter().map(|(k, v)| (k, v, true)))
        {
            let sql = self.query.upsert_context_property(value);
            let mut query = sqlx::query(&sql)
                .bind(context_id)
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(Id::new(context_id))
    }

    // TODO: remove redundant code
    pub async fn put_context(&mut self, context: &Context) -> Result<(), PutError> {
        let context_type = self
            .get_context_types()
            .id(context.type_id)
            .execute()
            .await?
            .into_iter()
            .nth(0)
            .ok_or_else(|| PutError::TypeNotFound)?;
        for (name, value) in &context.properties {
            if context_type.properties.get(name).copied() != Some(value.ty()) {
                return Err(PutError::UndefinedProperty);
            }
        }

        // TODO: check if type id is unchanged
        let old = self
            .get_context(context.id)
            .await?
            .ok_or_else(|| PutError::NotFound)?;
        if old.type_id != context.type_id {
            return Err(PutError::WrongTypeId);
        }

        let mut connection = self.connection.begin().await?;

        let count: i32 = sqlx::query_scalar(self.query.check_context_name())
            .bind(context.type_id.get())
            .bind(&context.name)
            .bind(context.id.get())
            .fetch_one(&mut connection)
            .await?;
        if count > 0 {
            return Err(PutError::NameConflict);
        }

        let sql = self.query.update_context();
        sqlx::query(&sql)
            .bind(context.create_time_since_epoch.as_millis() as i64)
            .bind(context.last_update_time_since_epoch.as_millis() as i64)
            .bind(&context.name)
            .bind(context.id.get())
            .execute(&mut connection)
            .await?;

        for (name, value, is_custom) in context
            .properties
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(context.custom_properties.iter().map(|(k, v)| (k, v, true)))
        {
            let sql = self.query.upsert_context_property(value);
            dbg!(&sql);
            let mut query = sqlx::query(&sql)
                .bind(context.id.get())
                .bind(name)
                .bind(is_custom);
            query = match value {
                Value::Int(v) => query.bind(*v),
                Value::Double(v) => query.bind(*v),
                Value::String(v) => query.bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(())
    }

    pub async fn get_context(&mut self, context_id: Id) -> Result<Option<Context>, GetError> {
        let contexts = self
            .get_contexts(GetContextsOptions::default().ids(&[context_id]))
            .await?;
        Ok(contexts.into_iter().nth(0))
    }

    pub async fn get_contexts(
        &mut self,
        options: GetContextsOptions,
    ) -> Result<Vec<Context>, GetError> {
        let sql = self.query.get_contexts(&options);
        let mut query = sqlx::query_as::<_, query::Context>(&sql);
        for v in options.values() {
            match v {
                query::QueryValue::Str(v) => {
                    query = query.bind(v);
                }
                query::QueryValue::Int(v) => {
                    query = query.bind(v);
                }
            }
        }

        let mut contexts = BTreeMap::new();
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            contexts.insert(
                row.id,
                Context {
                    id: Id::new(row.id),
                    type_id: Id::new(row.type_id),
                    name: row.name,
                    properties: BTreeMap::new(),
                    custom_properties: BTreeMap::new(),
                    create_time_since_epoch: Duration::from_millis(
                        row.create_time_since_epoch as u64,
                    ),
                    last_update_time_since_epoch: Duration::from_millis(
                        row.last_update_time_since_epoch as u64,
                    ),
                },
            );
        }
        std::mem::drop(rows);

        let sql = self.query.get_context_properties(contexts.len());
        let mut query = sqlx::query_as::<_, query::ContextProperty>(&sql);
        for id in contexts.keys() {
            query = query.bind(*id);
        }
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let context = contexts.get_mut(&row.context_id).expect("bug");
            let is_custom_property = row.is_custom_property;
            let (name, value) = row.into_name_and_vaue()?;
            if is_custom_property {
                context.custom_properties.insert(name, value);
            } else {
                context.properties.insert(name, value);
            }
        }

        Ok(contexts.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn put_attribution(
        &mut self,
        context_id: Id,
        artifact_id: Id,
    ) -> Result<(), PutError> {
        // TODO: check whether context and artifact exist
        sqlx::query(self.query.insert_attribution())
            .bind(context_id.get())
            .bind(artifact_id.get())
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }

    pub async fn put_association(
        &mut self,
        context_id: Id,
        execution_id: Id,
    ) -> Result<(), PutError> {
        // TODO: check whether context and execution exist
        sqlx::query(self.query.insert_association())
            .bind(context_id.get())
            .bind(execution_id.get())
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }

    pub async fn put_event(
        &mut self,
        event_type: EventType,
        artifact_id: Id,
        execution_id: Id,
        options: PutEventOptions,
    ) -> Result<(), PutError> {
        // TODO: check whether artifact and execution exist
        let mut connection = self.connection.begin().await?;

        sqlx::query(self.query.insert_event())
            .bind(artifact_id.get())
            .bind(execution_id.get())
            .bind(event_type as i32)
            .bind(options.create_time_since_epoch.as_millis() as i64)
            .execute(&mut connection)
            .await?;
        let event_id: i32 = sqlx::query_scalar(self.query.get_last_event_id())
            .fetch_one(&mut connection)
            .await?;

        for step in &options.path {
            let sql = self.query.insert_event_path(step);
            let query = match step {
                EventStep::Index(v) => sqlx::query(&sql).bind(event_id).bind(*v),
                EventStep::Key(v) => sqlx::query(&sql).bind(event_id).bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(())
    }

    pub async fn get_events(&mut self, options: GetEventsOptions) -> Result<Vec<Event>, GetError> {
        let sql = self.query.get_events(&options);
        let mut query = sqlx::query_as::<_, query::Event>(&sql);
        for id in &options.artifact_ids {
            query = query.bind(id.get());
        }
        for id in &options.execution_ids {
            query = query.bind(id.get());
        }

        let mut events = BTreeMap::new();
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            events.insert(
                row.id,
                Event {
                    artifact_id: Id::new(row.artifact_id),
                    execution_id: Id::new(row.execution_id),
                    path: Vec::new(),
                    ty: EventType::from_i32(row.ty)?,
                    create_time_since_epoch: Duration::from_millis(
                        row.milliseconds_since_epoch as u64,
                    ),
                },
            );
        }
        std::mem::drop(rows);

        let sql = self.query.get_event_paths(events.len());
        let mut query = sqlx::query_as::<_, query::EventPath>(&sql);
        for id in events.keys().cloned() {
            query = query.bind(id);
        }

        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let event = events.get_mut(&row.event_id).expect("bug");
            event.path.push(if row.is_index_step {
                EventStep::Index(row.step_index.expect("TODO"))
            } else {
                EventStep::Key(row.step_key.expect("TODO"))
            });
        }

        Ok(events.into_iter().map(|(_, v)| v).collect())
    }

    async fn initialize_database(&mut self) -> Result<(), InitError> {
        if sqlx::query(self.query.select_schema_version())
            .fetch_all(&mut self.connection)
            .await
            .is_ok()
        {
            return Ok(());
        }

        for query in self.query.create_tables() {
            sqlx::query(query).execute(&mut self.connection).await?;
        }

        Ok(())
    }

    async fn check_schema_version(&mut self) -> Result<(), InitError> {
        let rows = sqlx::query(self.query.select_schema_version())
            .fetch_all(&mut self.connection)
            .await?;

        if rows.is_empty() {
            sqlx::query(self.query.insert_schema_version())
                .bind(SCHEMA_VERSION)
                .execute(&mut self.connection)
                .await?;
            return Ok(());
        }

        if rows.len() > 1 {
            return Err(InitError::TooManyMlmdEnvRecords { count: rows.len() });
        }

        let version: i32 = rows[0].try_get("schema_version")?;
        if version != SCHEMA_VERSION {
            return Err(InitError::UnsupportedSchemaVersion {
                actual: version,
                expected: SCHEMA_VERSION,
            });
        }
        Ok(())
    }

    pub(crate) async fn put_type(
        &mut self,
        type_kind: TypeKind,
        type_name: &str,
        mut options: PutTypeOptions,
    ) -> Result<Id, PutTypeError> {
        let mut connection = self.connection.begin().await?;
        let ty = sqlx::query_as::<_, query::Type>(self.query.get_type_by_name())
            .bind(type_kind as i32)
            .bind(type_name)
            .fetch_optional(&mut connection)
            .await?;
        let ty = if let Some(ty) = ty {
            let properties = sqlx::query_as::<_, query::TypeProperty>(
                self.query.get_type_properties_by_type_id(),
            )
            .bind(ty.id)
            .fetch_all(&mut connection)
            .await?;

            for property in properties {
                match options.properties.remove(&property.name) {
                    None if options.can_omit_fields => {}
                    Some(v) if v as i32 == property.data_type => {}
                    _ => {
                        return Err(PutTypeError::AlreadyExists {
                            kind: type_kind.as_str(),
                            name: type_name.to_owned(),
                        });
                    }
                }
            }
            if !options.properties.is_empty() && !options.can_add_fields {
                return Err(PutTypeError::AlreadyExists {
                    kind: type_kind.as_str(),
                    name: type_name.to_owned(),
                });
            }

            ty
        } else {
            sqlx::query(self.query.insert_type())
                .bind(type_kind as i32)
                .bind(type_name)
                .execute(&mut connection)
                .await?;

            sqlx::query_as::<_, query::Type>(self.query.get_type_by_name())
                .bind(type_kind as i32)
                .bind(type_name)
                .fetch_one(&mut connection)
                .await?
        };
        for (name, value) in &options.properties {
            sqlx::query(self.query.insert_type_property())
                .bind(ty.id)
                .bind(name)
                .bind(*value as i32)
                .execute(&mut connection)
                .await?;
        }
        connection.commit().await?;

        Ok(Id::new(ty.id))
    }

    pub(crate) async fn get_types<F, T>(
        &mut self,
        type_kind: TypeKind,
        f: F,
        options: GetTypesOptions,
    ) -> Result<Vec<T>, GetError>
    where
        F: Fn(Id, String, BTreeMap<String, PropertyType>) -> T,
    {
        let sql = self.query.get_types(&options);
        let mut query = sqlx::query_as::<_, query::Type>(&sql).bind(type_kind as i32);
        if let Some(v) = &options.name {
            query = query.bind(v);
        }
        for id in &options.ids {
            query = query.bind(id.get());
        }

        let mut types = BTreeMap::new();
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            types.insert(row.id, (row.name, BTreeMap::new()));
        }
        std::mem::drop(rows);

        let mut rows = sqlx::query_as::<_, query::TypeProperty>(self.query.get_type_properties())
            .fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            if let Some(ty) = types.get_mut(&row.type_id) {
                ty.1.insert(row.name, PropertyType::from_i32(row.data_type)?);
            }
        }

        Ok(types
            .into_iter()
            .map(|(id, (name, properties))| f(Id::new(id), name, properties))
            .collect())
    }
}
