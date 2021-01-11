use self::errors::{GetError, InitError, PostError, PutError};
use self::options::{
    GetArtifactsOptions, GetExecutionsOptions, PostArtifactOptions, PostExecutionOptions,
    PutTypeOptions,
};
use crate::metadata::{
    Artifact, ArtifactState, ArtifactType, ContextType, Execution, ExecutionState, ExecutionType,
    Id, PropertyType, Value,
};
use crate::query::{self, Query, TypeKind};
use futures::TryStreamExt as _;
use sqlx::{AnyConnection, Connection as _, Row as _};
use std::collections::BTreeMap;
use std::time::Duration;

mod errors;
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

    pub async fn put_artifact_type(
        &mut self,
        type_name: &str,
        options: PutTypeOptions,
    ) -> Result<Id, PutError> {
        self.put_type(TypeKind::Artifact, type_name, options).await
    }

    // TODO: get_artifact_types(&mut self, options: GetTypeOptions) -> Result<Vec<ArtifactType>, GetError>>
    pub async fn get_artifact_type(&mut self, type_name: &str) -> Result<ArtifactType, GetError> {
        let (id, properties) = self.get_type(TypeKind::Artifact, type_name).await?;
        Ok(ArtifactType {
            id,
            name: type_name.to_owned(),
            properties,
        })
    }

    pub async fn get_artifact_types(&mut self) -> Result<Vec<ArtifactType>, GetError> {
        let types = self
            .get_types(TypeKind::Artifact, |id, name, properties| ArtifactType {
                id,
                name,
                properties,
            })
            .await?;
        Ok(types)
    }

    pub async fn put_execution_type(
        &mut self,
        type_name: &str,
        options: PutTypeOptions,
    ) -> Result<Id, PutError> {
        self.put_type(TypeKind::Execution, type_name, options).await
    }

    pub async fn get_execution_type(&mut self, type_name: &str) -> Result<ExecutionType, GetError> {
        let (id, properties) = self.get_type(TypeKind::Execution, type_name).await?;
        Ok(ExecutionType {
            id,
            name: type_name.to_owned(),
            properties,
        })
    }

    pub async fn get_execution_types(&mut self) -> Result<Vec<ExecutionType>, GetError> {
        let types = self
            .get_types(TypeKind::Execution, |id, name, properties| ExecutionType {
                id,
                name,
                properties,
            })
            .await?;
        Ok(types)
    }

    pub async fn put_context_type(
        &mut self,
        type_name: &str,
        options: PutTypeOptions,
    ) -> Result<Id, PutError> {
        self.put_type(TypeKind::Context, type_name, options).await
    }

    pub async fn get_context_type(&mut self, type_name: &str) -> Result<ContextType, GetError> {
        let (id, properties) = self.get_type(TypeKind::Context, type_name).await?;
        Ok(ContextType {
            id,
            name: type_name.to_owned(),
            properties,
        })
    }

    pub async fn get_context_types(&mut self) -> Result<Vec<ContextType>, GetError> {
        let types = self
            .get_types(TypeKind::Context, |id, name, properties| ContextType {
                id,
                name,
                properties,
            })
            .await?;
        Ok(types)
    }

    pub async fn post_artifact(
        &mut self,
        type_id: Id,
        options: PostArtifactOptions,
    ) -> Result<Id, PostError> {
        // TODO: optimize
        let artifact_type = self
            .get_artifact_types()
            .await?
            .into_iter()
            .find(|a| a.id == type_id)
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
        // TODO: optimize
        let artifact_type = self
            .get_artifact_types()
            .await?
            .into_iter()
            .find(|a| a.id == artifact.type_id)
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
        // TODO: optimize
        let execution_type = self
            .get_execution_types()
            .await?
            .into_iter()
            .find(|a| a.id == type_id)
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
        // TODO: optimize
        let execution_type = self
            .get_execution_types()
            .await?
            .into_iter()
            .find(|a| a.id == execution.type_id)
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

    async fn put_type(
        &mut self,
        type_kind: TypeKind,
        type_name: &str,
        mut options: PutTypeOptions,
    ) -> Result<Id, PutError> {
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
                        return Err(PutError::TypeAlreadyExists {
                            kind: type_kind.as_str(),
                            name: type_name.to_owned(),
                        });
                    }
                }
            }
            if !options.properties.is_empty() && !options.can_add_fields {
                return Err(PutError::TypeAlreadyExists {
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

    async fn get_types<F, T>(&mut self, type_kind: TypeKind, f: F) -> Result<Vec<T>, GetError>
    where
        F: Fn(Id, String, BTreeMap<String, PropertyType>) -> T,
    {
        let mut types = BTreeMap::new();
        let mut rows = sqlx::query_as::<_, query::Type>(self.query.get_types())
            .bind(type_kind as i32)
            .fetch(&mut self.connection);
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

    async fn get_type(
        &mut self,
        type_kind: TypeKind,
        type_name: &str,
    ) -> Result<(Id, BTreeMap<String, PropertyType>), GetError> {
        let ty = sqlx::query_as::<_, query::Type>(self.query.get_type_by_name())
            .bind(type_kind as i32)
            .bind(type_name)
            .fetch_optional(&mut self.connection)
            .await?;
        let ty = ty.ok_or_else(|| GetError::NotFound {
            target: format!("artifact type with the name {:?}", type_name),
        })?;

        let mut properties = BTreeMap::new();
        let mut rows =
            sqlx::query_as::<_, query::TypeProperty>(self.query.get_type_properties_by_type_id())
                .bind(ty.id)
                .fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            properties.insert(row.name, PropertyType::from_i32(row.data_type)?);
        }

        Ok((Id::new(ty.id), properties))
    }
}
