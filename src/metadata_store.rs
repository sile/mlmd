use self::options::{GetEventsOptions, GetTypesOptions, PutEventOptions, PutTypeOptions};
use crate::errors::{GetError, InitError, PostError, PutError};
use crate::metadata::{
    ArtifactId, ContextId, Event, EventStep, EventType, ExecutionId, Id, PropertyType,
    PropertyValue, TypeId, TypeKind,
};
use crate::query::{self, GetItemsQueryGenerator, InsertProperty as _, Query};
use crate::requests;
use futures::TryStreamExt as _;
use sqlx::FromRow as _;
use sqlx::{AnyConnection, Connection as _, Row as _};
use std::collections::BTreeMap;
use std::time::Duration;

pub mod options;
#[cfg(test)]
mod tests;

const SCHEMA_VERSION: i32 = 6;

#[derive(Debug)]
pub struct MetadataStore {
    connection: sqlx::AnyConnection,
    pub(crate) query: Query, // TODO
}

impl MetadataStore {
    // TODO: connect
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

    pub(crate) async fn post_item<T>(
        &mut self,
        type_id: TypeId,
        generator: T,
    ) -> Result<i32, PostError>
    where
        T: query::PostItemQueryGenerator,
    {
        let property_types = self
            .get_type_properties(T::TYPE_KIND, type_id)
            .await?
            .ok_or_else(|| PostError::TypeNotFound {
                type_kind: T::TYPE_KIND,
                type_id,
            })?;
        for (name, value) in generator.item_properties() {
            if property_types.get(name).copied() != Some(value.ty()) {
                return Err(PostError::UndefinedProperty {
                    type_kind: T::TYPE_KIND,
                    type_id,
                    property_name: name.clone(),
                });
            }
        }

        let mut connection = self.connection.begin().await?;

        if let Some((sql, values)) = generator.generate_check_item_name_query() {
            let count: i32 = values
                .into_iter()
                .fold(sqlx::query_scalar(&sql), |q, v| v.bind_scalar(q))
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PostError::NameAlreadyExists {
                    type_kind: T::TYPE_KIND,
                    item_name: generator.item_name().expect("bug").to_owned(),
                });
            }
        }

        let (sql, values) = generator.generate_insert_item_query();
        values
            .into_iter()
            .fold(sqlx::query(&sql), |q, v| v.bind(q))
            .execute(&mut connection)
            .await?;

        let item_id: i32 = sqlx::query_scalar(generator.generate_last_item_id())
            .fetch_one(&mut connection)
            .await?;

        let properties = generator
            .item_properties()
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(
                generator
                    .item_custom_properties()
                    .iter()
                    .map(|(k, v)| (k, v, true)),
            );
        for (name, value, is_custom) in properties {
            let sql = generator.generate_upsert_item_property(value);
            let mut query = sqlx::query(&sql).bind(item_id).bind(name).bind(is_custom);
            query = match value {
                PropertyValue::Int(v) => query.bind(*v).bind(*v),
                PropertyValue::Double(v) => query.bind(*v).bind(*v),
                PropertyValue::String(v) => query.bind(v).bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(item_id)
    }

    pub fn post_artifact(&mut self, type_id: TypeId) -> requests::PostArtifactRequest {
        requests::PostArtifactRequest::new(self, type_id)
    }

    pub fn get_artifacts(&mut self) -> requests::GetArtifactsRequest {
        requests::GetArtifactsRequest::new(self)
    }

    async fn get_type_properties(
        &mut self,
        type_kind: TypeKind,
        type_id: TypeId,
    ) -> Result<Option<BTreeMap<String, PropertyType>>, GetError> {
        Ok(self
            .get_types(
                type_kind,
                |_, _, properties| properties,
                GetTypesOptions::by_id(type_id),
            )
            .await?
            .into_iter()
            .nth(0))
    }

    pub(crate) async fn put_item<T>(&mut self, item_id: Id, generator: T) -> Result<(), PutError>
    where
        T: query::PutItemQueryGenerator,
    {
        let type_kind = item_id.kind();
        let type_id = sqlx::query_scalar(generator.generate_get_type_id_query())
            .bind(item_id.get())
            .fetch_optional(&mut self.connection)
            .await?
            .map(TypeId::new)
            .ok_or_else(|| PutError::NotFound { item_id })?;

        let property_types = self
            .get_type_properties(type_kind, type_id)
            .await?
            .ok_or_else(|| PutError::TypeNotFound { type_id, item_id })?;
        for (name, value) in generator.item_properties() {
            if property_types.get(name).copied() != Some(value.ty()) {
                return Err(PutError::UndefinedProperty {
                    item_id,
                    property_name: name.clone(),
                    property_type: value.ty(),
                });
            }
        }

        let mut connection = self.connection.begin().await?;

        if let Some((sql, values)) = generator.generate_check_item_name_query(type_id) {
            let count: i32 = values
                .into_iter()
                .fold(sqlx::query_scalar(&sql), |q, v| v.bind_scalar(q))
                .fetch_one(&mut connection)
                .await?;
            if count > 0 {
                return Err(PutError::NameAlreadyExists {
                    item_id,
                    item_name: generator.item_name().expect("bug").to_owned(),
                });
            }
        }

        let (sql, values) = generator.generate_update_item_query(item_id);
        values
            .into_iter()
            .fold(sqlx::query(&sql), |q, v| v.bind(q))
            .execute(&mut connection)
            .await?;

        let properties = generator
            .item_properties()
            .iter()
            .map(|(k, v)| (k, v, false))
            .chain(
                generator
                    .item_custom_properties()
                    .iter()
                    .map(|(k, v)| (k, v, true)),
            );
        for (name, value, is_custom) in properties {
            let sql = generator.generate_upsert_item_property(value);
            let mut query = sqlx::query(&sql)
                .bind(item_id.get())
                .bind(name)
                .bind(is_custom);
            query = match value {
                PropertyValue::Int(v) => query.bind(*v).bind(*v),
                PropertyValue::Double(v) => query.bind(*v).bind(*v),
                PropertyValue::String(v) => query.bind(v).bind(v),
            };
            query.execute(&mut connection).await?;
        }

        connection.commit().await?;
        Ok(())
    }

    pub fn put_artifact(&mut self, artifact_id: ArtifactId) -> requests::PutArtifactRequest {
        requests::PutArtifactRequest::new(self, artifact_id)
    }

    pub(crate) async fn get_items<T>(&mut self, generator: T) -> Result<Vec<T::Item>, GetError>
    where
        T: GetItemsQueryGenerator,
    {
        let sql = generator.generate_select_items_sql();
        let mut rows = generator
            .query_values()
            .into_iter()
            .fold(sqlx::query(&sql), |q, v| v.bind(q))
            .fetch(&mut self.connection);
        let mut items = BTreeMap::new();
        while let Some(row) = rows.try_next().await? {
            let id: i32 = row.try_get("id")?;
            items.insert(id, T::Item::from_row(&row)?);
        }
        std::mem::drop(rows);

        if items.is_empty() {
            return Ok(Vec::new());
        }

        let sql = generator.generate_select_properties_sql(items.len());
        let mut query = sqlx::query_as::<_, query::Property>(&sql);
        for id in items.keys() {
            query = query.bind(*id);
        }
        let mut rows = query.fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let item = items.get_mut(&row.id).expect("bug");
            let is_custom_property = row.is_custom_property;
            let (name, value) = row.into_name_and_vaue()?;
            item.insert_property(is_custom_property, name, value);
        }

        Ok(items.into_iter().map(|(_, v)| v).collect())
    }

    pub fn post_execution(&mut self, type_id: TypeId) -> requests::PostExecutionRequest {
        requests::PostExecutionRequest::new(self, type_id)
    }

    pub fn put_execution(&mut self, execution_id: ExecutionId) -> requests::PutExecutionRequest {
        requests::PutExecutionRequest::new(self, execution_id)
    }

    pub fn get_executions(&mut self) -> requests::GetExecutionsRequest {
        requests::GetExecutionsRequest::new(self)
    }

    pub fn post_context(
        &mut self,
        type_id: TypeId,
        context_name: &str,
    ) -> requests::PostContextRequest {
        requests::PostContextRequest::new(self, type_id, context_name)
    }

    pub fn put_context(&mut self, context_id: ContextId) -> requests::PutContextRequest {
        requests::PutContextRequest::new(self, context_id)
    }

    pub fn get_contexts(&mut self) -> requests::GetContextsRequest {
        requests::GetContextsRequest::new(self)
    }

    pub(crate) async fn put_relation(
        &mut self,
        context_id: ContextId,
        item_id: Id,
    ) -> Result<(), PutError> {
        let is_attribution = matches!(item_id, Id::Artifact(_));
        let count: i32 = sqlx::query_scalar(self.query.check_context_id())
            .bind(context_id.get())
            .fetch_one(&mut self.connection)
            .await?;
        if count == 0 {
            return Err(PutError::NotFound {
                item_id: Id::Context(context_id),
            });
        }

        let count: i32 = sqlx::query_scalar(if is_attribution {
            self.query.check_artifact_id()
        } else {
            self.query.check_execution_id()
        })
        .bind(item_id.get())
        .fetch_one(&mut self.connection)
        .await?;
        if count == 0 {
            return Err(PutError::NotFound { item_id });
        }

        // TODO: check AlreadyExists (error or ignore)
        sqlx::query(if is_attribution {
            self.query.insert_attribution()
        } else {
            self.query.insert_association()
        })
        .bind(context_id.get())
        .bind(item_id.get())
        .execute(&mut self.connection)
        .await?;

        Ok(())
    }

    pub fn put_attribution(
        &mut self,
        context_id: ContextId,
        artifact_id: ArtifactId,
    ) -> requests::PutAttributionRequest {
        requests::PutAttributionRequest::new(self, context_id, artifact_id)
    }

    pub fn put_association(
        &mut self,
        context_id: ContextId,
        execution_id: ExecutionId,
    ) -> requests::PutAssociationRequest {
        requests::PutAssociationRequest::new(self, context_id, execution_id)
    }

    pub fn put_event(
        &mut self,
        execution_id: ExecutionId,
        artifact_id: ArtifactId,
    ) -> requests::PutEventRequest {
        requests::PutEventRequest::new(self, execution_id, artifact_id)
    }

    // TODO: rename
    pub(crate) async fn execute_put_event(
        &mut self,
        execution_id: ExecutionId,
        artifact_id: ArtifactId,
        options: PutEventOptions,
    ) -> Result<(), PutError> {
        let count: i32 = sqlx::query_scalar(self.query.check_execution_id())
            .bind(execution_id.get())
            .fetch_one(&mut self.connection)
            .await?;
        if count == 0 {
            return Err(PutError::NotFound {
                item_id: Id::Execution(execution_id),
            });
        }

        let count: i32 = sqlx::query_scalar(self.query.check_artifact_id())
            .bind(artifact_id.get())
            .fetch_one(&mut self.connection)
            .await?;
        if count == 0 {
            return Err(PutError::NotFound {
                item_id: Id::Artifact(artifact_id),
            });
        }

        let mut connection = self.connection.begin().await?;

        sqlx::query(self.query.insert_event())
            .bind(artifact_id.get())
            .bind(execution_id.get())
            .bind(options.event_type as i32)
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

    pub fn get_events(&mut self) -> requests::GetEventsRequest {
        requests::GetEventsRequest::new(self)
    }

    pub(crate) async fn execute_get_events(
        &mut self,
        options: GetEventsOptions,
    ) -> Result<Vec<Event>, GetError> {
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
                    artifact_id: ArtifactId::new(row.artifact_id),
                    execution_id: ExecutionId::new(row.execution_id),
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
    ) -> Result<TypeId, PutError> {
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
                            type_kind,
                            type_name: type_name.to_owned(),
                        });
                    }
                }
            }
            if !options.properties.is_empty() && !options.can_add_fields {
                return Err(PutError::TypeAlreadyExists {
                    type_kind,
                    type_name: type_name.to_owned(),
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

        Ok(TypeId::new(ty.id))
    }

    pub(crate) async fn get_types<F, T>(
        &mut self,
        type_kind: TypeKind,
        f: F,
        options: GetTypesOptions,
    ) -> Result<Vec<T>, GetError>
    where
        F: Fn(TypeId, String, BTreeMap<String, PropertyType>) -> T,
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
            .map(|(id, (name, properties))| f(TypeId::new(id), name, properties))
            .collect())
    }
}
