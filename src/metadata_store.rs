use self::errors::{GetError, InitError, PutError};
use self::options::PutTypeOptions;
use crate::metadata::{ArtifactType, ContextType, ExecutionType, Id, PropertyType};
use crate::query::{self, Query, TypeKind};
use futures::TryStreamExt as _;
use sqlx::{AnyConnection, Connection as _, Executor as _, Row as _};
use std::collections::BTreeMap;

mod errors;
mod options;

macro_rules! transaction {
    ($connection:expr, $block:expr) => {{
        $connection.execute("BEGIN").await?;
        let result = $block;
        if result.is_ok() {
            $connection.execute("COMMIT").await?;
        } else {
            $connection.execute("ROLLBACK").await?;
        }
        result
    }};
}

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

    pub async fn put_artifact_type(
        &mut self,
        type_name: &str,
        options: PutTypeOptions,
    ) -> Result<Id, PutError> {
        transaction!(
            self.connection,
            self.put_type(TypeKind::Artifact, type_name, options).await
        )
    }

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
        transaction!(
            self.connection,
            self.put_type(TypeKind::Execution, type_name, options).await
        )
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
        transaction!(
            self.connection,
            self.put_type(TypeKind::Context, type_name, options).await
        )
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

    async fn put_type(
        &mut self,
        type_kind: TypeKind,
        type_name: &str,
        mut options: PutTypeOptions,
    ) -> Result<Id, PutError> {
        let ty = sqlx::query_as::<_, query::Type>(self.query.get_type_by_name())
            .bind(type_kind as i32)
            .bind(type_name)
            .fetch_optional(&mut self.connection)
            .await?;
        let ty = if let Some(ty) = ty {
            let properties = sqlx::query_as::<_, query::TypeProperty>(
                self.query.get_type_properties_by_type_id(),
            )
            .bind(ty.id)
            .fetch_all(&mut self.connection)
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
                .execute(&mut self.connection)
                .await?;

            let ty = sqlx::query_as::<_, query::Type>(self.query.get_type_by_name())
                .bind(type_kind as i32)
                .bind(type_name)
                .fetch_one(&mut self.connection)
                .await?;
            ty
        };
        for (name, value) in &options.properties {
            sqlx::query(self.query.insert_type_property())
                .bind(ty.id)
                .bind(name)
                .bind(*value as i32)
                .execute(&mut self.connection)
                .await?;
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[async_std::test]
    async fn initialization_works() {
        // Create a new database.
        let file = NamedTempFile::new().unwrap();
        MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        // Open an existing database.
        let file = existing_db();
        MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();
    }

    #[async_std::test]
    async fn put_artifact_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        store
            .put_artifact_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();

        assert!(matches!(
            store
                .put_artifact_type("t0", options.clone().property_double("p0"))
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));

        assert!(matches!(
            store
                .put_artifact_type(
                    "t0",
                    options.clone().property_int("p0").property_string("p1")
                )
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_artifact_type(
                "t0",
                options
                    .clone()
                    .can_add_fields()
                    .property_int("p0")
                    .property_string("p1"),
            )
            .await
            .unwrap();

        assert!(matches!(
            store.put_artifact_type("t0", options.clone()).await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_artifact_type("t0", options.clone().can_omit_fields())
            .await
            .unwrap();

        store
            .put_artifact_type("t1", options.clone())
            .await
            .unwrap();
    }

    #[async_std::test]
    async fn get_artifact_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        let t0_id = store
            .put_artifact_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();
        let t1_id = store
            .put_artifact_type("t1", options.clone())
            .await
            .unwrap();
        assert_ne!(t0_id, t1_id);

        assert_eq!(
            store.get_artifact_type("t0").await.unwrap(),
            ArtifactType {
                id: t0_id,
                name: "t0".to_owned(),
                properties: vec![("p0".to_owned(), PropertyType::Int)]
                    .into_iter()
                    .collect()
            }
        );
        assert_eq!(
            store.get_artifact_type("t1").await.unwrap(),
            ArtifactType {
                id: t1_id,
                name: "t1".to_owned(),
                properties: BTreeMap::new(),
            }
        );
        assert!(matches!(
            store.get_artifact_type("t2").await.err(),
            Some(GetError::NotFound { .. })
        ));
    }

    #[async_std::test]
    async fn get_artifact_types_works() {
        let file = existing_db();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();
        let types = store.get_artifact_types().await.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].name, "Trainer");
    }

    #[async_std::test]
    async fn put_execution_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        store
            .put_execution_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();

        assert!(matches!(
            store
                .put_execution_type("t0", options.clone().property_double("p0"))
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));

        assert!(matches!(
            store
                .put_execution_type(
                    "t0",
                    options.clone().property_int("p0").property_string("p1")
                )
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_execution_type(
                "t0",
                options
                    .clone()
                    .can_add_fields()
                    .property_int("p0")
                    .property_string("p1"),
            )
            .await
            .unwrap();

        assert!(matches!(
            store.put_execution_type("t0", options.clone()).await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_execution_type("t0", options.clone().can_omit_fields())
            .await
            .unwrap();

        store
            .put_execution_type("t1", options.clone())
            .await
            .unwrap();
    }

    #[async_std::test]
    async fn get_execution_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        let t0_id = store
            .put_execution_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();
        let t1_id = store
            .put_execution_type("t1", options.clone())
            .await
            .unwrap();
        assert_ne!(t0_id, t1_id);

        assert_eq!(
            store.get_execution_type("t0").await.unwrap(),
            ExecutionType {
                id: t0_id,
                name: "t0".to_owned(),
                properties: vec![("p0".to_owned(), PropertyType::Int)]
                    .into_iter()
                    .collect()
            }
        );
        assert_eq!(
            store.get_execution_type("t1").await.unwrap(),
            ExecutionType {
                id: t1_id,
                name: "t1".to_owned(),
                properties: BTreeMap::new(),
            }
        );
        assert!(matches!(
            store.get_execution_type("t2").await.err(),
            Some(GetError::NotFound { .. })
        ));
    }

    #[async_std::test]
    async fn get_execution_types_works() {
        let file = existing_db();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();
        let types = store.get_execution_types().await.unwrap();
        assert_eq!(types.len(), 2);
        assert_eq!(types[0].name, "DataSet");
        assert_eq!(types[1].name, "SavedModel");
    }

    #[async_std::test]
    async fn put_context_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        store
            .put_context_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();

        assert!(matches!(
            store
                .put_context_type("t0", options.clone().property_double("p0"))
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));

        assert!(matches!(
            store
                .put_context_type(
                    "t0",
                    options.clone().property_int("p0").property_string("p1")
                )
                .await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_context_type(
                "t0",
                options
                    .clone()
                    .can_add_fields()
                    .property_int("p0")
                    .property_string("p1"),
            )
            .await
            .unwrap();

        assert!(matches!(
            store.put_context_type("t0", options.clone()).await,
            Err(PutError::TypeAlreadyExists { .. })
        ));
        store
            .put_context_type("t0", options.clone().can_omit_fields())
            .await
            .unwrap();

        store.put_context_type("t1", options.clone()).await.unwrap();
    }

    #[async_std::test]
    async fn get_context_type_works() {
        let file = NamedTempFile::new().unwrap();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

        let options = PutTypeOptions::default();
        let t0_id = store
            .put_context_type("t0", options.clone().property_int("p0"))
            .await
            .unwrap();
        let t1_id = store.put_context_type("t1", options.clone()).await.unwrap();
        assert_ne!(t0_id, t1_id);

        assert_eq!(
            store.get_context_type("t0").await.unwrap(),
            ContextType {
                id: t0_id,
                name: "t0".to_owned(),
                properties: vec![("p0".to_owned(), PropertyType::Int)]
                    .into_iter()
                    .collect()
            }
        );
        assert_eq!(
            store.get_context_type("t1").await.unwrap(),
            ContextType {
                id: t1_id,
                name: "t1".to_owned(),
                properties: BTreeMap::new(),
            }
        );
        assert!(matches!(
            store.get_context_type("t2").await.err(),
            Some(GetError::NotFound { .. })
        ));
    }

    #[async_std::test]
    async fn get_context_types_works() {
        let file = existing_db();
        let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();
        let types = store.get_context_types().await.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].name, "Experiment");
    }

    fn sqlite_uri(path: impl AsRef<std::path::Path>) -> String {
        format!(
            "sqlite://{}",
            path.as_ref()
                .to_str()
                .ok_or_else(|| format!("invalid path: {:?}", path.as_ref()))
                .unwrap()
        )
    }

    fn existing_db() -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("cannot create a temporary file");
        std::io::copy(
            &mut std::fs::File::open("tests/test.db").expect("cannot open 'tests/test.db'"),
            &mut file,
        )
        .expect("cannot copy the existing database file");
        file
    }
}
