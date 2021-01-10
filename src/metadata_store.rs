use self::errors::{GetError, InitError, PutError};
use self::options::PutArtifactTypeOptions;
use crate::metadata::{ArtifactType, Id, PropertyType};
use crate::query::Query;
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
        options: PutArtifactTypeOptions,
    ) -> Result<Id, PutError> {
        transaction!(
            self.connection,
            self.put_artifact_type_without_transaction(type_name, options)
                .await
        )
    }

    async fn put_artifact_type_without_transaction(
        &mut self,
        type_name: &str,
        mut options: PutArtifactTypeOptions,
    ) -> Result<Id, PutError> {
        #[derive(Debug, sqlx::FromRow)]
        struct Type {
            id: i32,
        }

        #[derive(Debug, sqlx::FromRow)]
        struct Property {
            name: String,
            data_type: i32,
        }

        let ty = sqlx::query_as::<_, Type>(self.query.get_artifact_type())
            .bind(type_name)
            .fetch_optional(&mut self.connection)
            .await?;
        let ty = if let Some(ty) = ty {
            let properties =
                sqlx::query_as::<_, Property>(self.query.get_artifact_type_properties())
                    .bind(ty.id)
                    .fetch_all(&mut self.connection)
                    .await?;

            for property in properties {
                match options.properties.remove(&property.name) {
                    None if options.can_omit_fields => {}
                    Some(v) if v as i32 == property.data_type => {}
                    _ => {
                        return Err(PutError::TypeAlreadyExists {
                            kind: "artifact".to_owned(),
                            name: type_name.to_owned(),
                        });
                    }
                }
            }
            if !options.properties.is_empty() && !options.can_add_fields {
                return Err(PutError::TypeAlreadyExists {
                    kind: "artifact".to_owned(),
                    name: type_name.to_owned(),
                });
            }

            ty
        } else {
            sqlx::query(self.query.insert_artifact_type())
                .bind(type_name)
                .execute(&mut self.connection)
                .await?;

            let ty = sqlx::query_as::<_, Type>(self.query.get_artifact_type())
                .bind(type_name)
                .fetch_one(&mut self.connection)
                .await?;
            ty
        };
        for (name, value) in &options.properties {
            sqlx::query(self.query.insert_artifact_type_property())
                .bind(ty.id)
                .bind(name)
                .bind(*value as i32)
                .execute(&mut self.connection)
                .await?;
        }

        Ok(Id::new(ty.id))
    }

    pub async fn get_artifact_type(&mut self, type_name: &str) -> Result<ArtifactType, GetError> {
        #[derive(Debug, sqlx::FromRow)]
        struct Type {
            id: i32,
        }

        #[derive(Debug, sqlx::FromRow)]
        struct Property {
            name: String,
            data_type: i32,
        }

        let ty = sqlx::query_as::<_, Type>(self.query.get_artifact_type())
            .bind(type_name)
            .fetch_optional(&mut self.connection)
            .await?;
        let ty = ty.ok_or_else(|| GetError::NotFound {
            target: format!("artifact type with the name {:?}", type_name),
        })?;
        let properties = sqlx::query_as::<_, Property>(self.query.get_artifact_type_properties())
            .bind(ty.id)
            .fetch_all(&mut self.connection)
            .await?;

        let mut artifact = ArtifactType {
            id: Id::new(ty.id),
            name: type_name.to_owned(),
            properties: BTreeMap::new(),
        };
        for property in properties {
            artifact
                .properties
                .insert(property.name, PropertyType::from_i32(property.data_type)?);
        }
        Ok(artifact)
    }

    pub async fn get_artifact_types(&mut self) -> Result<Vec<ArtifactType>, GetError> {
        #[derive(Debug, sqlx::FromRow)]
        struct Type {
            id: i32,
            name: String,
        }

        #[derive(Debug, sqlx::FromRow)]
        struct Property {
            type_id: i32,
            name: String,
            data_type: i32,
        }

        let mut types = BTreeMap::new();
        let mut rows =
            sqlx::query_as::<_, Type>(self.query.get_artifact_types()).fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            types.insert(
                row.id,
                ArtifactType {
                    id: Id::new(row.id),
                    name: row.name,
                    properties: BTreeMap::new(),
                },
            );
        }
        std::mem::drop(rows);

        let mut rows = sqlx::query_as::<_, Property>(self.query.get_type_properties())
            .fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            if let Some(ty) = types.get_mut(&row.type_id) {
                ty.properties
                    .insert(row.name, PropertyType::from_i32(row.data_type)?);
            }
        }

        Ok(types.into_iter().map(|(_, v)| v).collect())
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

        let options = PutArtifactTypeOptions::default();
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

        let options = PutArtifactTypeOptions::default();
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
