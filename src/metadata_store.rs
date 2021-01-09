use crate::metadata::{ArtifactType, ConvertError, Id, PropertyType};
use crate::query::Query;
use sqlx::{AnyConnection, Connection as _, Executor as _, Row as _};
use std::collections::BTreeMap;

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

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("only SQLite or MySQL are supported by ml-metadata")]
    UnsupportedDatabase,

    #[error("schema version {actual} is not supported (expected version is {expected})")]
    UnsupportedSchemaVersion { actual: i32, expected: i32 },

    #[error("there are {count} MLMDEnv records (only one record is expected)")]
    TooManyMlmdEnvRecords { count: usize },
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
        let this = self;

        #[derive(Debug, sqlx::FromRow)]
        struct Type {
            id: i32,
        }

        #[derive(Debug, sqlx::FromRow)]
        struct Property {
            name: String,
            data_type: i32,
        }

        let ty = sqlx::query_as::<_, Type>(this.query.get_artifact_type())
            .bind(type_name)
            .fetch_optional(&mut this.connection)
            .await?;
        let ty = if let Some(ty) = ty {
            let properties =
                sqlx::query_as::<_, Property>(this.query.get_artifact_type_properties())
                    .bind(ty.id)
                    .fetch_all(&mut this.connection)
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
            sqlx::query(this.query.insert_artifact_type())
                .bind(type_name)
                .execute(&mut this.connection)
                .await?;

            let ty = sqlx::query_as::<_, Type>(this.query.get_artifact_type())
                .bind(type_name)
                .fetch_one(&mut this.connection)
                .await?;
            ty
        };
        for (name, value) in &options.properties {
            sqlx::query(this.query.insert_artifact_type_property())
                .bind(ty.id)
                .bind(name)
                .bind(*value as i32)
                .execute(&mut this.connection)
                .await?;
        }

        Ok(Id::new(ty.id))
    }

    pub async fn get_artifact_type(&mut self, type_name: &str) -> Result<ArtifactType, GetError> {
        todo!()
    }

    pub fn get_artifact_types(&mut self) -> Result<Vec<ArtifactType>, GetError> {
        // let ids_csv = type_ids
        //     .map(|x| x.to_string())
        //     .collect::<Vec<_>>()
        //     .join(",");

        // let query = format!("SELECT * FROM Type WHERE id IN ({})", ids_csv);
        // let mut rows = sqlx::query(&query).fetch(&mut self.connection);
        // let mut types = Vec::new();
        // while let Some(row) = rows.try_next().await? {
        //     let ty = TypeRecord::from_row(row)?;
        //     types.push(ty);
        // }
        // std::mem::drop(rows);

        // let query = format!("SELECT * FROM TypeProperty WHERE type_id IN ({})", ids_csv);
        // let mut rows = sqlx::query(&query).fetch(&mut self.connection);
        // let mut properties: HashMap<_, BTreeMap<_, _>> = HashMap::new();
        // while let Some(row) = rows.try_next().await? {
        //     let property = TypePropertyRecord::from_row(row)?;
        //     properties
        //         .entry(property.type_id)
        //         .or_default()
        //         .insert(property.name, property.data_type);
        // }
        // std::mem::drop(rows);

        // Ok(types
        //     .into_iter()
        //     .map(move |ty| {
        //         let id = ty.id;
        //         (id, f(ty, properties.remove(&id).unwrap_or_default()))
        //     })
        //     .collect())
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("{target} is not found")]
    NotFound { target: String },
}

#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("conversion error")]
    Convert(#[from] ConvertError),

    #[error("{kind} type with the name {name} already exists")]
    TypeAlreadyExists { kind: String, name: String },
}

#[derive(Debug, Default, Clone)]
pub struct PutArtifactTypeOptions {
    can_add_fields: bool,
    can_omit_fields: bool,
    properties: BTreeMap<String, PropertyType>,
}

impl PutArtifactTypeOptions {
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
