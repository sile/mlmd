use crate::metadata::{ArtifactType, PropertyType};
use crate::query::Query;
use sqlx::any::AnyRow;
use sqlx::{AnyConnection, Connection as _, Row as _};
use std::collections::BTreeMap;

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

    pub fn put_artifact_type(
        &mut self,
        type_name: &str,
        options: PutArtifactTypeOptions,
    ) -> Result<ArtifactType, PutError> {
        todo!()
    }

    pub fn get_artifact_type(&mut self, type_name: &str) -> Result<ArtifactType, GetError> {
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
    #[error("{target} is not found")]
    NotFound { target: String },
}

#[derive(Debug, thiserror::Error)]
pub enum PutError {}

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
}

#[derive(Debug, Clone)]
pub struct TypeRecord {
    pub id: i32,
    pub name: String,
    pub version: Option<String>,
    pub type_kind: bool,
    pub description: Option<String>,
    pub input_type: Option<String>,
    pub output_type: Option<String>,
}

impl TypeRecord {
    // pub fn from_row(row: AnyRow) -> Result<Self,  MetadataStoreError> {
    //     Ok(Self {
    //         id: row.try_get("id")?,
    //         name: row.try_get("name")?,
    //         version: row.try_get("version")?,
    //         type_kind: row.try_get("type_kind")?,
    //         description: row.try_get("description")?,
    //         input_type: row.try_get("input_type")?,
    //         output_type: row.try_get("output_type")?,
    //     })
    // }
}

#[derive(Debug, Clone)]
pub struct TypePropertyRecord {
    pub type_id: i32,
    pub name: String,
    pub data_type: PropertyType,
}

impl TypePropertyRecord {
    // pub fn from_row(row: AnyRow) -> Result<Self, MetadataStoreError> {
    //     Ok(Self {
    //         type_id: row.try_get("type_id")?,
    //         name: row.try_get("name")?,
    //         data_type: PropertyType::from_i32(row.try_get("data_type")?)?,
    //     })
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[async_std::test]
    async fn initialization_works() -> anyhow::Result<()> {
        // Open an existing database.
        MetadataStore::new("sqlite://tests/test.db").await?;

        // Create a new database.
        let file = NamedTempFile::new()?;
        let path = format!(
            "sqlite://{}",
            file.path()
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid path"))?
        );
        MetadataStore::new(&path).await?;

        Ok(())
    }
}
