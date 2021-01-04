use crate::metadata::{
    Artifact, ArtifactId, ArtifactState, ArtifactType, ArtifactTypeId, ConvertError, NewArtifact,
    PropertyType,
};
use futures::TryStreamExt;
use sqlx::any::AnyRow;
use sqlx::Row;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("conversion error")]
    Convert(#[from] ConvertError),
}

#[derive(Debug)]
pub struct MetadataStore {
    connection: sqlx::AnyConnection,
    artifact_types: HashMap<ArtifactTypeId, ArtifactType>,
}

impl MetadataStore {
    pub fn new(connection: sqlx::AnyConnection) -> Self {
        Self {
            connection,
            artifact_types: HashMap::new(),
        }
    }

    // TODO: option
    pub async fn get_artifacts(&mut self) -> Result<Vec<Artifact>, MetadataStoreError> {
        let mut rows = sqlx::query("SELECT * FROM Artifact").fetch(&mut self.connection);

        let mut artifacts = Vec::new();
        let mut unknown_types = HashSet::new();
        while let Some(row) = rows.try_next().await? {
            let artifact = ArtifactRecord::from_row(row)?;
            if !self.artifact_types.contains_key(&artifact.type_id) {
                unknown_types.insert(artifact.type_id);
            }
            artifacts.push(artifact);
        }
        std::mem::drop(rows);

        // TODO: artifact properties

        let types = self
            .get_types(
                unknown_types.into_iter().map(|k| k.get()),
                |ty, properties| ArtifactType {
                    id: ArtifactTypeId::new(ty.id),
                    name: ty.name,
                    properties,
                },
            )
            .await?;

        todo!()
    }

    async fn get_types<F, T>(
        &mut self,
        type_ids: impl Iterator<Item = i32>,
        f: F,
    ) -> Result<HashMap<i32, T>, MetadataStoreError>
    where
        F: Fn(TypeRecord, BTreeMap<String, PropertyType>) -> T,
    {
        let ids_csv = type_ids
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let query = format!("SELECT * FROM Type WHERE id IN ({})", ids_csv);
        let mut rows = sqlx::query(&query).fetch(&mut self.connection);
        let mut types = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let ty = TypeRecord::from_row(row)?;
            types.push(ty);
        }
        std::mem::drop(rows);

        let query = format!("SELECT * FROM TypeProperty WHERE type_id IN ({})", ids_csv);
        let mut rows = sqlx::query(&query).fetch(&mut self.connection);
        let mut properties: HashMap<_, BTreeMap<_, _>> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let property = TypePropertyRecord::from_row(row)?;
            properties
                .entry(property.type_id)
                .or_default()
                .insert(property.name, property.data_type);
        }
        std::mem::drop(rows);

        Ok(types
            .into_iter()
            .map(move |ty| {
                let id = ty.id;
                (id, f(ty, properties.remove(&id).unwrap_or_default()))
            })
            .collect())
    }
    // pub fn post_artifact(&self, artifact: &NewArtifact) -> Result<ArtifactId, MetadataStoreError> {
    //     todo!()
    // }
}

#[derive(Debug, Clone)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub type_id: ArtifactTypeId,
    pub uri: Option<String>,
    pub state: ArtifactState,
    pub name: Option<String>,
    pub create_time_since_epoch: Duration,
    pub last_update_time_since_epoch: Duration,
}

impl ArtifactRecord {
    pub fn from_row(row: AnyRow) -> Result<Self, MetadataStoreError> {
        let create_time_since_epoch: i64 = row.try_get("create_time_since_epoch")?;
        let last_update_time_since_epoch: i64 = row.try_get("last_update_time_since_epoch")?;
        Ok(Self {
            id: ArtifactId::new(row.try_get("id")?),
            type_id: ArtifactTypeId::new(row.try_get("type_id")?),
            uri: row.try_get("uri")?,
            state: ArtifactState::from_i32(row.try_get("state")?)?,
            name: row.try_get("name")?,
            create_time_since_epoch: Duration::from_millis(create_time_since_epoch as u64),
            last_update_time_since_epoch: Duration::from_millis(
                last_update_time_since_epoch as u64,
            ),
        })
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
    pub fn from_row(row: AnyRow) -> Result<Self, MetadataStoreError> {
        Ok(Self {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            version: row.try_get("version")?,
            type_kind: row.try_get("type_kind")?,
            description: row.try_get("description")?,
            input_type: row.try_get("input_type")?,
            output_type: row.try_get("output_type")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TypePropertyRecord {
    pub type_id: i32,
    pub name: String,
    pub data_type: PropertyType,
}

impl TypePropertyRecord {
    pub fn from_row(row: AnyRow) -> Result<Self, MetadataStoreError> {
        Ok(Self {
            type_id: row.try_get("id")?,
            name: row.try_get("name")?,
            data_type: PropertyType::from_i32(row.try_get("data_type")?)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[async_std::test]
    async fn get_artifacts_by_id_works() -> anyhow::Result<()> {
        let connection = sqlx::AnyConnection::connect("sqlite://tests/test.db").await?;
        let mut store = MetadataStore::new(connection);
        store.get_artifacts().await?;
        Ok(())
    }
}
