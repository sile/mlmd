use crate::metadata::{
    Artifact, ArtifactId, ArtifactState, ArtifactType, ArtifactTypeId, ConvertError, NewArtifact,
};
use futures::TryStreamExt;
use sqlx::any::AnyRow;
use sqlx::Row;
use std::collections::{HashMap, HashSet};
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

        let q = format!(
            "SELECT * FROM Type WHERE id IN ({})",
            unknown_types
                .iter()
                .map(|x| x.get().to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut rows = sqlx::query(&q).fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            let ty = TypeRecord::from_row(row)?;
            dbg!(ty);
        }
        todo!()
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
