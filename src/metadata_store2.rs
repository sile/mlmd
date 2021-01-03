use crate::metadata::{
    Artifact, ArtifactId, ArtifactState, ArtifactTypeId, ConvertError, NewArtifact,
};
use futures::TryStreamExt;
use sqlx::any::AnyRow;
use sqlx::Row;
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
}

impl MetadataStore {
    pub fn new(connection: sqlx::AnyConnection) -> Self {
        Self { connection }
    }

    // TODO: option
    pub async fn get_artifacts(&mut self) -> Result<Vec<Artifact>, MetadataStoreError> {
        let mut rows = sqlx::query("SELECT * FROM Artifact").fetch(&mut self.connection);

        let mut artifacts = Vec::new();
        while let Some(row) = rows.try_next().await? {
            artifacts.push(ArtifactRecord::from_row(row)?);
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
