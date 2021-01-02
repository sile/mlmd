use crate::metadata::{Artifact, ArtifactId, NewArtifact};
use crate::models::{ArtifactModel, ArtifactPropertyModel};
use crate::schema;
use diesel::connection::Connection as _;
use diesel::sqlite::SqliteConnection;
use diesel::{ExpressionMethods as _, QueryDsl as _, RunQueryDsl as _};
use std::time::{SystemTimeError, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    #[error("wrong system time")]
    Time(#[from] SystemTimeError),

    #[error("database error")]
    Db(#[from] diesel::result::Error),
}

pub enum Connection {
    Sqlite(SqliteConnection),
}

impl Connection {
    pub fn put_artifact<F>(
        &mut self,
        mut model: ArtifactModel,
        make_properties: F,
    ) -> Result<ArtifactModel, MetadataStoreError>
    where
        F: FnOnce(&ArtifactModel) -> Vec<ArtifactPropertyModel>,
    {
        let model = match self {
            Self::Sqlite(c) => c.transaction(|| {
                if model.id.is_none() {
                    diesel::insert_into(schema::Artifact::table)
                        .values(&model)
                        .execute(c)?;
                    model = schema::Artifact::dsl::Artifact
                        .order(schema::Artifact::dsl::id.desc())
                        .first(c)?;
                } else {
                    diesel::update(schema::Artifact::dsl::Artifact.find(schema::Artifact::dsl::id))
                        .set(&model)
                        .execute(c)?;
                }
                diesel::insert_into(schema::ArtifactProperty::table)
                    .values(make_properties(&model))
                    .execute(c)
                    .map(|_| model)
            }),
        }?;
        Ok(model)
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Sqlite(_) => write!(f, "Sqlite(_)"),
        }
    }
}

#[derive(Debug)]
pub struct MetadataStore {
    connection: Connection, // TODO: type cache
}

impl MetadataStore {
    pub fn post_artifact(
        &mut self,
        artifact: &NewArtifact,
    ) -> Result<ArtifactId, MetadataStoreError> {
        let model = ArtifactModel {
            id: None,
            type_id: artifact.ty.id.get(),
            uri: artifact.uri.clone(),
            state: Some(artifact.state as i32),
            name: artifact.name.clone(),
            create_time_since_epoch: UNIX_EPOCH.elapsed()?.as_millis() as i64,
            last_update_time_since_epoch: UNIX_EPOCH.elapsed()?.as_millis() as i64,
        };

        let model = self.connection.put_artifact(model, |model| {
            artifact
                .properties
                .iter()
                .map(|(k, v)| ArtifactPropertyModel {
                    artifact_id: model.id.expect("bug"),
                    name: k.clone(),
                    is_custom_property: artifact.ty.properties.contains_key(k),
                    int_value: v.as_int(),
                    double_value: v.as_double(),
                    string_value: v.as_string().cloned(),
                })
                .collect()
        })?;

        Ok(ArtifactId::new(model.id.expect("bug")))
    }

    pub fn put_artifact(&mut self, artifact: &Artifact) -> Result<ArtifactId, MetadataStoreError> {
        let model = ArtifactModel {
            id: Some(artifact.id.get()),
            type_id: artifact.ty.id.get(),
            uri: artifact.uri.clone(),
            state: Some(artifact.state as i32),
            name: artifact.name.clone(),
            create_time_since_epoch: artifact.create_time_since_epoch.as_millis() as i64,
            last_update_time_since_epoch: UNIX_EPOCH.elapsed()?.as_millis() as i64,
        };

        self.connection.put_artifact(model, |model| {
            artifact
                .properties
                .iter()
                .map(|(k, v)| ArtifactPropertyModel {
                    artifact_id: model.id.expect("bug"),
                    name: k.clone(),
                    is_custom_property: artifact.ty.properties.contains_key(k),
                    int_value: v.as_int(),
                    double_value: v.as_double(),
                    string_value: v.as_string().cloned(),
                })
                .collect()
        })?;

        Ok(artifact.id)
    }
}
