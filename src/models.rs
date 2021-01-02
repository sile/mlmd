use crate::schema::{Artifact, ArtifactProperty};
use diesel::Queryable;

#[derive(Debug, Clone, Queryable, Insertable, AsChangeset)]
#[table_name = "Artifact"]
pub struct ArtifactModel {
    pub id: Option<i32>,
    pub type_id: i32,
    pub uri: Option<String>,
    pub state: Option<i32>,
    pub name: Option<String>,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, Clone, Queryable, Insertable)]
#[table_name = "ArtifactProperty"]
pub struct ArtifactPropertyModel {
    pub artifact_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

#[derive(Debug, Clone, Queryable)]
pub struct AssociationModel {
    pub id: Option<i32>,
    pub context_id: i32,
    pub execution_id: i32,
}

#[derive(Debug, Clone, Queryable)]
pub struct AttributionModel {
    pub id: Option<i32>,
    pub context_id: i32,
    pub artifact_id: i32,
}

#[derive(Debug, Clone, Queryable)]
pub struct ContextModel {
    pub id: Option<i32>,
    pub type_id: i32,
    pub name: String,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, Clone, Queryable)]
pub struct ContextPropertyModel {
    pub context_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

#[derive(Debug, Clone, Queryable)]
pub struct EventModel {
    pub id: Option<i32>,
    pub artifact_id: i32,
    pub execution_id: i32,
    pub event_type: i32,
    pub milliseconds_since_epoch: Option<i64>,
}

#[derive(Debug, Clone, Queryable)]
pub struct ExecutionModel {
    pub id: Option<i32>,
    pub type_id: i32,
    pub last_known_state: Option<i32>,
    pub name: Option<String>,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, Clone, Queryable)]
pub struct ExecutionPropertyModel {
    pub context_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

#[derive(Debug, Clone, Queryable)]
pub struct MlmdEnvModel {
    pub schema_version: Option<i32>,
}

#[derive(Debug, Clone, Queryable)]
pub struct ParentContextModel {
    pub context_id: i32,
    pub parent_context_id: i32,
}

#[derive(Debug, Clone, Queryable)]
pub struct ParentTypeModel {
    pub type_id: i32,
    pub parent_type_id: i32,
}

#[derive(Debug, Clone, Queryable)]
pub struct TypeModel {
    pub id: Option<i32>,
    pub name: String,
    pub version: Option<String>,
    pub type_kind: bool,
    pub description: Option<String>,
    pub input_type: Option<String>,
    pub output_type: Option<String>,
}

#[derive(Debug, Clone, Queryable)]
pub struct TypePropertyModel {
    pub type_id: i32,
    pub name: String,
    pub data_type: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use diesel::{Connection as _, RunQueryDsl as _};

    #[test]
    fn load_artifact_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::Artifact::dsl::Artifact.load::<ArtifactModel>(&connection)?;
        assert_eq!(results.len(), 2);
        Ok(())
    }

    #[test]
    fn load_artifact_property_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::ArtifactProperty::dsl::ArtifactProperty
            .load::<ArtifactPropertyModel>(&connection)?;
        assert_eq!(results.len(), 4);
        Ok(())
    }

    #[test]
    fn load_association_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results =
            schema::Association::dsl::Association.load::<AssociationModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_attribution_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results =
            schema::Attribution::dsl::Attribution.load::<AttributionModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_context_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::Context::dsl::Context.load::<ContextModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_context_property_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::ContextProperty::dsl::ContextProperty
            .load::<ContextPropertyModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_event_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::Event::dsl::Event.load::<EventModel>(&connection)?;
        assert_eq!(results.len(), 2);
        Ok(())
    }

    #[test]
    fn load_execution_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::Execution::dsl::Execution.load::<ExecutionModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_execution_property_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::ExecutionProperty::dsl::ExecutionProperty
            .load::<ExecutionPropertyModel>(&connection)?;
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn load_mlmd_env_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::MLMDEnv::dsl::MLMDEnv.load::<MlmdEnvModel>(&connection)?;
        assert_eq!(results.len(), 1);
        Ok(())
    }

    #[test]
    fn load_parent_context_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results =
            schema::ParentContext::dsl::ParentContext.load::<ParentContextModel>(&connection)?;
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn load_parent_type_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::ParentType::dsl::ParentType.load::<ParentTypeModel>(&connection)?;
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn load_type_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results = schema::Type::dsl::Type.load::<TypeModel>(&connection)?;
        assert_eq!(results.len(), 4);
        Ok(())
    }

    #[test]
    fn load_type_property_model() -> anyhow::Result<()> {
        let connection = diesel::sqlite::SqliteConnection::establish("tests/test.db")?;
        let results =
            schema::TypeProperty::dsl::TypeProperty.load::<TypePropertyModel>(&connection)?;
        assert_eq!(results.len(), 5);
        Ok(())
    }
}
