use super::*;
use crate::metadata::Value;
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
    assert_eq!(types.len(), 2);
    assert_eq!(types[0].name, "DataSet");
    assert_eq!(types[1].name, "SavedModel");
}

#[async_std::test]
async fn get_artifacts_works() {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

    let options = GetArtifactsOptions::default();

    // All.
    let artifacts = store.get_artifacts(options.clone()).await.unwrap();
    assert_eq!(artifacts, vec![artifact0(), artifact1()]);

    // By type name.
    let artifacts = store
        .get_artifacts(options.clone().ty("DataSet"))
        .await
        .unwrap();
    assert_eq!(artifacts, vec![artifact0()]);

    // By ID.
    let unregistered_id = Id::new(100);
    let artifacts = store
        .get_artifacts(options.clone().ids(&[Id::new(2), unregistered_id]))
        .await
        .unwrap();
    assert_eq!(artifacts[0].id, Id::new(2));
    assert_eq!(artifacts, vec![artifact1()]);

    // By URI.
    let artifacts = store
        .get_artifacts(options.clone().uri("path/to/model/file"))
        .await
        .unwrap();
    assert_eq!(artifacts, vec![artifact1()]);

    // By Context.
    let artifacts = store
        .get_artifacts(options.clone().context(Id::new(1)))
        .await
        .unwrap();
    assert_eq!(artifacts, vec![artifact1()]);
}

#[async_std::test]
async fn post_artifact_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    assert!(store.get_artifacts(default()).await?.is_empty());

    let type_id = store
        .put_artifact_type(
            "DataSet",
            PutTypeOptions::default()
                .property_int("day")
                .property_string("split"),
        )
        .await?;

    // Simple artifact.
    let artifact_id = store.post_artifact(type_id, default()).await?;

    let artifacts = store.get_artifacts(default()).await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].id, artifact_id);

    // Complex artifact.
    let mut expected = artifact0();
    expected.id = store
        .post_artifact(
            type_id,
            PostArtifactOptions::default()
                .uri(expected.uri.as_ref().unwrap())
                .properties(expected.properties.clone())
                .create_time_since_epoch(expected.create_time_since_epoch)
                .last_update_time_since_epoch(expected.last_update_time_since_epoch),
        )
        .await?;
    let artifacts = store.get_artifacts(default()).await?;
    assert_eq!(artifacts.len(), 2);
    assert_eq!(artifacts[1], expected);

    // Name confilict.
    store
        .post_artifact(type_id, PostArtifactOptions::default().name("foo"))
        .await?;
    assert!(matches!(
        store
            .post_artifact(type_id, PostArtifactOptions::default().name("foo"))
            .await
            .err(),
        Some(PostError::NameConflict)
    ));

    Ok(())
}

#[async_std::test]
async fn put_artifact_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    assert_eq!(store.get_artifacts(default()).await?.len(), 2);

    let mut artifact = artifact0();
    artifact.name = Some("foo".to_string());
    artifact.state = ArtifactState::Live;
    artifact
        .properties
        .insert("day".to_owned(), Value::Int(234));
    artifact
        .custom_properties
        .insert("bar".to_string(), Value::Int(10));
    store.put_artifact(&artifact).await?;

    assert_eq!(store.get_artifacts(default()).await?.len(), 2);
    assert_eq!(store.get_artifact(artifact.id).await?, Some(artifact));

    Ok(())
}

#[async_std::test]
async fn get_executions_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    let options = GetExecutionsOptions::default();

    // All.
    let executions = store.get_executions(options.clone()).await?;
    assert_eq!(executions, vec![execution0()]);

    // By type name.
    let executions = store
        .get_executions(options.clone().ty("Trainer"))
        .await
        .unwrap();
    assert_eq!(executions, vec![execution0()]);

    let executions = store
        .get_executions(options.clone().ty("foo"))
        .await
        .unwrap();
    assert_eq!(executions, vec![]);

    // By ID.
    let unregistered_id = Id::new(100);
    let executions = store
        .get_executions(options.clone().ids(&[Id::new(1), unregistered_id]))
        .await
        .unwrap();
    assert_eq!(executions, vec![execution0()]);

    // By Context.
    let executions = store
        .get_executions(options.clone().context(Id::new(1)))
        .await
        .unwrap();
    assert_eq!(executions, vec![execution0()]);

    Ok(())
}

#[async_std::test]
async fn put_execution_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    assert_eq!(store.get_executions(default()).await?.len(), 1);

    let mut execution = execution0();
    execution.name = Some("foo".to_string());
    execution.last_known_state = ExecutionState::Running;
    execution
        .custom_properties
        .insert("bar".to_string(), Value::Int(10));
    store.put_execution(&execution).await?;

    assert_eq!(store.get_executions(default()).await?.len(), 1);
    assert_eq!(store.get_execution(execution.id).await?, Some(execution));

    Ok(())
}

#[async_std::test]
async fn post_execution_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    assert!(store.get_executions(default()).await?.is_empty());

    let type_id = store
        .put_execution_type(
            "DataSet",
            PutTypeOptions::default()
                .property_int("day")
                .property_string("split"),
        )
        .await?;

    // Simple execution.
    let execution_id = store.post_execution(type_id, default()).await?;

    let executions = store.get_executions(default()).await?;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].id, execution_id);

    // Name confilict.
    store
        .post_execution(type_id, PostExecutionOptions::default().name("foo"))
        .await?;
    assert!(matches!(
        store
            .post_execution(type_id, PostExecutionOptions::default().name("foo"))
            .await
            .err(),
        Some(PostError::NameConflict)
    ));

    Ok(())
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
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].name, "Trainer");
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

fn artifact0() -> Artifact {
    Artifact {
        id: Id::new(1),
        type_id: Id::new(1),
        name: None,
        uri: Some("path/to/data".to_owned()),
        properties: vec![
            ("day".to_owned(), Value::Int(1)),
            ("split".to_owned(), Value::String("train".to_owned())),
        ]
        .into_iter()
        .collect(),
        custom_properties: BTreeMap::new(),
        state: ArtifactState::Unknown,
        create_time_since_epoch: Duration::from_millis(1609134222018),
        last_update_time_since_epoch: Duration::from_millis(1609134222239),
    }
}

fn artifact1() -> Artifact {
    Artifact {
        id: Id::new(2),
        type_id: Id::new(2),
        name: None,
        uri: Some("path/to/model/file".to_owned()),
        properties: vec![
            ("name".to_owned(), Value::String("MNIST-v1".to_owned())),
            ("version".to_owned(), Value::Int(1)),
        ]
        .into_iter()
        .collect(),
        custom_properties: BTreeMap::new(),
        state: ArtifactState::Unknown,
        create_time_since_epoch: Duration::from_millis(1609134223250),
        last_update_time_since_epoch: Duration::from_millis(1609134223518),
    }
}

fn default<T: Default>() -> T {
    T::default()
}

fn execution0() -> Execution {
    Execution {
        id: Id::new(1),
        type_id: Id::new(3),
        name: None,
        last_known_state: ExecutionState::Complete,
        properties: BTreeMap::new(),
        custom_properties: BTreeMap::new(),
        create_time_since_epoch: Duration::from_millis(1609134222505),
        last_update_time_since_epoch: Duration::from_millis(1609134224027),
    }
}
