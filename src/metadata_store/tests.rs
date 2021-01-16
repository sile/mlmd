use super::*;
use crate::metadata::{
    Artifact, ArtifactState, ArtifactType, Context, ContextType, Execution, ExecutionState,
    ExecutionType, PropertyValue,
};
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
async fn put_artifact_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    store
        .put_artifact_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;

    assert!(matches!(
        store
            .put_artifact_type("t0")
            .property("p0", PropertyType::Double)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));

    assert!(matches!(
        store
            .put_artifact_type("t0")
            .property("p0", PropertyType::Int)
            .property("p1", PropertyType::String)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_artifact_type("t0")
        .can_add_fields()
        .property("p0", PropertyType::Int)
        .property("p1", PropertyType::String)
        .execute()
        .await?;

    assert!(matches!(
        store.put_artifact_type("t0").execute().await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_artifact_type("t0")
        .can_omit_fields()
        .execute()
        .await?;
    store.put_artifact_type("t1").execute().await?;

    Ok(())
}

#[async_std::test]
async fn get_artifact_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    let t0_id = store
        .put_artifact_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;
    let t1_id = store.put_artifact_type("t1").execute().await?;
    assert_ne!(t0_id, t1_id);

    assert_eq!(
        store.get_artifact_types().name("t0").execute().await?[0],
        ArtifactType {
            id: t0_id,
            name: "t0".to_owned(),
            properties: vec![("p0".to_owned(), PropertyType::Int)]
                .into_iter()
                .collect()
        }
    );
    assert_eq!(
        store.get_artifact_types().name("t1").execute().await?[0],
        ArtifactType {
            id: t1_id,
            name: "t1".to_owned(),
            properties: BTreeMap::new(),
        }
    );
    assert!(store
        .get_artifact_types()
        .name("t2")
        .execute()
        .await?
        .is_empty());

    Ok(())
}

#[async_std::test]
async fn get_artifact_types_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    let types = store.get_artifact_types().execute().await?;
    assert_eq!(types.len(), 2);
    assert_eq!(types[0].name, "DataSet");
    assert_eq!(types[1].name, "SavedModel");
    Ok(())
}

#[async_std::test]
async fn get_artifacts_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    // All.
    let artifacts = store.get_artifacts().execute().await?;
    assert_eq!(artifacts, vec![artifact0(), artifact1()]);

    // By type name.
    let artifacts = store.get_artifacts().ty("DataSet").execute().await?;
    assert_eq!(artifacts, vec![artifact0()]);

    // By ID.
    let unregistered_id = Id::new(100);
    let artifacts = store
        .get_artifacts()
        .ids(&[Id::new(2), unregistered_id])
        .execute()
        .await?;
    assert_eq!(artifacts[0].id, Id::new(2));
    assert_eq!(artifacts, vec![artifact1()]);

    // By URI.
    let artifacts = store
        .get_artifacts()
        .uri("path/to/model/file")
        .execute()
        .await?;
    assert_eq!(artifacts, vec![artifact1()]);

    // By Context.
    let artifacts = store.get_artifacts().context(Id::new(1)).execute().await?;
    assert_eq!(artifacts, vec![artifact1()]);

    Ok(())
}

#[async_std::test]
async fn post_artifact_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    assert!(store.get_artifacts().execute().await?.is_empty());

    let type_id = store
        .put_artifact_type("DataSet")
        .property("day", PropertyType::Int)
        .property("split", PropertyType::String)
        .execute()
        .await?;

    // Simple artifact.
    let artifact_id = store.post_artifact(type_id).execute().await?;

    let artifacts = store.get_artifacts().execute().await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].id, artifact_id);

    // Complex artifact.
    let mut expected = artifact0();
    expected.id = store
        .post_artifact(type_id)
        .uri(expected.uri.as_ref().unwrap())
        .properties(expected.properties.clone())
        .create_time_since_epoch(expected.create_time_since_epoch)
        .last_update_time_since_epoch(expected.last_update_time_since_epoch)
        .execute()
        .await?;
    let artifacts = store.get_artifacts().execute().await?;
    assert_eq!(artifacts.len(), 2);
    assert_eq!(artifacts[1], expected);

    // Name confilict.
    store.post_artifact(type_id).name("foo").execute().await?;
    assert!(matches!(
        store
            .post_artifact(type_id)
            .name("foo")
            .execute()
            .await
            .err(),
        Some(PostError::NameAlreadyExists { .. })
    ));

    Ok(())
}

#[async_std::test]
async fn put_artifact_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    assert_eq!(store.get_artifacts().execute().await?.len(), 2);

    let mut artifact = artifact0();
    artifact.name = Some("foo".to_string());
    artifact.state = ArtifactState::Live;
    artifact
        .properties
        .insert("day".to_owned(), PropertyValue::Int(234));
    artifact
        .custom_properties
        .insert("bar".to_string(), PropertyValue::Int(10));
    store
        .put_artifact(artifact.id)
        .name(artifact.name.as_ref().unwrap().as_str())
        .state(artifact.state)
        .properties(artifact.properties.clone())
        .custom_properties(artifact.custom_properties.clone())
        .execute()
        .await?;

    let artifacts = store.get_artifacts().id(artifact.id).execute().await?;
    assert_eq!(artifacts.len(), 1);

    artifact.last_update_time_since_epoch = artifacts[0].last_update_time_since_epoch;
    assert_eq!(artifacts[0], artifact);

    Ok(())
}

#[async_std::test]
async fn get_executions_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    // All.
    let executions = store.get_executions().execute().await?;
    assert_eq!(executions, vec![execution0()]);

    // By type name.
    let executions = store.get_executions().ty("Trainer").execute().await?;
    assert_eq!(executions, vec![execution0()]);

    let executions = store.get_executions().ty("foo").execute().await?;
    assert_eq!(executions, vec![]);

    // By ID.
    let unregistered_id = Id::new(100);
    let executions = store
        .get_executions()
        .ids(&[Id::new(1), unregistered_id])
        .execute()
        .await?;
    assert_eq!(executions, vec![execution0()]);

    // By Context.
    let executions = store.get_executions().context(Id::new(1)).execute().await?;
    assert_eq!(executions, vec![execution0()]);

    Ok(())
}

#[async_std::test]
async fn put_execution_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    assert_eq!(store.get_executions().execute().await?.len(), 1);

    let mut execution = execution0();
    execution.name = Some("foo".to_string());
    execution.last_known_state = ExecutionState::Running;
    execution
        .custom_properties
        .insert("bar".to_string(), PropertyValue::Int(10));
    store
        .put_execution(execution.id)
        .name(execution.name.as_ref().unwrap())
        .last_known_state(execution.last_known_state)
        .custom_properties(execution.custom_properties.clone())
        .execute()
        .await?;

    let executions = store.get_executions().execute().await?;
    assert_eq!(executions.len(), 1);

    execution.last_update_time_since_epoch = executions[0].last_update_time_since_epoch;
    assert_eq!(executions[0], execution);

    Ok(())
}

#[async_std::test]
async fn post_execution_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    assert!(store.get_executions().execute().await?.is_empty());

    let type_id = store
        .put_execution_type("DataSet")
        .property("day", PropertyType::Int)
        .property("split", PropertyType::String)
        .execute()
        .await?;

    // Simple execution.
    let execution_id = store.post_execution(type_id).execute().await?;

    let executions = store.get_executions().execute().await?;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].id, execution_id);

    // Name confilict.
    store.post_execution(type_id).name("foo").execute().await?;
    assert!(matches!(
        store
            .post_execution(type_id)
            .name("foo")
            .execute()
            .await
            .err(),
        Some(PostError::NameAlreadyExists { .. })
    ));

    Ok(())
}

#[async_std::test]
async fn put_execution_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    store
        .put_execution_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;

    assert!(matches!(
        store
            .put_execution_type("t0")
            .property("p0", PropertyType::Double)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));

    assert!(matches!(
        store
            .put_execution_type("t0")
            .property("p0", PropertyType::Int)
            .property("p1", PropertyType::String)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_execution_type("t0")
        .can_add_fields()
        .property("p0", PropertyType::Int)
        .property("p1", PropertyType::String)
        .execute()
        .await?;

    assert!(matches!(
        store.put_execution_type("t0").execute().await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_execution_type("t0")
        .can_omit_fields()
        .execute()
        .await?;
    store.put_execution_type("t1").execute().await?;

    Ok(())
}

#[async_std::test]
async fn get_execution_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    let t0_id = store
        .put_execution_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;
    let t1_id = store.put_execution_type("t1").execute().await?;
    assert_ne!(t0_id, t1_id);

    assert_eq!(
        store.get_execution_types().name("t0").execute().await?[0],
        ExecutionType {
            id: t0_id,
            name: "t0".to_owned(),
            properties: vec![("p0".to_owned(), PropertyType::Int)]
                .into_iter()
                .collect()
        }
    );
    assert_eq!(
        store.get_execution_types().name("t1").execute().await?[0],
        ExecutionType {
            id: t1_id,
            name: "t1".to_owned(),
            properties: BTreeMap::new(),
        }
    );
    assert!(store
        .get_execution_types()
        .name("t2")
        .execute()
        .await?
        .is_empty());

    Ok(())
}

#[async_std::test]
async fn get_execution_types_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    let types = store.get_execution_types().execute().await?;
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].name, "Trainer");
    Ok(())
}

#[async_std::test]
async fn get_contexts_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    // All.
    let contexts = store.get_contexts().execute().await?;
    assert_eq!(contexts, vec![context0()]);

    // By type name.
    let contexts = store.get_contexts().ty("Experiment").execute().await?;
    assert_eq!(contexts, vec![context0()]);

    let contexts = store.get_contexts().ty("foo").execute().await?;
    assert_eq!(contexts, vec![]);

    let contexts = store
        .get_contexts()
        .type_and_name("Experiment", "exp.27823")
        .execute()
        .await?;
    assert_eq!(contexts, vec![context0()]);

    // By ID.
    let unregistered_id = Id::new(100);
    let contexts = store
        .get_contexts()
        .ids(&[Id::new(1), unregistered_id])
        .execute()
        .await?;
    assert_eq!(contexts, vec![context0()]);

    // By artifact.
    let contexts = store.get_contexts().artifact(Id::new(2)).execute().await?;
    assert_eq!(contexts, vec![context0()]);

    // By execution.
    let contexts = store.get_contexts().execution(Id::new(1)).execute().await?;
    assert_eq!(contexts, vec![context0()]);

    Ok(())
}

#[async_std::test]
async fn put_context_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    assert_eq!(store.get_contexts().execute().await?.len(), 1);

    let mut context = context0();
    context.name = "foo".to_string();
    context
        .custom_properties
        .insert("bar".to_string(), PropertyValue::Int(10));
    store
        .put_context(context.id)
        .name(&context.name)
        .custom_properties(context.custom_properties.clone())
        .execute()
        .await?;

    let contexts = store.get_contexts().execute().await?;
    assert_eq!(contexts.len(), 1);

    context.last_update_time_since_epoch = contexts[0].last_update_time_since_epoch;
    assert_eq!(contexts[0], context);

    Ok(())
}

#[async_std::test]
async fn post_context_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    assert!(store.get_contexts().execute().await?.is_empty());

    let type_id = store.put_context_type("Context").execute().await?;

    // Simple context.
    let context_id = store.post_context(type_id, "bar").execute().await?;

    let contexts = store.get_contexts().execute().await?;
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].id, context_id);

    // Name confilict.
    store.post_context(type_id, "foo").execute().await?;
    assert!(matches!(
        store.post_context(type_id, "foo").execute().await.err(),
        Some(PostError::NameAlreadyExists { .. })
    ));

    Ok(())
}

#[async_std::test]
async fn put_context_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    store
        .put_context_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;

    assert!(matches!(
        store
            .put_context_type("t0")
            .property("p0", PropertyType::Double)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));

    assert!(matches!(
        store
            .put_context_type("t0")
            .property("p0", PropertyType::Int)
            .property("p1", PropertyType::String)
            .execute()
            .await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_context_type("t0")
        .can_add_fields()
        .property("p0", PropertyType::Int)
        .property("p1", PropertyType::String)
        .execute()
        .await?;

    assert!(matches!(
        store.put_context_type("t0").execute().await,
        Err(PutError::TypeAlreadyExists { .. })
    ));
    store
        .put_context_type("t0")
        .can_omit_fields()
        .execute()
        .await?;
    store.put_context_type("t1").execute().await?;

    Ok(())
}

#[async_std::test]
async fn get_context_type_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new()?;
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    let t0_id = store
        .put_context_type("t0")
        .property("p0", PropertyType::Int)
        .execute()
        .await?;
    let t1_id = store.put_context_type("t1").execute().await?;
    assert_ne!(t0_id, t1_id);

    assert_eq!(
        store.get_context_types().name("t0").execute().await?[0],
        ContextType {
            id: t0_id,
            name: "t0".to_owned(),
            properties: vec![("p0".to_owned(), PropertyType::Int)]
                .into_iter()
                .collect()
        }
    );
    assert_eq!(
        store.get_context_types().name("t1").execute().await?[0],
        ContextType {
            id: t1_id,
            name: "t1".to_owned(),
            properties: BTreeMap::new(),
        }
    );
    assert!(store
        .get_context_types()
        .name("t2")
        .execute()
        .await?
        .is_empty(),);

    Ok(())
}

#[async_std::test]
async fn get_context_types_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;
    let types = store.get_context_types().execute().await?;
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].name, "Experiment");
    Ok(())
}

#[async_std::test]
async fn put_attribution_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

    let t0 = store.put_artifact_type("t0").execute().await?;
    let a0 = store.post_artifact(t0).execute().await?;
    let _a1 = store.post_artifact(t0).execute().await?;

    let t1 = store.put_context_type("t1").execute().await?;
    let _c0 = store.post_context(t1, "foo").execute().await?;
    let c1 = store.post_context(t1, "bar").execute().await?;

    store.put_attribution(c1, a0).execute().await?;
    let contexts = store.get_contexts().artifact(a0).execute().await?;
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].id, c1);

    let artifacts = store.get_artifacts().context(c1).execute().await?;
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].id, a0);

    Ok(())
}

#[async_std::test]
async fn put_association_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

    let t0 = store.put_execution_type("t0").execute().await?;
    let e0 = store.post_execution(t0).execute().await?;
    let _e1 = store.post_execution(t0).execute().await?;

    let t1 = store.put_context_type("t1").execute().await?;
    let _c0 = store.post_context(t1, "foo").execute().await?;
    let c1 = store.post_context(t1, "bar").execute().await?;

    store.put_association(c1, e0).execute().await?;
    let contexts = store.get_contexts().execution(e0).execute().await?;
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].id, c1);

    let executions = store.get_executions().context(c1).execute().await?;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].id, e0);

    Ok(())
}

#[async_std::test]
async fn put_event_works() -> anyhow::Result<()> {
    let file = NamedTempFile::new().unwrap();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await.unwrap();

    let t0 = store.put_execution_type("t0").execute().await?;
    let e0 = store.post_execution(t0).execute().await?;
    let e1 = store.post_execution(t0).execute().await?;

    let t1 = store.put_artifact_type("t1").execute().await?;
    let a0 = store.post_artifact(t1).execute().await?;
    let a1 = store.post_artifact(t1).execute().await?;

    store
        .put_event(e0, a0)
        .ty(EventType::Input)
        .execute()
        .await?;
    store
        .put_event(e1, a1)
        .ty(EventType::Output)
        .step(EventStep::Index(30))
        .execute()
        .await?;

    let events = store.get_events().execute().await?;
    assert_eq!(events.len(), 2);

    assert_eq!(events[0].artifact_id, a0);
    assert_eq!(events[0].execution_id, e0);
    assert_eq!(events[0].ty, EventType::Input);
    assert_eq!(events[0].path, vec![]);

    assert_eq!(events[1].artifact_id, a1);
    assert_eq!(events[1].execution_id, e1);
    assert_eq!(events[1].ty, EventType::Output);
    assert_eq!(events[1].path, vec![EventStep::Index(30)]);

    Ok(())
}

#[async_std::test]
async fn get_events_works() -> anyhow::Result<()> {
    let file = existing_db();
    let mut store = MetadataStore::new(&sqlite_uri(file.path())).await?;

    let events = store.get_events().execute().await?;
    assert_eq!(events, vec![event0(), event1()]);

    let events = store.get_events().artifact(Id::new(1)).execute().await?;
    assert_eq!(events, vec![event0()]);

    let events = store.get_events().artifact(Id::new(2)).execute().await?;
    assert_eq!(events, vec![event1()]);

    let events = store.get_events().execution(Id::new(1)).execute().await?;
    assert_eq!(events, vec![event0(), event1()]);

    let events = store.get_events().execution(Id::new(2)).execute().await?;
    assert_eq!(events, vec![]);

    let events = store
        .get_events()
        .artifact(Id::new(1))
        .execution(Id::new(1))
        .execute()
        .await?;
    assert_eq!(events, vec![event0(), event1()]);

    Ok(())
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
            ("day".to_owned(), PropertyValue::Int(1)),
            (
                "split".to_owned(),
                PropertyValue::String("train".to_owned()),
            ),
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
            (
                "name".to_owned(),
                PropertyValue::String("MNIST-v1".to_owned()),
            ),
            ("version".to_owned(), PropertyValue::Int(1)),
        ]
        .into_iter()
        .collect(),
        custom_properties: BTreeMap::new(),
        state: ArtifactState::Unknown,
        create_time_since_epoch: Duration::from_millis(1609134223250),
        last_update_time_since_epoch: Duration::from_millis(1609134223518),
    }
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

fn context0() -> Context {
    Context {
        id: Id::new(1),
        type_id: Id::new(4),
        name: "exp.27823".to_owned(),
        properties: vec![(
            "note".to_owned(),
            PropertyValue::String("My first experiment.".to_owned()),
        )]
        .into_iter()
        .collect(),
        custom_properties: BTreeMap::new(),
        create_time_since_epoch: Duration::from_millis(1609134224698),
        last_update_time_since_epoch: Duration::from_millis(1609134224922),
    }
}

fn event0() -> Event {
    Event {
        artifact_id: Id::new(1),
        execution_id: Id::new(1),
        path: Vec::new(),
        ty: EventType::DeclaredInput,
        create_time_since_epoch: Duration::from_millis(1609134223004),
    }
}

fn event1() -> Event {
    Event {
        artifact_id: Id::new(2),
        execution_id: Id::new(1),
        path: Vec::new(),
        ty: EventType::DeclaredOutput,
        create_time_since_epoch: Duration::from_millis(1609134223788),
    }
}
