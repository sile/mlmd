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
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].name, "Trainer");
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
    assert_eq!(types.len(), 2);
    assert_eq!(types[0].name, "DataSet");
    assert_eq!(types[1].name, "SavedModel");
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
