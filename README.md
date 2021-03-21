mlmd
====

[![mlmd](https://img.shields.io/crates/v/mlmd.svg)](https://crates.io/crates/mlmd)
[![Documentation](https://docs.rs/mlmd/badge.svg)](https://docs.rs/mlmd)
[![Actions Status](https://github.com/sile/mlmd/workflows/CI/badge.svg)](https://github.com/sile/mlmd/actions)
[![Coverage Status](https://coveralls.io/repos/github/sile/mlmd/badge.svg?branch=main)](https://coveralls.io/github/sile/mlmd?branch=main)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Rust implementation of [ml-metadata].

This crate supports the schema version 6 used in [ml-metadata-v0.25.0][v0.25.0] or later.

[ml-metadata]: https://github.com/google/ml-metadata
[v0.25.0]: https://github.com/google/ml-metadata/releases/tag/v0.25.0

Examples
--------

```rust
use mlmd::MetadataStore;
use mlmd::metadata::EventType;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Creates metadata store.
    let db_file = NamedTempFile::new()?;
    let sqlite_uri = format!("sqlite://{}", db_file.path().to_str().unwrap());
    let mut store = MetadataStore::connect(&sqlite_uri).await?;

    // Creates an artifact.
    let artifact_type_id = store.put_artifact_type("DataSet").execute().await?;
    let artifact_id = store.post_artifact(artifact_type_id).uri("/foo/bar").execute().await?;

    // Creates an execution.
    let execution_type_id = store.put_execution_type("Training").execute().await?;
    let execution_id = store.post_execution(execution_type_id).execute().await?;

    // Links the above execution with the artifact.
    store.put_event(execution_id, artifact_id).ty(EventType::Input).execute().await?;

    // Gets executions.
    let executions = store.get_executions().execute().await?;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0].id, execution_id);

    Ok(())
}
```

Limitations
-----------

The following features are not supported yet:
- gRPC client
- `input_type` and `output_type` fields of `Execution`

The following features are not planned to be supported:
- gRPC server
- Database schema migration

`ml-metadata` References
------------------------

- [GitHub][ml-metadata]
- [Guide](https://www.tensorflow.org/tfx/guide/mlmd)
- [API Docs](https://www.tensorflow.org/tfx/ml_metadata/api_docs/python/mlmd)
