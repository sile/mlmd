//! This example script is based on the Python code shown in [the official doc].
//!
//!# [the official doc]: https://github.com/google/ml-metadata/blob/master/g3doc/get_started.md
use mlmd::metadata::{EventType, ExecutionState, PropertyType};
use mlmd::MetadataStore;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
    database_uri: String,
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let opt = Opt::from_args();
    let mut store = MetadataStore::new(&opt.database_uri).await?;

    println!("Create ArtifactTypes, e.g., Data and Model");
    let data_type_id = store
        .put_artifact_type("DataSet")
        .property("day", PropertyType::Int)
        .property("split", PropertyType::String)
        .execute()
        .await?;
    let model_type_id = store
        .put_artifact_type("SavedModel")
        .property("version", PropertyType::Int)
        .property("name", PropertyType::String)
        .execute()
        .await?;

    println!("Create an ExecutionType, e.g., Trainer");
    let trainer_type_id = store.put_execution_type("Trainer").execute().await?;

    println!("Create an input artifact of type DataSet");
    let data_artifact_id = store
        .post_artifact(data_type_id)
        .uri("path/to/data")
        .property("day", 1i32)
        .property("split", "train")
        .execute()
        .await?;

    println!("Register the Execution of a Trainer run");
    let trainer_run_id = store
        .post_execution(trainer_type_id)
        .last_known_state(ExecutionState::Running)
        .execute()
        .await?;

    println!("Define the input event and record it in the metadata store");
    store
        .put_event(trainer_run_id, data_artifact_id)
        .ty(EventType::DeclaredInput)
        .execute()
        .await?;

    println!("Declare the output artifact of type SavedModel");
    let model_artifact_id = store
        .post_artifact(model_type_id)
        .uri("path/to/model")
        .property("version", 1i32)
        .property("name", "MNIST-v1")
        .execute()
        .await?;

    println!("Declare the output event and submit it to the Metadata Store");
    store
        .put_event(trainer_run_id, model_artifact_id)
        .ty(EventType::DeclaredOutput)
        .execute()
        .await?;

    println!("Mark the execution as completed");
    store
        .put_execution(trainer_run_id)
        .last_known_state(ExecutionState::Complete)
        .execute()
        .await?;

    println!("Create a ContextType, e.g., Experiment with a note property");
    let experiment_type_id = store
        .put_context_type("Experiment")
        .property("note", PropertyType::String)
        .execute()
        .await?;

    println!("Group the model and the trainer run to an experiment");
    let experiment_id = store
        .post_context(experiment_type_id, &format!("exp.{}", std::process::id()))
        .property("note", "My first experiment")
        .execute()
        .await?;
    store
        .put_attribution(experiment_id, model_artifact_id)
        .execute()
        .await?;
    store
        .put_association(experiment_id, trainer_run_id)
        .execute()
        .await?;

    Ok(())
}
