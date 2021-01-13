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
    println!("Connected");
    println!();

    for execution in store.get_executions().execute().await? {
        println!("{:?}", execution);
    }
    Ok(())
}
