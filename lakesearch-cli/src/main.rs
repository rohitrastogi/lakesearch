use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};

use lakesearch_core::metadata::{ColumnStatus, IndexedColumn, Metadata, Snapshot};
use lakesearch_core::runtime::LakeRuntime;

#[derive(Parser)]
#[command(
    name = "lakesearch",
    about = "Full-text search index for Parquet files"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build a search index from local Parquet files
    Index {
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        #[arg(long)]
        column: String,
        #[arg(long)]
        output: String,
    },
    /// Query a local search index
    Query {
        #[arg(long)]
        segment: String,
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        #[arg(long)]
        column: String,
        #[arg(long = "match")]
        match_text: String,
        #[arg(long, value_enum, default_value_t = OperatorArg::Or)]
        operator: OperatorArg,
        #[arg(long)]
        score: bool,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Initialize a table in object storage
    CreateTable {
        /// Table location URL (e.g., s3://bucket/lakesearch/tables/events/)
        #[arg(long)]
        location: String,
        /// Table name
        #[arg(long)]
        table_name: String,
        /// Column(s) to index
        #[arg(long, required = true, num_args = 1..)]
        column: Vec<String>,
    },
    /// Index Parquet files into an object storage table
    RemoteIndex {
        /// Table location URL
        #[arg(long)]
        location: String,
        /// Parquet file URL(s) to index
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        /// Column to index
        #[arg(long)]
        column: String,
    },
    /// Query a table in object storage
    RemoteQuery {
        /// Table location URL
        #[arg(long)]
        location: String,
        /// Column to search
        #[arg(long)]
        column: String,
        /// Search query text
        #[arg(long = "match")]
        match_text: String,
        /// Boolean operator (and / or)
        #[arg(long, value_enum, default_value_t = OperatorArg::Or)]
        operator: OperatorArg,
        /// Compute BM25 relevance scores
        #[arg(long)]
        score: bool,
        /// Maximum number of results
        #[arg(long)]
        limit: Option<usize>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum OperatorArg {
    And,
    Or,
}

impl From<OperatorArg> for lakesearch_cli::Operator {
    fn from(arg: OperatorArg) -> Self {
        match arg {
            OperatorArg::And => Self::And,
            OperatorArg::Or => Self::Or,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Index {
            file,
            column,
            output,
        } => {
            lakesearch_cli::index::run_index(&file, &column, &output)?;
        }
        Command::Query {
            segment,
            file,
            column,
            match_text,
            operator,
            score,
            limit,
        } => {
            let result = lakesearch_cli::query::run_query(
                &segment,
                &file,
                &column,
                &match_text,
                operator.into(),
                score,
                limit,
            )?;
            let json = serde_json::to_string_pretty(&result)?;
            println!("{json}");
        }
        Command::CreateTable {
            location,
            table_name,
            column,
        } => {
            let (store, base) = lakesearch_cli::storage::parse_location(&location)?;

            if lakesearch_cli::storage::current_exists(store.as_ref(), &base).await? {
                bail!("table already exists at {location}");
            }

            let table_id = uuid::Uuid::new_v4().to_string();
            let indexed_columns: Vec<IndexedColumn> = column
                .iter()
                .map(|name| IndexedColumn {
                    name: name.clone(),
                    tokenizer: "whitespace_lowercase".to_owned(),
                    status: ColumnStatus::Active,
                })
                .collect();

            let metadata = Metadata {
                format_version: 1,
                table_id,
                table_name: table_name.clone(),
                location: location.clone(),
                indexed_columns,
                snapshot: Snapshot {
                    timestamp_ms: chrono::Utc::now().timestamp_millis() as u64,
                    manifest_lists: vec![],
                },
            };

            let meta_path =
                lakesearch_cli::storage::write_metadata(store.as_ref(), &base, &metadata).await?;

            let pointer = lakesearch_core::metadata::CurrentPointer {
                metadata_path: meta_path,
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            lakesearch_cli::storage::write_json(
                store.as_ref(),
                &base.child("metadata").child("current.json"),
                &pointer,
            )
            .await?;

            println!("Created table '{table_name}' at {location}");
        }
        Command::RemoteIndex {
            location,
            file,
            column,
        } => {
            let (store, base) = lakesearch_cli::storage::parse_location(&location)?;
            let runtime = LakeRuntime::default();
            lakesearch_cli::remote_index::run_remote_index(&store, &base, &file, &column, &runtime)
                .await?;
            println!("Indexing complete.");
        }
        Command::RemoteQuery {
            location,
            column,
            match_text,
            operator,
            score,
            limit,
        } => {
            let (store, base) = lakesearch_cli::storage::parse_location(&location)?;
            let runtime = LakeRuntime::default();
            let result = lakesearch_cli::remote_query::run_remote_query(
                &store,
                &base,
                &column,
                &match_text,
                operator.into(),
                score,
                limit,
                &runtime,
            )
            .await?;
            let json = serde_json::to_string_pretty(&result)?;
            println!("{json}");
        }
    }
    Ok(())
}
