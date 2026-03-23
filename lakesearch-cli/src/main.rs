use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};

use lakesearch_core::metadata::{ColumnStatus, IndexedColumn, Metadata, Snapshot};
use lakesearch_core::runtime::LakeRuntime;
use lakesearch_core::tokenizer::DEFAULT_TOKENIZER;

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
    /// Initialize a table (local or remote)
    CreateTable {
        /// Table location URL (e.g., file:///tmp/events/ or s3://bucket/tables/events/)
        #[arg(long)]
        location: String,
        /// Table name
        #[arg(long)]
        table_name: String,
        /// Column(s) to index
        #[arg(long, required = true, num_args = 1..)]
        column: Vec<String>,
    },
    /// Index Parquet files into a table
    Index {
        /// Table location URL
        #[arg(long)]
        location: String,
        /// Parquet file path(s) or URL(s) to index
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        /// Column to index
        #[arg(long)]
        column: String,
    },
    /// Query a table
    Query {
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
        /// Scoring mode: none, indexed, all
        #[arg(long, value_enum, default_value_t = ScoreModeArg::None)]
        score: ScoreModeArg,
        /// Maximum number of results
        #[arg(long)]
        limit: Option<usize>,
        /// Additional columns to include in output
        #[arg(long)]
        select: Vec<String>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum OperatorArg {
    And,
    Or,
}

impl From<OperatorArg> for lakesearch_query::Operator {
    fn from(arg: OperatorArg) -> Self {
        match arg {
            OperatorArg::And => Self::And,
            OperatorArg::Or => Self::Or,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum ScoreModeArg {
    None,
    Indexed,
    All,
}

impl From<ScoreModeArg> for lakesearch_query::ScoreMode {
    fn from(arg: ScoreModeArg) -> Self {
        match arg {
            ScoreModeArg::None => Self::None,
            ScoreModeArg::Indexed => Self::Indexed,
            ScoreModeArg::All => Self::All,
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
        Command::CreateTable {
            location,
            table_name,
            column,
        } => {
            let (store, base) = lakesearch_query::storage::parse_location(&location)?;

            if lakesearch_query::storage::current_exists(store.as_ref(), &base).await? {
                bail!("table already exists at {location}");
            }

            let table_id = uuid::Uuid::new_v4().to_string();
            let indexed_columns: Vec<IndexedColumn> = column
                .iter()
                .map(|name| IndexedColumn {
                    name: name.clone(),
                    tokenizer: DEFAULT_TOKENIZER.to_owned(),
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
                lakesearch_query::storage::write_metadata(store.as_ref(), &base, &metadata).await?;

            let pointer = lakesearch_core::metadata::CurrentPointer {
                metadata_path: meta_path,
                updated_at: chrono::Utc::now().to_rfc3339(),
            };
            lakesearch_query::storage::write_json(
                store.as_ref(),
                &base.child("metadata").child("current.json"),
                &pointer,
            )
            .await?;

            println!("Created table '{table_name}' at {location}");
        }
        Command::Index {
            location,
            file,
            column,
        } => {
            let (store, base) = lakesearch_query::storage::parse_location(&location)?;
            let runtime = LakeRuntime::default();
            lakesearch_cli::index::run_index(&store, &base, &file, &column, &runtime).await?;
            println!("Indexing complete.");
        }
        Command::Query {
            location,
            column,
            match_text,
            operator,
            score,
            limit,
            select,
        } => {
            let (store, base) = lakesearch_query::storage::parse_location(&location)?;
            let cache =
                std::sync::Arc::new(lakesearch_query::object_cache::ObjectCache::new(store));
            let runtime = std::sync::Arc::new(LakeRuntime::default());
            let result = lakesearch_query::query::run_query(
                cache,
                base,
                column,
                &match_text,
                operator.into(),
                score.into(),
                limit,
                select,
                8, // default io_concurrency for CLI
                runtime,
            )
            .await?;
            let json = serde_json::to_string_pretty(&result)?;
            println!("{json}");
        }
    }
    Ok(())
}
