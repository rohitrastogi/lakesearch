use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

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
    /// Build a search index from Parquet files
    Index {
        /// Parquet file(s) to index
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        /// Column to index (must be Utf8 or LargeUtf8)
        #[arg(long)]
        column: String,
        /// Output path for the segment file
        #[arg(long)]
        output: String,
    },
    /// Query a search index
    Query {
        /// Path to the segment file
        #[arg(long)]
        segment: String,
        /// Parquet file(s) to read (same files used during indexing, in order)
        #[arg(long, required = true, num_args = 1..)]
        file: Vec<String>,
        /// Column that was indexed
        #[arg(long)]
        column: String,
        /// Search query text
        #[arg(long = "match")]
        match_text: String,
        /// Boolean operator for combining terms (and / or)
        #[arg(long, value_enum, default_value_t = OperatorArg::Or)]
        operator: OperatorArg,
        /// Compute BM25 relevance scores
        #[arg(long)]
        score: bool,
        /// Maximum number of results to return
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

fn main() -> Result<()> {
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
    }
    Ok(())
}
