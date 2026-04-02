use std::path::PathBuf;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "vtf", version, about = "VTF — Vector Table Format database engine")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Create a new empty VTF table
    Create {
        /// Output file path
        file: PathBuf,
        /// Column definitions: "name:type,name:type,..."
        #[arg(long)]
        columns: String,
        /// Primary key column name
        #[arg(long)]
        primary_key: Option<String>,
        /// Comma-separated columns that must be unique: "email,username"
        #[arg(long)]
        unique: Option<String>,
        /// Comma-separated columns that reject null values: "name,email"
        #[arg(long)]
        not_null: Option<String>,
        /// Default values as a JSON object: '{"status":"active","score":0}'
        #[arg(long)]
        default: Option<String>,
    },
    /// Validate a VTF file
    Validate {
        /// File to validate
        file: PathBuf,
    },
    /// Insert row(s) into a VTF table
    Insert {
        /// VTF file path
        file: PathBuf,
        /// Single row as JSON object: '{"col": value, ...}'
        #[arg(long, conflicts_with = "rows")]
        row: Option<String>,
        /// Multiple rows as JSON array: '[{"col": val}, ...]'
        #[arg(long, conflicts_with = "row")]
        rows: Option<String>,
    },
    /// Delete rows matching a filter
    Delete {
        /// VTF file path
        file: PathBuf,
        /// Equality filter: "column=value"
        #[arg(long, name = "where", required = true)]
        filter: String,
    },
    /// Update rows matching a filter
    Update {
        /// VTF file path
        file: PathBuf,
        /// Equality filter: "column=value"
        #[arg(long, name = "where", required = true)]
        filter: String,
        /// Values to set as JSON object: '{"col": newval, ...}'
        #[arg(long, required = true)]
        set: String,
    },
    /// Query rows from a VTF table
    Query {
        /// VTF file path
        file: PathBuf,
        /// Equality filter: "column=value"
        #[arg(long, name = "where")]
        filter: Option<String>,
        /// Columns to select (comma-separated)
        #[arg(long)]
        select: Option<String>,
        /// Maximum number of rows to return
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show table info (schema, row count, indexes)
    Info {
        /// VTF file path
        file: PathBuf,
    },
    /// Export table in JSON, binary, or compressed format
    Export {
        /// VTF file path
        file: PathBuf,
        /// Pretty-print the JSON output
        #[arg(long)]
        pretty: bool,
        /// Output format: json (default), binary, compressed
        #[arg(long, default_value = "json")]
        format: String,
        /// Output file path (required for binary/compressed, optional for json)
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Run aggregation functions on a column
    Aggregate {
        /// VTF file path
        file: PathBuf,
        /// Column to aggregate
        #[arg(long)]
        column: String,
        /// Functions to run (comma-separated): count, sum, avg, min, max
        #[arg(long)]
        function: String,
        /// Optional filter expression
        #[arg(long, name = "where")]
        filter: Option<String>,
    },
    /// Add a column to an existing table
    AddColumn {
        /// VTF file path
        file: PathBuf,
        /// Column name
        #[arg(long)]
        name: String,
        /// Column type (int, float, string, boolean, date, array<int>, array<float>, array<string>)
        #[arg(long, name = "type")]
        col_type: String,
    },
    /// Vector similarity search on an array<float> column
    Search {
        /// VTF file path
        file: PathBuf,
        /// Column containing embeddings (must be array<float>)
        #[arg(long)]
        column: String,
        /// Query vector as JSON array: "[0.12, -0.98, 0.44]"
        #[arg(long)]
        vector: String,
        /// Number of top results to return
        #[arg(long, default_value = "5")]
        top_k: usize,
        /// Similarity metric: "cosine" or "euclidean"
        #[arg(long, default_value = "cosine")]
        metric: String,
        /// Columns to display in results (comma-separated)
        #[arg(long)]
        select: Option<String>,
    },
    /// Show the query execution plan without running the query
    Explain {
        /// VTF file path
        file: PathBuf,
        /// Filter expression to explain
        #[arg(long, name = "where", required = true)]
        filter: String,
    },
    /// Execute multiple operations as a single atomic transaction
    Txn {
        /// VTF file path
        file: PathBuf,
        /// Operations as a JSON array: '[{"op":"insert","row":{...}},{"op":"delete","where":"id=5"}]'
        #[arg(long)]
        ops: String,
    },
    /// Drop an index from a column
    DropIndex {
        /// VTF file path
        file: PathBuf,
        /// Column to drop the index from
        #[arg(long)]
        column: String,
    },
    /// Create an index on a column
    CreateIndex {
        /// VTF file path
        file: PathBuf,
        /// Column to index
        #[arg(long)]
        column: String,
        /// Index type (hash or sorted)
        #[arg(long, name = "type")]
        index_type: String,
    },
}
