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
    },
    /// Show table info (schema, row count, indexes)
    Info {
        /// VTF file path
        file: PathBuf,
    },
    /// Export table as JSON
    Export {
        /// VTF file path
        file: PathBuf,
        /// Pretty-print the JSON output
        #[arg(long)]
        pretty: bool,
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
