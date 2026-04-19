#[derive(Debug, thiserror::Error)]
pub enum VtfError {
    #[error("validation: {0}")]
    Validation(String),

    #[error("type error: expected {expected}, got {got} at column '{column}' row {row}")]
    TypeError {
        column: String,
        row: usize,
        expected: String,
        got: String,
    },

    #[error("primary key violation: duplicate value '{value}' in column '{column}'")]
    PrimaryKeyViolation { column: String, value: String },

    #[error("unique constraint violation: duplicate value '{value}' in column '{column}'")]
    UniqueViolation { column: String, value: String },

    #[error("not null constraint: column '{column}' does not allow null values")]
    NotNullViolation { column: String },

    #[error("insert error: {0}")]
    Insert(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("storage: {0}")]
    Storage(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("schema: {0}")]
    Schema(String),

    /// Optimistic concurrency conflict: the table was modified by another
    /// writer between the time this transaction started and commit time.
    /// The caller should re-read the data and retry the transaction.
    #[error("OCC conflict: table LSN {current_lsn} != transaction read LSN {read_lsn}; retry the transaction")]
    OccConflict { read_lsn: u64, current_lsn: u64 },
}

pub type VtfResult<T> = Result<T, VtfError>;

impl VtfError {
    pub fn validation(msg: impl Into<String>) -> Self {
        VtfError::Validation(msg.into())
    }

    pub fn type_error(
        column: impl Into<String>,
        row: usize,
        expected: impl Into<String>,
        got: impl Into<String>,
    ) -> Self {
        VtfError::TypeError {
            column: column.into(),
            row,
            expected: expected.into(),
            got: got.into(),
        }
    }

    pub fn insert(msg: impl Into<String>) -> Self {
        VtfError::Insert(msg.into())
    }

    pub fn query(msg: impl Into<String>) -> Self {
        VtfError::Query(msg.into())
    }

    pub fn schema(msg: impl Into<String>) -> Self {
        VtfError::Schema(msg.into())
    }
}
