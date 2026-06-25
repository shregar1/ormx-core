use thiserror::Error;

#[derive(Debug, Error)]
pub enum MigrateError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("duplicate migration version {version:?}: {first} and {second}")]
    DuplicateVersion {
        version: String,
        first: String,
        second: String,
    },

    #[error("invalid SQL identifier: {0:?}")]
    InvalidIdentifier(String),

    #[error("row length {row_len} does not match columns length {col_len}")]
    RowColumnMismatch { row_len: usize, col_len: usize },

    #[error("unsupported SQL literal type")]
    UnsupportedLiteral,

    #[error("migration SQL check failed ({context}): {message}")]
    SqlCheck { context: String, message: String },
}
