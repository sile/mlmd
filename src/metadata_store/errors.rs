use crate::metadata::ConvertError;

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("only SQLite or MySQL are supported by ml-metadata")]
    UnsupportedDatabase,

    #[error("schema version {actual} is not supported (expected version is {expected})")]
    UnsupportedSchemaVersion { actual: i32, expected: i32 },

    #[error("there are {count} MLMDEnv records (only one record is expected)")]
    TooManyMlmdEnvRecords { count: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum GetError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("conversion error")]
    Convert(#[from] ConvertError),

    #[error("{target} is not found")]
    NotFound { target: String },
}

#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("conversion error")]
    Convert(#[from] ConvertError),

    #[error("{kind} type with the name {name} already exists")]
    TypeAlreadyExists { kind: &'static str, name: String },

    #[error("type not found")]
    TypeNotFound,

    #[error("undefined property")]
    UndefinedProperty,

    #[error("name already exists")]
    NameConflict,

    #[error("not found")]
    NotFound,

    #[error("wrong type id")]
    WrongTypeId,

    #[error(transparent)]
    Get(#[from] GetError),
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("conversion error")]
    Convert(#[from] ConvertError),

    #[error("type not found")]
    TypeNotFound,

    #[error("undefined property")]
    UndefinedProperty,

    #[error("name already exists")]
    NameConflict,

    #[error(transparent)]
    Get(#[from] GetError),
}
