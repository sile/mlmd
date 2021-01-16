use crate::metadata::{Id, PropertyType, TypeId};

pub use crate::query::TypeKind; // TODO: move

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("only SQLite or MySQL are supported by ml-metadata")]
    UnsupportedDatabase,

    #[error("schema version {actual} is not supported (supported version is {expected})")]
    UnsupportedSchemaVersion { actual: i32, expected: i32 },

    #[error("there are {count} MLMDEnv records (only one record is expected)")]
    TooManyMlmdEnvRecords { count: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum GetError {
    #[error("database error")]
    Db(#[from] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("{type_kind} {item_id} has a type {type_id} that doesn't exist")]
    TypeNotFound {
        type_kind: TypeKind,
        type_id: TypeId,
        item_id: Id,
    },

    #[error("{type_kind} type with the name {type_name} already exists")]
    TypeAlreadyExists {
        type_kind: TypeKind,
        type_name: String,
    },

    #[error("{type_kind} {item_id} has an undefined property {property_name:?}({property_type})")]
    UndefinedProperty {
        type_kind: TypeKind,
        item_id: Id,
        property_name: String,
        property_type: PropertyType,
    },

    #[error("{type_kind} {item_id} has a name {item_name:?} that already exists")]
    NameAlreadyExists {
        type_kind: TypeKind,
        item_id: Id,
        item_name: String,
    },

    #[error("{type_kind} {item_id} is not found")]
    NotFound { type_kind: TypeKind, item_id: Id },
}

impl From<GetError> for PutError {
    fn from(e: GetError) -> Self {
        let GetError::Db(e) = e;
        Self::Db(e)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PostError {
    #[error("database error")]
    Db(#[from] sqlx::Error),

    #[error("{type_kind} type {type_id} is not found")]
    TypeNotFound {
        type_kind: TypeKind,
        type_id: TypeId,
    },

    #[error("new {type_kind} with the type {type_id} has an undefined property {property_name:?}")]
    UndefinedProperty {
        type_kind: TypeKind,
        type_id: TypeId,
        property_name: String,
    },

    #[error("new {type_kind} has a name {item_name:?} that already exists")]
    NameAlreadyExists {
        type_kind: TypeKind,
        item_name: String,
    },
}

impl From<GetError> for PostError {
    fn from(e: GetError) -> Self {
        let GetError::Db(e) = e;
        Self::Db(e)
    }
}
