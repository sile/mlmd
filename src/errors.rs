//! Errors.
#![allow(missing_docs)]
use crate::metadata::{Id, PropertyType, TypeId, TypeKind};

/// Possible errors during database initialization.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// Database error.
    #[error("database error")]
    Db(#[from] sqlx::Error),

    /// Unsupported database is specified.
    ///
    /// [ml-metadata] only supports SQLite or MySQL.
    ///
    /// [ml-metadata]: https://github.com/google/ml-metadata
    #[error("only SQLite or MySQL are supported by ml-metadata")]
    UnsupportedDatabase,

    /// Incompatible database schema is used in the ml-metadata database.
    ///
    /// Please upgrade or downgrade the database by following [the official doc][migration].
    ///
    /// [migration]: https://github.com/google/ml-metadata/blob/master/g3doc/get_started.md#upgrade-the-mlmd-library
    #[error("schema version {actual} is not supported (supported version is {expected})")]
    UnsupportedSchemaVersion {
        /// The schema version of the database.
        actual: i32,

        /// The schema version supported by this crate.
        expected: i32,
    },
}

/// Possible errors while getting items from database.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Database error.
    #[error("database error")]
    Db(#[from] sqlx::Error),
}

/// Possible errors while putting items into database.
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    /// Database error.
    #[error("database error")]
    Db(#[from] sqlx::Error),

    /// Artifact, execution or context has a type that doesn't exist.
    #[error("{item_id} has a type {type_id} that doesn't exist")]
    TypeNotFound { type_id: TypeId, item_id: Id },

    /// The same name type already exists and it has incompatible properties.
    #[error("{type_kind} type with the name {type_name} already exists")]
    TypeAlreadyExists {
        type_kind: TypeKind,
        type_name: String,
    },

    /// Artifact, execution or context has a property of which type doesn't define.
    #[error("{item_id} has an undefined property {property_name:?}({property_type})")]
    UndefinedProperty {
        item_id: Id,
        property_name: String,
        property_type: PropertyType,
    },

    /// A name which already exists is specified.
    #[error("{item_id} has a name {item_name:?} that already exists")]
    NameAlreadyExists { item_id: Id, item_name: String },

    /// The artifact, execution or context hasn't been created yet.
    #[error("{item_id} is not found")]
    NotFound { item_id: Id },
}

impl From<GetError> for PutError {
    fn from(e: GetError) -> Self {
        let GetError::Db(e) = e;
        Self::Db(e)
    }
}

/// Possible errors while creating new items.
#[derive(Debug, thiserror::Error)]
pub enum PostError {
    /// Database error.
    #[error("database error")]
    Db(#[from] sqlx::Error),

    /// Specified type hasn't been defined.
    #[error("{type_kind} type {type_id} is not found")]
    TypeNotFound {
        type_kind: TypeKind,
        type_id: TypeId,
    },

    /// Specified property isn't defined by the type.
    #[error("new {type_kind} with the type {type_id} has an undefined property {property_name:?}")]
    UndefinedProperty {
        type_kind: TypeKind,
        type_id: TypeId,
        property_name: String,
    },

    /// A name which already exists is specified.
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
