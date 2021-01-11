// https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/util/metadata_source_query_config.cc
use crate::metadata::{self, ConvertError, Value};
use crate::metadata_store::options::{
    GetArtifactsOptions, GetContextsOptions, GetExecutionsOptions, PostArtifactOptions,
    PostExecutionOptions,
};

#[derive(Debug, Clone)]
pub enum Query {
    Sqlite(SqliteQuery),
    Mysql(MysqlQuery),
}

impl Query {
    pub fn sqlite() -> Self {
        Self::Sqlite(SqliteQuery)
    }

    pub fn mysql() -> Self {
        Self::Mysql(MysqlQuery)
    }

    pub fn create_tables(&self) -> &'static [&'static str] {
        match self {
            Self::Sqlite(x) => x.create_tables(),
            Self::Mysql(x) => x.create_tables(),
        }
    }

    pub fn insert_attribution(&self) -> &'static str {
        match self {
            Self::Sqlite(x) => x.insert_attribution(),
            Self::Mysql(x) => x.insert_attribution(),
        }
    }

    pub fn insert_association(&self) -> &'static str {
        match self {
            Self::Sqlite(x) => x.insert_association(),
            Self::Mysql(x) => x.insert_association(),
        }
    }

    pub fn select_schema_version(&self) -> &'static str {
        "SELECT schema_version FROM MLMDEnv"
    }

    pub fn insert_schema_version(&self) -> &'static str {
        "INSERT INTO MLMDEnv VALUES ($1)"
    }

    pub fn get_types(&self) -> &'static str {
        "SELECT id, name FROM Type WHERE type_kind=$1"
    }

    pub fn get_type_properties(&self) -> &'static str {
        "SELECT type_id, name, data_type FROM TypeProperty"
    }

    pub fn get_type_by_name(&self) -> &'static str {
        "SELECT id, name FROM Type WHERE type_kind=$1 AND name=$2"
    }

    pub fn get_type_properties_by_type_id(&self) -> &'static str {
        "SELECT type_id, name, data_type FROM TypeProperty WHERE type_id=$1"
    }

    pub fn insert_type(&self) -> &'static str {
        "INSERT INTO Type (type_kind, name) VALUES ($1, $2)"
    }

    pub fn insert_type_property(&self) -> &'static str {
        "INSERT INTO TypeProperty (type_id, name, data_type) VALUES ($1, $2, $3)"
    }

    pub fn check_artifact_name(&self) -> &'static str {
        "SELECT count(*) FROM Artifact WHERE type_id=? AND name=? AND id != ?"
    }

    pub fn insert_artifact(&self, options: &PostArtifactOptions) -> String {
        // If https://github.com/launchbadge/sqlx/issues/772 is resolved,
        // we can use a static INSERT statement without regarding `options`.
        let mut fields =
            "type_id, state, create_time_since_epoch, last_update_time_since_epoch".to_owned();
        let mut values = "?, ?, ?, ?".to_owned();

        if options.name.is_some() {
            fields += ", name";
            values += ", ?";
        }
        if options.uri.is_some() {
            fields += ", uri";
            values += ", ?";
        }

        format!("INSERT INTO Artifact ({}) VALUES ({})", fields, values)
    }

    pub fn update_artifact(&self, artifact: &metadata::Artifact) -> String {
        // If https://github.com/launchbadge/sqlx/issues/772 is resolved,
        // we can use a static UPDATE statement.
        let mut fields =
            "state=?, create_time_since_epoch=?, last_update_time_since_epoch=?".to_owned();
        if artifact.name.is_some() {
            fields += ", name=?";
        }
        if artifact.uri.is_some() {
            fields += ", uri=?";
        }

        format!("UPDATE Artifact SET {} WHERE id=?", fields)
    }

    pub fn upsert_artifact_property(&self, value: &Value) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_artifact_property(value),
            Self::Mysql(x) => x.upsert_artifact_property(value),
        }
    }

    pub fn get_last_artifact_id(&self) -> &'static str {
        "SELECT id FROM Artifact ORDER BY id DESC LIMIT 1"
    }

    pub fn get_artifact_properties(&self, n_ids: usize) -> String {
        format!(
            concat!(
                "SELECT artifact_id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ArtifactProperty ",
                "WHERE artifact_id IN ({})"
            ),
            (1..=n_ids)
                .map(|n| format!("${}", n))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub fn get_artifacts(&self, options: &GetArtifactsOptions) -> String {
        let mut query = concat!(
            "SELECT ",
            "A.id, A.type_id, A.name, A.uri, A.state, A.create_time_since_epoch, A.last_update_time_since_epoch ",
            "FROM Artifact as A ",
        ).to_owned();

        if options.type_name.is_some() {
            query += "JOIN Type as T ON A.type_id = T.id ";
        };
        if options.context_id.is_some() {
            query += "JOIN Attribution as C ON A.id = C.artifact_id ";
        }

        let mut conditions = Vec::new();
        if options.type_name.is_some() {
            conditions.push("T.name = ?".to_owned());
        }
        if options.artifact_name.is_some() {
            conditions.push("A.name = ?".to_owned());
        }
        if !options.artifact_ids.is_empty() {
            conditions.push(format!(
                "A.id IN ({})",
                (0..options.artifact_ids.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if options.uri.is_some() {
            conditions.push("A.uri = ?".to_owned());
        }
        if options.context_id.is_some() {
            conditions.push("C.context_id = ?".to_owned());
        }

        if !conditions.is_empty() {
            query += &format!("WHERE {}", conditions.join(" AND "));
        }

        query
    }

    pub fn get_executions(&self, options: &GetExecutionsOptions) -> String {
        let mut query = concat!(
            "SELECT ",
            "A.id, A.name, A.type_id, A.last_known_state, A.create_time_since_epoch, A.last_update_time_since_epoch ",
            "FROM Execution as A ",
        ).to_owned();

        if options.type_name.is_some() {
            query += "JOIN Type as T ON A.type_id = T.id ";
        };
        if options.context_id.is_some() {
            query += "JOIN Association as C ON A.id = C.execution_id ";
        }

        let mut conditions = Vec::new();
        if options.type_name.is_some() {
            conditions.push("T.name = ?".to_owned());
        }
        if options.execution_name.is_some() {
            conditions.push("A.name = ?".to_owned());
        }
        if !options.execution_ids.is_empty() {
            conditions.push(format!(
                "A.id IN ({})",
                (0..options.execution_ids.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if options.context_id.is_some() {
            conditions.push("C.context_id = ?".to_owned());
        }

        if !conditions.is_empty() {
            query += &format!("WHERE {}", conditions.join(" AND "));
        }

        query
    }

    pub fn get_execution_properties(&self, n_ids: usize) -> String {
        format!(
            concat!(
                "SELECT execution_id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ExecutionProperty ",
                "WHERE execution_id IN ({})"
            ),
            (1..=n_ids)
                .map(|n| format!("${}", n))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub fn insert_execution(&self, options: &PostExecutionOptions) -> String {
        // If https://github.com/launchbadge/sqlx/issues/772 is resolved,
        // we can use a static INSERT statement without regarding `options`.
        let mut fields =
            "type_id, last_known_state, create_time_since_epoch, last_update_time_since_epoch"
                .to_owned();
        let mut values = "?, ?, ?, ?".to_owned();

        if options.name.is_some() {
            fields += ", name";
            values += ", ?";
        }

        format!("INSERT INTO Execution ({}) VALUES ({})", fields, values)
    }

    pub fn update_execution(&self, execution: &metadata::Execution) -> String {
        // If https://github.com/launchbadge/sqlx/issues/772 is resolved,
        // we can use a static UPDATE statement.
        let mut fields =
            "last_known_state=?, create_time_since_epoch=?, last_update_time_since_epoch=?"
                .to_owned();
        if execution.name.is_some() {
            fields += ", name=?";
        }

        format!("UPDATE Execution SET {} WHERE id=?", fields)
    }

    pub fn upsert_execution_property(&self, value: &Value) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_execution_property(value),
            Self::Mysql(x) => x.upsert_execution_property(value),
        }
    }

    pub fn get_last_execution_id(&self) -> &'static str {
        "SELECT id FROM Execution ORDER BY id DESC LIMIT 1"
    }

    pub fn check_execution_name(&self) -> &'static str {
        "SELECT count(*) FROM Execution WHERE type_id=? AND name=? AND id != ?"
    }

    pub fn get_contexts(&self, options: &GetContextsOptions) -> String {
        let mut query = concat!(
            "SELECT ",
            "A.id, A.name, A.type_id, A.create_time_since_epoch, A.last_update_time_since_epoch ",
            "FROM Context as A ",
        )
        .to_owned();

        if options.type_name.is_some() {
            query += "JOIN Type as T ON A.type_id = T.id ";
        };
        if options.artifact_id.is_some() {
            query += "JOIN Attribution as B ON A.id = B.context_id ";
        }
        if options.execution_id.is_some() {
            query += "JOIN Association as C ON A.id = C.context_id ";
        }

        let mut conditions = Vec::new();
        if options.type_name.is_some() {
            conditions.push("T.name = ?".to_owned());
        }
        if options.context_name.is_some() {
            conditions.push("A.name = ?".to_owned());
        }
        if !options.context_ids.is_empty() {
            conditions.push(format!(
                "A.id IN ({})",
                (0..options.context_ids.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if options.artifact_id.is_some() {
            conditions.push("B.artifact_id = ?".to_owned());
        }
        if options.execution_id.is_some() {
            conditions.push("C.execution_id = ?".to_owned());
        }

        if !conditions.is_empty() {
            query += &format!("WHERE {}", conditions.join(" AND "));
        }

        query
    }

    pub fn get_context_properties(&self, n_ids: usize) -> String {
        format!(
            concat!(
                "SELECT context_id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ContextProperty ",
                "WHERE context_id IN ({})"
            ),
            (1..=n_ids)
                .map(|n| format!("${}", n))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub fn insert_context(&self) -> &'static str {
        concat!(
            "INSERT INTO Context ",
            "(type_id, create_time_since_epoch, last_update_time_since_epoch, name) ",
            "VALUES (?, ?, ?, ?)"
        )
    }

    pub fn update_context(&self) -> &'static str {
        concat!(
            "UPDATE Context SET ",
            "create_time_since_epoch=?, last_update_time_since_epoch=?, name=? ",
            "WHERE id=?"
        )
    }

    pub fn upsert_context_property(&self, value: &Value) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_context_property(value),
            Self::Mysql(x) => x.upsert_context_property(value),
        }
    }

    pub fn get_last_context_id(&self) -> &'static str {
        "SELECT id FROM Context ORDER BY id DESC LIMIT 1"
    }

    pub fn check_context_name(&self) -> &'static str {
        "SELECT count(*) FROM Context WHERE type_id=? AND name=? AND id != ?"
    }
}

#[derive(Debug, Clone)]
pub struct SqliteQuery;

impl SqliteQuery {
    fn create_tables(&self) -> &'static [&'static str] {
        &[
            concat!(
                " CREATE TABLE IF NOT EXISTS `Type` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `version` VARCHAR(255), ",
                "   `type_kind` TINYINT(1) NOT NULL, ",
                "   `description` TEXT, ",
                "   `input_type` TEXT, ",
                "   `output_type` TEXT",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ParentType` ( ",
                "   `type_id` INT NOT NULL, ",
                "   `parent_type_id` INT NOT NULL, ",
                " PRIMARY KEY (`type_id`, `parent_type_id`));"
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `TypeProperty` ( ",
                "   `type_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `data_type` INT NULL, ",
                " PRIMARY KEY (`type_id`, `name`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Artifact` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `uri` TEXT, ",
                "   `state` INT, ",
                "   `name` VARCHAR(255), ",
                "   `create_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ArtifactProperty` ( ",
                "   `artifact_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`artifact_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Execution` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `last_known_state` INT, ",
                "   `name` VARCHAR(255), ",
                "   `create_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ExecutionProperty` ( ",
                "   `execution_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`execution_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Context` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `create_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` INT NOT NULL DEFAULT 0, ",
                "   UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ContextProperty` ( ",
                "   `context_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`context_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ParentContext` ( ",
                "   `context_id` INT NOT NULL, ",
                "   `parent_context_id` INT NOT NULL, ",
                " PRIMARY KEY (`context_id`, `parent_context_id`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Event` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `artifact_id` INT NOT NULL, ",
                "   `execution_id` INT NOT NULL, ",
                "   `type` INT NOT NULL, ",
                "   `milliseconds_since_epoch` INT ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `EventPath` ( ",
                "   `event_id` INT NOT NULL, ",
                "   `is_index_step` TINYINT(1) NOT NULL, ",
                "   `step_index` INT, ",
                "   `step_key` TEXT ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Association` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `context_id` INT NOT NULL, ",
                "   `execution_id` INT NOT NULL, ",
                "   UNIQUE(`context_id`, `execution_id`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Attribution` ( ",
                "   `id` INTEGER PRIMARY KEY AUTOINCREMENT, ",
                "   `context_id` INT NOT NULL, ",
                "   `artifact_id` INT NOT NULL, ",
                "   UNIQUE(`context_id`, `artifact_id`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `MLMDEnv` ( ",
                "   `schema_version` INTEGER PRIMARY KEY ",
                " ); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_artifact_uri` ",
                " ON `Artifact`(`uri`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_artifact_create_time_since_epoch` ",
                " ON `Artifact`(`create_time_since_epoch`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_artifact_last_update_time_since_epoch` ",
                " ON `Artifact`(`last_update_time_since_epoch`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_event_artifact_id` ",
                " ON `Event`(`artifact_id`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_event_execution_id` ",
                " ON `Event`(`execution_id`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_parentcontext_parent_context_id` ",
                " ON `ParentContext`(`parent_context_id`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_type_name` ",
                " ON `Type`(`name`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_execution_create_time_since_epoch` ",
                " ON `Execution`(`create_time_since_epoch`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_execution_last_update_time_since_epoch` ",
                " ON `Execution`(`last_update_time_since_epoch`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_context_create_time_since_epoch` ",
                " ON `Context`(`create_time_since_epoch`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS ",
                "   `idx_context_last_update_time_since_epoch` ",
                " ON `Context`(`last_update_time_since_epoch`); "
            ),
        ]
    }

    fn insert_attribution(&self) -> &'static str {
        "INSERT OR IGNORE INTO Attribution (context_id, artifact_id) VALUES (?, ?)"
    }

    fn insert_association(&self) -> &'static str {
        "INSERT OR IGNORE INTO Association (context_id, execution_id) VALUES (?, ?)"
    }

    fn upsert_artifact_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ArtifactProperty ",
                "(artifact_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON CONFLICT (artifact_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }

    fn upsert_execution_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ExecutionProperty ",
                "(execution_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON CONFLICT (execution_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }

    fn upsert_context_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ContextProperty ",
                "(context_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON CONFLICT (context_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }
}

fn maybe_null(b: bool, s: &str) -> &str {
    if b {
        s
    } else {
        "NULL"
    }
}

#[derive(Debug, Clone)]
pub struct MysqlQuery;

impl MysqlQuery {
    fn create_tables(&self) -> &'static [&'static str] {
        &[
            concat!(
                " CREATE TABLE IF NOT EXISTS `Type` ( ",
                "   `id` INT PRIMARY KEY AUTO_INCREMENT, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `version` VARCHAR(255), ",
                "   `type_kind` TINYINT(1) NOT NULL, ",
                "   `description` TEXT, ",
                "   `input_type` TEXT, ",
                "   `output_type` TEXT",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ParentType` ( ",
                "   `type_id` INT NOT NULL, ",
                "   `parent_type_id` INT NOT NULL, ",
                " PRIMARY KEY (`type_id`, `parent_type_id`));"
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `TypeProperty` ( ",
                "   `type_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `data_type` INT NULL, ",
                " PRIMARY KEY (`type_id`, `name`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Artifact` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `uri` TEXT, ",
                "   `state` INT, ",
                "   `name` VARCHAR(255), ",
                "   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   CONSTRAINT UniqueArtifactTypeName UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ArtifactProperty` ( ",
                "   `artifact_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`artifact_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Execution` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `last_known_state` INT, ",
                "   `name` VARCHAR(255), ",
                "   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   CONSTRAINT UniqueExecutionTypeName UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ExecutionProperty` ( ",
                "   `execution_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`execution_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Context` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `type_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0, ",
                "   UNIQUE(`type_id`, `name`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ContextProperty` ( ",
                "   `context_id` INT NOT NULL, ",
                "   `name` VARCHAR(255) NOT NULL, ",
                "   `is_custom_property` TINYINT(1) NOT NULL, ",
                "   `int_value` INT, ",
                "   `double_value` DOUBLE, ",
                "   `string_value` TEXT, ",
                " PRIMARY KEY (`context_id`, `name`, `is_custom_property`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `ParentContext` ( ",
                "   `context_id` INT NOT NULL, ",
                "   `parent_context_id` INT NOT NULL, ",
                " PRIMARY KEY (`context_id`, `parent_context_id`)); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Event` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `artifact_id` INT NOT NULL, ",
                "   `execution_id` INT NOT NULL, ",
                "   `type` INT NOT NULL, ",
                "   `milliseconds_since_epoch` BIGINT ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `EventPath` ( ",
                "   `event_id` INT NOT NULL, ",
                "   `is_index_step` TINYINT(1) NOT NULL, ",
                "   `step_index` INT, ",
                "   `step_key` TEXT ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Association` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `context_id` INT NOT NULL, ",
                "   `execution_id` INT NOT NULL, ",
                "   UNIQUE(`context_id`, `execution_id`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `Attribution` ( ",
                "   `id` INTEGER PRIMARY KEY AUTO_INCREMENT, ",
                "   `context_id` INT NOT NULL, ",
                "   `artifact_id` INT NOT NULL, ",
                "   UNIQUE(`context_id`, `artifact_id`) ",
                " ); "
            ),
            concat!(
                " CREATE TABLE IF NOT EXISTS `MLMDEnv` ( ",
                "   `schema_version` INTEGER PRIMARY KEY ",
                " ); "
            ),
            concat!(
                " ALTER TABLE `Artifact` ",
                "  ADD INDEX `idx_artifact_uri`(`uri`(255)), ",
                "  ADD INDEX `idx_artifact_create_time_since_epoch` ",
                "             (`create_time_since_epoch`), ",
                "  ADD INDEX `idx_artifact_last_update_time_since_epoch` ",
                "             (`last_update_time_since_epoch`); "
            ),
            concat!(
                " ALTER TABLE `Event` ",
                " ADD INDEX `idx_event_artifact_id` (`artifact_id`), ",
                " ADD INDEX `idx_event_execution_id` (`execution_id`); "
            ),
            concat!(
                " ALTER TABLE `ParentContext` ",
                " ADD INDEX ",
                "   `idx_parentcontext_parent_context_id` (`parent_context_id`); "
            ),
            concat!(
                " ALTER TABLE `Type` ",
                " ADD INDEX `idx_type_name` (`name`); "
            ),
            concat!(
                " ALTER TABLE `Execution` ",
                "  ADD INDEX `idx_execution_create_time_since_epoch` ",
                "             (`create_time_since_epoch`), ",
                "  ADD INDEX `idx_execution_last_update_time_since_epoch` ",
                "             (`last_update_time_since_epoch`); "
            ),
            concat!(
                " ALTER TABLE `Context` ",
                "  ADD INDEX `idx_context_create_time_since_epoch` ",
                "             (`create_time_since_epoch`), ",
                "  ADD INDEX `idx_context_last_update_time_since_epoch` ",
                "             (`last_update_time_since_epoch`); "
            ),
        ]
    }

    fn insert_attribution(&self) -> &'static str {
        "INSERT IGNORE INTO Attribution (context_id, artifact_id) VALUES (?, ?)"
    }

    fn insert_association(&self) -> &'static str {
        "INSERT IGNORE INTO Association (context_id, execution_id) VALUES (?, ?)"
    }

    fn upsert_artifact_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ArtifactProperty ",
                "(artifact_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }

    fn upsert_execution_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ExecutionProperty ",
                "(execution_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }

    fn upsert_context_property(&self, value: &Value) -> String {
        format!(
            concat!(
                "INSERT INTO ContextProperty ",
                "(context_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES ($1, $2, $3, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "$4"),
            maybe_null(value.as_double().is_some(), "$4"),
            maybe_null(value.as_string().is_some(), "$4")
        )
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Type {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct TypeProperty {
    pub type_id: i32,
    pub name: String,
    pub data_type: i32,
}

#[derive(Debug, Clone, Copy)]
pub enum TypeKind {
    Execution = 0,
    Artifact = 1,
    Context = 2,
}

impl TypeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Artifact => "artifact",
            Self::Context => "context",
        }
    }
}

#[derive(Debug)]
pub enum QueryValue<'a> {
    Int(i32),
    Str(&'a str),
}

#[derive(Debug, sqlx::FromRow)]
pub struct Artifact {
    pub id: i32,
    pub type_id: i32,
    pub name: Option<String>,
    pub uri: Option<String>,
    pub state: i32,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ArtifactProperty {
    pub artifact_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

impl ArtifactProperty {
    pub fn into_name_and_vaue(self) -> Result<(String, Value), ConvertError> {
        match self {
            Self {
                name,
                int_value: Some(v),
                double_value: None,
                string_value: None,
                ..
            } => Ok((name, Value::Int(v))),
            Self {
                name,
                int_value: None,
                double_value: Some(v),
                string_value: None,
                ..
            } => Ok((name, Value::Double(v))),
            Self {
                name,
                int_value: None,
                double_value: None,
                string_value: Some(v),
                ..
            } => Ok((name, Value::String(v))),
            _ => Err(ConvertError::WrongPropertyValue),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Execution {
    pub id: i32,
    pub type_id: i32,
    pub name: Option<String>,
    pub last_known_state: i32,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ExecutionProperty {
    pub execution_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

impl ExecutionProperty {
    pub fn into_name_and_vaue(self) -> Result<(String, Value), ConvertError> {
        match self {
            Self {
                name,
                int_value: Some(v),
                double_value: None,
                string_value: None,
                ..
            } => Ok((name, Value::Int(v))),
            Self {
                name,
                int_value: None,
                double_value: Some(v),
                string_value: None,
                ..
            } => Ok((name, Value::Double(v))),
            Self {
                name,
                int_value: None,
                double_value: None,
                string_value: Some(v),
                ..
            } => Ok((name, Value::String(v))),
            _ => Err(ConvertError::WrongPropertyValue),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Context {
    pub id: i32,
    pub type_id: i32,
    pub name: String,
    pub create_time_since_epoch: i64,
    pub last_update_time_since_epoch: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ContextProperty {
    pub context_id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

impl ContextProperty {
    pub fn into_name_and_vaue(self) -> Result<(String, Value), ConvertError> {
        match self {
            Self {
                name,
                int_value: Some(v),
                double_value: None,
                string_value: None,
                ..
            } => Ok((name, Value::Int(v))),
            Self {
                name,
                int_value: None,
                double_value: Some(v),
                string_value: None,
                ..
            } => Ok((name, Value::Double(v))),
            Self {
                name,
                int_value: None,
                double_value: None,
                string_value: Some(v),
                ..
            } => Ok((name, Value::String(v))),
            _ => Err(ConvertError::WrongPropertyValue),
        }
    }
}
