// https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/util/metadata_source_query_config.cc
use crate::metadata::{self, EventStep, Id, PropertyValue};
use crate::metadata_store::options::{
    self, GetArtifactsOptions, GetContextsOptions, GetEventsOptions, GetExecutionsOptions,
    GetTypesOptions, PostArtifactOptions, PostExecutionOptions,
};
use sqlx::database::HasArguments;
use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

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
        "INSERT INTO MLMDEnv VALUES (?)"
    }

    pub fn get_types(&self, options: &GetTypesOptions) -> String {
        let mut query = "SELECT id, name FROM Type WHERE type_kind=? ".to_owned();
        if options.name.is_some() {
            query += "AND name = ? ";
        }
        if !options.ids.is_empty() {
            query += &format!(
                "AND id IN ({})",
                options
                    .ids
                    .iter()
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        query
    }

    pub fn get_type_properties(&self) -> &'static str {
        "SELECT type_id, name, data_type FROM TypeProperty"
    }

    pub fn get_type_by_name(&self) -> &'static str {
        "SELECT id, name FROM Type WHERE type_kind=? AND name=?"
    }

    pub fn get_type_properties_by_type_id(&self) -> &'static str {
        "SELECT type_id, name, data_type FROM TypeProperty WHERE type_id=?"
    }

    pub fn insert_type(&self) -> &'static str {
        "INSERT INTO Type (type_kind, name) VALUES (?, ?)"
    }

    pub fn insert_type_property(&self) -> &'static str {
        "INSERT INTO TypeProperty (type_id, name, data_type) VALUES (?, ?, ?)"
    }

    pub fn check_context_id(&self) -> &'static str {
        "SELECT count(*) FROM Context WHERE id=?"
    }

    pub fn check_artifact_id(&self) -> &'static str {
        "SELECT count(*) FROM Artifact WHERE id=?"
    }

    pub fn check_execution_id(&self) -> &'static str {
        "SELECT count(*) FROM Execution WHERE id=?"
    }

    pub fn check_artifact_name(&self, is_post: bool) -> &'static str {
        if is_post {
            "SELECT count(*) FROM Artifact WHERE type_id=? AND name=?"
        } else {
            "SELECT count(*) FROM Artifact WHERE type_id=? AND name=? AND id != ?"
        }
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

    pub fn upsert_artifact_property(&self, value: &PropertyValue) -> String {
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
                "SELECT artifact_id as id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ArtifactProperty ",
                "WHERE artifact_id IN ({})"
            ),
            (0..n_ids)
                .map(|_| "?")
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
                "SELECT execution_id as id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ExecutionProperty ",
                "WHERE execution_id IN ({})"
            ),
            (0..n_ids)
                .map(|_| "?")
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

    pub fn upsert_execution_property(&self, value: &PropertyValue) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_execution_property(value),
            Self::Mysql(x) => x.upsert_execution_property(value),
        }
    }

    pub fn get_last_execution_id(&self) -> &'static str {
        "SELECT id FROM Execution ORDER BY id DESC LIMIT 1"
    }

    pub fn check_execution_name(&self, is_post: bool) -> &'static str {
        if is_post {
            "SELECT count(*) FROM Execution WHERE type_id=? AND name=?"
        } else {
            "SELECT count(*) FROM Execution WHERE type_id=? AND name=? AND id != ?"
        }
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
                "SELECT context_id as id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM ContextProperty ",
                "WHERE context_id IN ({})"
            ),
            (0..n_ids)
                .map(|_| "?")
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

    pub fn upsert_context_property(&self, value: &PropertyValue) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_context_property(value),
            Self::Mysql(x) => x.upsert_context_property(value),
        }
    }

    pub fn get_last_context_id(&self) -> &'static str {
        "SELECT id FROM Context ORDER BY id DESC LIMIT 1"
    }

    pub fn check_context_name(&self, is_post: bool) -> &'static str {
        if is_post {
            "SELECT count(*) FROM Context WHERE type_id=? AND name=?"
        } else {
            "SELECT count(*) FROM Context WHERE type_id=? AND name=? AND id != ?"
        }
    }

    pub fn insert_event(&self) -> &'static str {
        "INSERT INTO Event (artifact_id, execution_id, type, milliseconds_since_epoch) VALUES (?, ?, ?, ?)"
    }

    pub fn get_last_event_id(&self) -> &'static str {
        "SELECT id FROM Event ORDER BY id DESC LIMIT 1"
    }

    pub fn insert_event_path(&self, step: &EventStep) -> &'static str {
        match step {
            EventStep::Index(_) => {
                "INSERT INTO EventPath (event_id, is_index_step, step_index) VALUES (?, 1, ?)"
            }
            EventStep::Key(_) => {
                "INSERT INTO EventPath (event_id, is_index_step, step_key) VALUES (?, 0, ?)"
            }
        }
    }

    pub fn get_events(&self, options: &GetEventsOptions) -> String {
        let mut query =
            "SELECT Event.id, artifact_id, execution_id, Event.type, milliseconds_since_epoch FROM Event "
            .to_owned();
        if !options.artifact_ids.is_empty() {
            query += "JOIN Artifact ON Event.artifact_id = Artifact.id ";
        }
        if !options.execution_ids.is_empty() {
            query += "JOIN Execution ON Event.execution_id = Execution.id ";
        }

        let mut conditions = Vec::new();
        if !options.artifact_ids.is_empty() {
            conditions.push(format!(
                "Artifact.id IN ({}) ",
                options
                    .artifact_ids
                    .iter()
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if !options.execution_ids.is_empty() {
            conditions.push(format!(
                "Execution.id IN ({}) ",
                options
                    .execution_ids
                    .iter()
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if !conditions.is_empty() {
            query += &format!("WHERE {}", conditions.join(" OR "));
        }
        query
    }

    pub fn get_event_paths(&self, n_events: usize) -> String {
        format!("SELECT event_id, is_index_step, step_index, step_key FROM EventPath WHERE event_id IN ({})",
                (0..n_events).map(|_| "?").collect::<Vec<_>>().join(","))
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

    fn upsert_artifact_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ArtifactProperty ",
                "(artifact_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON CONFLICT (artifact_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
        )
    }

    fn upsert_execution_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ExecutionProperty ",
                "(execution_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON CONFLICT (execution_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
        )
    }

    fn upsert_context_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ContextProperty ",
                "(context_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON CONFLICT (context_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
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

    fn upsert_artifact_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ArtifactProperty ",
                "(artifact_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
        )
    }

    fn upsert_execution_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ExecutionProperty ",
                "(execution_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
        )
    }

    fn upsert_context_property(&self, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO ContextProperty ",
                "(context_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeKind {
    Execution = 0,
    Artifact = 1,
    Context = 2,
}

impl std::fmt::Display for TypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Execution => write!(f, "execution"),
            Self::Artifact => write!(f, "artifact"),
            Self::Context => write!(f, "context"),
        }
    }
}

#[derive(Debug)]
pub enum QueryValue<'a> {
    Int(i32),
    I64(i64),
    Str(&'a str),
}

impl<'a> QueryValue<'a> {
    pub fn bind<'q, DB>(
        self,
        query: sqlx::query::Query<'q, DB, <DB as HasArguments<'q>>::Arguments>,
    ) -> sqlx::query::Query<'q, DB, <DB as HasArguments<'q>>::Arguments>
    where
        'a: 'q,
        DB: sqlx::Database,
        i32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        i64: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        &'a str: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        match self {
            Self::Int(v) => query.bind(v),
            Self::I64(v) => query.bind(v),
            Self::Str(v) => query.bind(v),
        }
    }

    pub fn bind_scalar<'q, O, DB>(
        self,
        query: sqlx::query::QueryScalar<'q, DB, O, <DB as HasArguments<'q>>::Arguments>,
    ) -> sqlx::query::QueryScalar<'q, DB, O, <DB as HasArguments<'q>>::Arguments>
    where
        'a: 'q,
        DB: sqlx::Database,
        i32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        i64: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        &'a str: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        match self {
            Self::Int(v) => query.bind(v),
            Self::I64(v) => query.bind(v),
            Self::Str(v) => query.bind(v),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Property {
    pub id: i32,
    pub name: String,
    pub is_custom_property: bool,
    pub int_value: Option<i32>,
    pub double_value: Option<f64>,
    pub string_value: Option<String>,
}

impl Property {
    pub fn into_name_and_vaue(self) -> Result<(String, PropertyValue), sqlx::Error> {
        match self {
            Self {
                name,
                int_value: Some(v),
                double_value: None,
                string_value: None,
                ..
            } => Ok((name, PropertyValue::Int(v))),
            Self {
                name,
                int_value: None,
                double_value: Some(v),
                string_value: None,
                ..
            } => Ok((name, PropertyValue::Double(v))),
            Self {
                name,
                int_value: None,
                double_value: None,
                string_value: Some(v),
                ..
            } => Ok((name, PropertyValue::String(v))),
            _ => Err(sqlx::Error::Decode(
                anyhow::anyhow!("a property must have just one value: {:?}", self).into(),
            )),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Event {
    pub id: i32,
    #[sqlx(rename = "type")]
    pub ty: i32,
    pub artifact_id: i32,
    pub execution_id: i32,
    pub milliseconds_since_epoch: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct EventPath {
    pub event_id: i32,
    pub is_index_step: bool,
    pub step_index: Option<i32>,
    pub step_key: Option<String>,
}

// TODO: move
pub trait InsertProperty {
    fn insert_property(&mut self, is_custom: bool, name: String, value: PropertyValue);
}

pub trait GetItemsQueryGenerator {
    type Item: for<'a> sqlx::FromRow<'a, sqlx::any::AnyRow> + InsertProperty;

    fn generate_select_items_sql(&self) -> String;
    fn generate_select_properties_sql(&self, items: usize) -> String;
    fn query_values(&self) -> Vec<QueryValue>;
}

#[derive(Debug)]
pub struct GetArtifactsQueryGenerator {
    pub query: Query,
    pub options: GetArtifactsOptions,
}

impl GetItemsQueryGenerator for GetArtifactsQueryGenerator {
    type Item = metadata::Artifact;

    fn generate_select_items_sql(&self) -> String {
        self.query.get_artifacts(&self.options)
    }

    fn generate_select_properties_sql(&self, items: usize) -> String {
        self.query.get_artifact_properties(items)
    }

    fn query_values(&self) -> Vec<QueryValue> {
        self.options.values()
    }
}

#[derive(Debug)]
pub struct GetExecutionsQueryGenerator {
    pub query: Query,
    pub options: GetExecutionsOptions,
}

impl GetItemsQueryGenerator for GetExecutionsQueryGenerator {
    type Item = metadata::Execution;

    fn generate_select_items_sql(&self) -> String {
        self.query.get_executions(&self.options)
    }

    fn generate_select_properties_sql(&self, items: usize) -> String {
        self.query.get_execution_properties(items)
    }

    fn query_values(&self) -> Vec<QueryValue> {
        self.options.values()
    }
}

#[derive(Debug)]
pub struct GetContextsQueryGenerator {
    pub query: Query,
    pub options: GetContextsOptions,
}

impl GetItemsQueryGenerator for GetContextsQueryGenerator {
    type Item = metadata::Context;

    fn generate_select_items_sql(&self) -> String {
        self.query.get_contexts(&self.options)
    }

    fn generate_select_properties_sql(&self, items: usize) -> String {
        self.query.get_context_properties(items)
    }

    fn query_values(&self) -> Vec<QueryValue> {
        self.options.values()
    }
}

pub trait PostItemQueryGenerator {
    const TYPE_KIND: TypeKind;

    fn item_name(&self) -> Option<&str>;
    fn item_properties(&self) -> &BTreeMap<String, PropertyValue>;
    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue>;
    fn generate_check_item_name_query(&self) -> Option<(&'static str, Vec<QueryValue>)>;
    fn generate_insert_item_query(&self) -> (String, Vec<QueryValue>);
    fn generate_last_item_id(&self) -> &'static str;
    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String;
}

#[derive(Debug)]
pub struct PostArtifactQueryGenerator {
    pub query: Query,
    pub type_id: Id,
    pub options: options::PostArtifactOptions,
}

impl PostItemQueryGenerator for PostArtifactQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Artifact;

    fn item_name(&self) -> Option<&str> {
        self.options.name.as_ref().map(|n| n.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(&self) -> Option<(&'static str, Vec<QueryValue>)> {
        if let Some(name) = &self.options.name {
            let values = vec![QueryValue::Int(self.type_id.get()), QueryValue::Str(name)];
            Some((self.query.check_artifact_name(true), values))
        } else {
            None
        }
    }

    fn generate_insert_item_query(&self) -> (String, Vec<QueryValue>) {
        let sql = self.query.insert_artifact(&self.options);
        let mut values = vec![
            QueryValue::Int(self.type_id.get()),
            QueryValue::Int(self.options.state as i32),
            QueryValue::I64(self.options.create_time_since_epoch.as_millis() as i64),
            QueryValue::I64(self.options.last_update_time_since_epoch.as_millis() as i64),
        ];
        if let Some(v) = &self.options.name {
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = &self.options.uri {
            values.push(QueryValue::Str(v));
        }
        (sql, values)
    }

    fn generate_last_item_id(&self) -> &'static str {
        self.query.get_last_artifact_id()
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_artifact_property(value)
    }
}

#[derive(Debug)]
pub struct PostExecutionQueryGenerator {
    pub query: Query,
    pub type_id: Id,
    pub options: options::PostExecutionOptions,
}

impl PostItemQueryGenerator for PostExecutionQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Execution;

    fn item_name(&self) -> Option<&str> {
        self.options.name.as_ref().map(|n| n.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(&self) -> Option<(&'static str, Vec<QueryValue>)> {
        if let Some(name) = &self.options.name {
            let values = vec![QueryValue::Int(self.type_id.get()), QueryValue::Str(name)];
            Some((self.query.check_execution_name(true), values))
        } else {
            None
        }
    }

    fn generate_insert_item_query(&self) -> (String, Vec<QueryValue>) {
        let sql = self.query.insert_execution(&self.options);
        let mut values = vec![
            QueryValue::Int(self.type_id.get()),
            QueryValue::Int(self.options.last_known_state as i32),
            QueryValue::I64(self.options.create_time_since_epoch.as_millis() as i64),
            QueryValue::I64(self.options.last_update_time_since_epoch.as_millis() as i64),
        ];
        if let Some(v) = &self.options.name {
            values.push(QueryValue::Str(v));
        }
        (sql, values)
    }

    fn generate_last_item_id(&self) -> &'static str {
        self.query.get_last_execution_id()
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_execution_property(value)
    }
}

#[derive(Debug)]
pub struct PostContextQueryGenerator {
    pub query: Query,
    pub type_id: Id,
    pub name: String,
    pub options: options::PostContextOptions,
}

impl PostItemQueryGenerator for PostContextQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Context;

    fn item_name(&self) -> Option<&str> {
        Some(self.name.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(&self) -> Option<(&'static str, Vec<QueryValue>)> {
        let values = vec![
            QueryValue::Int(self.type_id.get()),
            QueryValue::Str(&self.name),
        ];
        Some((self.query.check_context_name(true), values))
    }

    fn generate_insert_item_query(&self) -> (String, Vec<QueryValue>) {
        let sql = self.query.insert_context();
        let values = vec![
            QueryValue::Int(self.type_id.get()),
            QueryValue::I64(self.options.create_time_since_epoch.as_millis() as i64),
            QueryValue::I64(self.options.last_update_time_since_epoch.as_millis() as i64),
            QueryValue::Str(&self.name),
        ];
        (sql.to_owned(), values)
    }

    fn generate_last_item_id(&self) -> &'static str {
        self.query.get_last_context_id()
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_context_property(value)
    }
}

pub trait PutItemQueryGenerator {
    const TYPE_KIND: TypeKind;

    fn item_name(&self) -> Option<&str>;
    fn item_properties(&self) -> &BTreeMap<String, PropertyValue>;
    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue>;
    fn generate_get_type_id_query(&self) -> &'static str;
    fn generate_check_item_name_query(
        &self,
        type_id: Id,
    ) -> Option<(&'static str, Vec<QueryValue>)>;
    fn generate_update_item_query(&self, item_id: Id) -> (String, Vec<QueryValue>);
    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String;
}

#[derive(Debug)]
pub struct PutArtifactQueryGenerator {
    pub query: Query,
    pub options: options::ArtifactOptions,
}

impl PutItemQueryGenerator for PutArtifactQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Artifact;

    fn item_name(&self) -> Option<&str> {
        self.options.name.as_ref().map(|n| n.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(
        &self,
        type_id: Id,
    ) -> Option<(&'static str, Vec<QueryValue>)> {
        if let Some(name) = &self.options.name {
            let values = vec![QueryValue::Int(type_id.get()), QueryValue::Str(name)];
            Some((self.query.check_artifact_name(true), values))
        } else {
            None
        }
    }

    fn generate_get_type_id_query(&self) -> &'static str {
        // TODO
        "SELECT type_id FROM Artifact WHERE id = ?"
    }

    fn generate_update_item_query(&self, item_id: Id) -> (String, Vec<QueryValue>) {
        let mut fields = "last_update_time_since_epoch=?".to_owned();
        let mut values = vec![QueryValue::I64(current_millis())];

        if let Some(v) = self.options.state {
            fields += ", state=?";
            values.push(QueryValue::Int(v as i32));
        }
        if let Some(v) = &self.options.name {
            fields += ", name=?";
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = &self.options.uri {
            fields += ", uri=?";
            values.push(QueryValue::Str(v));
        }

        let sql = format!("UPDATE Artifact SET {} WHERE id=?", fields);
        values.push(QueryValue::Int(item_id.get()));

        (sql, values)
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_artifact_property(value)
    }
}

fn current_millis() -> i64 {
    UNIX_EPOCH.elapsed().unwrap_or_default().as_millis() as i64
}

#[derive(Debug)]
pub struct PutExecutionQueryGenerator {
    pub query: Query,
    pub options: options::ExecutionOptions,
}

impl PutItemQueryGenerator for PutExecutionQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Execution;

    fn item_name(&self) -> Option<&str> {
        self.options.name.as_ref().map(|n| n.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(
        &self,
        type_id: Id,
    ) -> Option<(&'static str, Vec<QueryValue>)> {
        if let Some(name) = &self.options.name {
            let values = vec![QueryValue::Int(type_id.get()), QueryValue::Str(name)];
            Some((self.query.check_execution_name(true), values))
        } else {
            None
        }
    }

    fn generate_get_type_id_query(&self) -> &'static str {
        // TODO
        "SELECT type_id FROM Execution WHERE id = ?"
    }

    fn generate_update_item_query(&self, item_id: Id) -> (String, Vec<QueryValue>) {
        let mut fields = "last_update_time_since_epoch=?".to_owned();
        let mut values = vec![QueryValue::I64(current_millis())];

        if let Some(v) = &self.options.name {
            fields += ", name=?";
            values.push(QueryValue::Str(v));
        }
        if let Some(v) = self.options.last_known_state {
            fields += ", last_known_state=?";
            values.push(QueryValue::Int(v as i32));
        }

        let sql = format!("UPDATE Execution SET {} WHERE id=?", fields);
        values.push(QueryValue::Int(item_id.get()));
        (sql, values)
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_execution_property(value)
    }
}

#[derive(Debug)]
pub struct PutContextQueryGenerator {
    pub query: Query,
    pub options: options::ContextOptions,
}

impl PutItemQueryGenerator for PutContextQueryGenerator {
    const TYPE_KIND: TypeKind = TypeKind::Context;

    fn item_name(&self) -> Option<&str> {
        self.options.name.as_ref().map(|n| n.as_str())
    }

    fn item_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.properties
    }

    fn item_custom_properties(&self) -> &BTreeMap<String, PropertyValue> {
        &self.options.custom_properties
    }

    fn generate_check_item_name_query(
        &self,
        type_id: Id,
    ) -> Option<(&'static str, Vec<QueryValue>)> {
        if let Some(name) = &self.options.name {
            let values = vec![QueryValue::Int(type_id.get()), QueryValue::Str(name)];
            Some((self.query.check_context_name(true), values))
        } else {
            None
        }
    }

    fn generate_get_type_id_query(&self) -> &'static str {
        // TODO
        "SELECT type_id FROM Context WHERE id = ?"
    }

    fn generate_update_item_query(&self, item_id: Id) -> (String, Vec<QueryValue>) {
        let mut fields = "last_update_time_since_epoch=?".to_owned();
        let mut values = vec![QueryValue::I64(current_millis())];

        if let Some(v) = &self.options.name {
            fields += ", name=?";
            values.push(QueryValue::Str(v));
        }

        let sql = format!("UPDATE Context SET {} WHERE id=?", fields);
        values.push(QueryValue::Int(item_id.get()));

        (sql.to_owned(), values)
    }

    fn generate_upsert_item_property(&self, value: &PropertyValue) -> String {
        self.query.upsert_context_property(value)
    }
}
