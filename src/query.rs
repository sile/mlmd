// https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/util/metadata_source_query_config.cc

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

    pub fn select_schema_version(&self) -> &'static str {
        "SELECT schema_version FROM MLMDEnv"
    }

    pub fn insert_schema_version(&self) -> &'static str {
        "INSERT INTO MLMDEnv VALUES ($1)"
    }

    pub fn get_artifact_types(&self) -> &'static str {
        "SELECT id, name FROM Type WHERE type_kind=0"
    }

    pub fn get_type_properties(&self) -> &'static str {
        "SELECT type_id, name, data_type FROM TypeProperty"
    }

    pub fn get_artifact_type(&self) -> &'static str {
        "SELECT id FROM Type WHERE name=$1 AND type_kind=0"
    }

    pub fn get_artifact_type_properties(&self) -> &'static str {
        "SELECT name, data_type FROM TypeProperty WHERE type_id=$1"
    }

    pub fn insert_artifact_type(&self) -> &'static str {
        "INSERT INTO Type (name, type_kind) VALUES ($1, 0)"
    }

    pub fn insert_artifact_type_property(&self) -> &'static str {
        "INSERT INTO TypeProperty (type_id, name, data_type) VALUES ($1, $2, $3)"
    }
}

#[derive(Debug, Clone)]
pub struct SqliteQuery;

impl SqliteQuery {
    pub fn create_tables(&self) -> &'static [&'static str] {
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
}

#[derive(Debug, Clone)]
pub struct MysqlQuery;

impl MysqlQuery {
    pub fn create_tables(&self) -> &'static [&'static str] {
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
}
