-- Your SQL goes here
-- See: https://github.com/google/ml-metadata/blob/v0.25.0/ml_metadata/util/metadata_source_query_config.cc

CREATE TABLE IF NOT EXISTS `Type` (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `version` VARCHAR(255),
  `type_kind` TINYINT(1) NOT NULL,
  `description` TEXT,
  `input_type` TEXT,
  `output_type` TEXT
);

CREATE TABLE IF NOT EXISTS `ParentType` (
   `type_id` INT NOT NULL,
   `parent_type_id` INT NOT NULL,
PRIMARY KEY (`type_id`, `parent_type_id`));

CREATE TABLE IF NOT EXISTS `TypeProperty` (
   `type_id` INT NOT NULL,
   `name` VARCHAR(255) NOT NULL,
   `data_type` INT NULL,
PRIMARY KEY (`type_id`, `name`));

CREATE TABLE IF NOT EXISTS `Artifact` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `type_id` INT NOT NULL,
   `uri` TEXT,
   `state` INT,
   `name` VARCHAR(255),
   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
   UNIQUE(`type_id`, `name`)
);

CREATE TABLE IF NOT EXISTS `ArtifactProperty` (
   `artifact_id` INT NOT NULL,
   `name` VARCHAR(255) NOT NULL,
   `is_custom_property` TINYINT(1) NOT NULL,
   `int_value` INT,
   `double_value` DOUBLE,
   `string_value` TEXT,
PRIMARY KEY (`artifact_id`, `name`, `is_custom_property`));

CREATE TABLE IF NOT EXISTS `Execution` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `type_id` INT NOT NULL,
   `last_known_state` INT,
   `name` VARCHAR(255),
   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
UNIQUE(`type_id`, `name`)
);

CREATE TABLE IF NOT EXISTS `ExecutionProperty` (
   `execution_id` INT NOT NULL,
   `name` VARCHAR(255) NOT NULL,
   `is_custom_property` TINYINT(1) NOT NULL,
   `int_value` INT,
   `double_value` DOUBLE,
   `string_value` TEXT,
PRIMARY KEY (`execution_id`, `name`, `is_custom_property`));

CREATE TABLE IF NOT EXISTS `Context` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `type_id` INT NOT NULL,
   `name` VARCHAR(255) NOT NULL,
   `create_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
   `last_update_time_since_epoch` BIGINT NOT NULL DEFAULT 0,
   UNIQUE(`type_id`, `name`)
);

CREATE TABLE IF NOT EXISTS `ContextProperty` (
   `context_id` INT NOT NULL,
   `name` VARCHAR(255) NOT NULL,
   `is_custom_property` TINYINT(1) NOT NULL,
   `int_value` INT,
   `double_value` DOUBLE,
   `string_value` TEXT,
PRIMARY KEY (`context_id`, `name`, `is_custom_property`));

CREATE TABLE IF NOT EXISTS `ParentContext` (
   `context_id` INT NOT NULL,
   `parent_context_id` INT NOT NULL,
PRIMARY KEY (`context_id`, `parent_context_id`));

CREATE TABLE IF NOT EXISTS `Event` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `artifact_id` INT NOT NULL,
   `execution_id` INT NOT NULL,
   `type` INT NOT NULL,
   `milliseconds_since_epoch` BIGINT
);

CREATE TABLE IF NOT EXISTS `EventPath` (
   `event_id` INT NOT NULL,
   `is_index_step` TINYINT(1) NOT NULL,
   `step_index` INT,
   `step_key` TEXT
);

CREATE TABLE IF NOT EXISTS `Association` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `context_id` INT NOT NULL,
   `execution_id` INT NOT NULL,
   UNIQUE(`context_id`, `execution_id`)
);

CREATE TABLE IF NOT EXISTS `Attribution` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `context_id` INT NOT NULL,
   `artifact_id` INT NOT NULL,
   UNIQUE(`context_id`, `artifact_id`)
);

CREATE TABLE IF NOT EXISTS `MLMDEnv` (
   `schema_version` INTEGER PRIMARY KEY
);
