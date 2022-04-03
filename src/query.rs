// https://github.com/google/ml-metadata/blob/v0.26.0/ml_metadata/util/metadata_source_query_config.cc
use crate::metadata::{EventStep, Id, PropertyValue, TypeId, TypeKind};
use crate::metadata_store::options::{
    GetArtifactsOptions, GetContextsOptions, GetEventsOptions, GetExecutionsOptions,
    GetItemsOptions, GetTypesOptions, ItemOptions,
};
use sqlx::any::AnyArguments;
use sqlx::Arguments as _;
use std::ops::Bound;
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

    pub fn insert_or_ignore_attribution(&self) -> &'static str {
        match self {
            Self::Sqlite(x) => x.insert_attribution(),
            Self::Mysql(x) => x.insert_attribution(),
        }
    }

    pub fn insert_or_ignore_association(&self) -> &'static str {
        match self {
            Self::Sqlite(x) => x.insert_association(),
            Self::Mysql(x) => x.insert_association(),
        }
    }

    pub fn get_type_id(&self, item_id: Id) -> (String, AnyArguments) {
        let sql = format!(
            "SELECT type_id FROM {} WHERE id = ?",
            item_id.kind().item_table_name(),
        );
        let mut args = sqlx::any::AnyArguments::default();
        args.add(item_id.get());
        (sql, args)
    }

    pub fn select_schema_version(&self) -> &'static str {
        "SELECT schema_version FROM MLMDEnv"
    }

    pub fn insert_schema_version(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "INSERT OR IGNORE INTO MLMDEnv VALUES (?)",
            Self::Mysql(_) => "INSERT IGNORE INTO MLMDEnv VALUES (?)",
        }
    }

    pub fn get_types(&self, options: &GetTypesOptions) -> String {
        let mut query = "SELECT id, name FROM Type WHERE type_kind=? ".to_owned();
        if options.name.is_some() {
            query += "AND name = ? ";
        }
        if !options.ids.is_empty() {
            query += &format!("AND id IN ({})", params(options.ids.len()));
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

    pub fn insert_item(&self, type_id: TypeId, options: &ItemOptions) -> (String, AnyArguments) {
        let current_millis = current_millis();

        let mut fields = vec![
            "type_id",
            "create_time_since_epoch",
            "last_update_time_since_epoch",
        ];
        let mut args = AnyArguments::default();
        args.add(type_id.get());
        args.add(current_millis);
        args.add(current_millis);

        if let Some(name) = options.name() {
            fields.push("name");
            args.add(name.to_owned());
        }
        for (name, value) in options.extra_fields() {
            fields.push(name);
            match value {
                QueryValue::Int(v) => args.add(v),
                QueryValue::Str(v) => args.add(v.to_owned()),
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            options.type_kind().item_table_name(),
            fields.join(","),
            params(fields.len())
        );
        (sql, args)
    }

    pub fn update_item(&self, item_id: Id, options: &ItemOptions) -> (String, AnyArguments) {
        let mut fields = "last_update_time_since_epoch=?".to_owned();
        let mut args = AnyArguments::default();
        args.add(current_millis());

        if let Some(v) = options.name() {
            fields += ", name=?";
            args.add(v.to_owned());
        }
        for (name, value) in options.extra_fields() {
            fields += &format!(", {}=?", name);
            match value {
                QueryValue::Int(v) => args.add(v),
                QueryValue::Str(v) => args.add(v.to_owned()),
            }
        }

        let sql = format!(
            "UPDATE {} SET {} WHERE id=?",
            item_id.kind().item_table_name(),
            fields
        );
        args.add(item_id.get());

        (sql, args)
    }

    pub fn upsert_item_property(
        &self,
        item_id: Id,
        property_name: &str,
        value: &PropertyValue,
        is_custom: bool,
    ) -> (String, AnyArguments) {
        let sql = self.upsert_item_property_sql(item_id, value);
        let mut args = AnyArguments::default();
        args.add(item_id.get());
        args.add(property_name.to_owned());
        args.add(is_custom);
        for _ in 0..2 {
            match value.clone() {
                PropertyValue::Int(v) => args.add(v),
                PropertyValue::Double(v) => args.add(v),
                PropertyValue::String(v) => args.add(v),
            }
        }
        (sql, args)
    }

    fn upsert_item_property_sql(&self, item_id: Id, value: &PropertyValue) -> String {
        match self {
            Self::Sqlite(x) => x.upsert_item_property_sql(item_id, value),
            Self::Mysql(x) => x.upsert_item_property_sql(item_id, value),
        }
    }

    pub fn get_item_properties(
        &self,
        type_kind: TypeKind,
        ids: impl Iterator<Item = i32>,
    ) -> (String, AnyArguments) {
        let mut n = 0;
        let mut args = AnyArguments::default();
        for id in ids {
            args.add(id);
            n += 1;
        }
        assert_ne!(n, 0);

        let sql = format!(
            concat!(
                "SELECT {0}_id as id, name, is_custom_property, int_value, double_value, string_value ",
                "FROM {1}Property ",
                "WHERE {0}_id IN ({2})"
            ),
            type_kind,
            type_kind.item_table_name(),
            params(n)
        );
        (sql, args)
    }

    pub fn get_items(&self, options: &GetItemsOptions, count: bool) -> (String, AnyArguments) {
        match options {
            GetItemsOptions::Artifact(x) => self.get_artifacts(x, count),
            GetItemsOptions::Execution(x) => self.get_executions(x, count),
            GetItemsOptions::Context(x) => self.get_contexts(x, count),
        }
    }

    pub fn get_artifacts(
        &self,
        options: &GetArtifactsOptions,
        count: bool,
    ) -> (String, AnyArguments) {
        let mut sql = format!(
            "SELECT {} FROM Artifact as A ",
            if count {
                "COUNT(*)"
            } else {
                "A.id, A.type_id, A.name, A.uri, A.state, A.create_time_since_epoch, A.last_update_time_since_epoch "
            }
        );
        let mut args = AnyArguments::default();

        if options.type_name.is_some() {
            sql += "JOIN Type as T ON A.type_id = T.id ";
        };
        if options.context_id.is_some() {
            sql += "JOIN Attribution as C ON A.id = C.artifact_id ";
        }

        let mut conditions = Vec::new();
        if let Some(v) = options.type_name.clone() {
            conditions.push("T.name = ?".to_owned());
            args.add(v)
        }
        if let Some(v) = options.artifact_name.clone() {
            conditions.push("A.name = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.artifact_name_pattern.clone() {
            conditions.push("A.name LIKE ?".to_owned());
            args.add(v);
        }
        if !options.artifact_ids.is_empty() {
            conditions.push(format!("A.id IN ({})", params(options.artifact_ids.len())));
            for id in &options.artifact_ids {
                args.add(id.get());
            }
        }
        if let Some(v) = options.uri.clone() {
            conditions.push("A.uri = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.context_id {
            conditions.push("C.context_id = ?".to_owned());
            args.add(v.get());
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.create_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.create_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.last_update_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.last_update_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        if !conditions.is_empty() {
            sql += &format!("WHERE {}", conditions.join(" AND "));
        }

        if let Some(field) = options.order_by {
            sql += &format!(
                " ORDER BY {} {}",
                field.field_name(),
                if options.desc { "DESC" } else { "ASC" }
            );
        }

        if let Some(n) = options.limit {
            sql += &format!(" LIMIT {}", n);
            if let Some(n) = options.offset {
                sql += &format!(" OFFSET {}", n);
            }
        }

        (sql, args)
    }

    pub fn get_executions(
        &self,
        options: &GetExecutionsOptions,
        count: bool,
    ) -> (String, AnyArguments) {
        let mut sql = format!(
            "SELECT {} FROM Execution as A ",
            if count {
                "COUNT(*)"
            } else {
                "A.id, A.name, A.type_id, A.last_known_state, A.create_time_since_epoch, A.last_update_time_since_epoch "
            }
        );
        let mut args = AnyArguments::default();

        if options.type_name.is_some() {
            sql += "JOIN Type as T ON A.type_id = T.id ";
        };
        if options.context_id.is_some() {
            sql += "JOIN Association as C ON A.id = C.execution_id ";
        }

        let mut conditions = Vec::new();
        if let Some(v) = options.type_name.clone() {
            conditions.push("T.name = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.execution_name.clone() {
            conditions.push("A.name = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.execution_name_pattern.clone() {
            conditions.push("A.name LIKE ?".to_owned());
            args.add(v);
        }
        if !options.execution_ids.is_empty() {
            conditions.push(format!("A.id IN ({})", params(options.execution_ids.len())));
            for id in &options.execution_ids {
                args.add(id.get());
            }
        }
        if let Some(v) = options.context_id {
            conditions.push("C.context_id = ?".to_owned());
            args.add(v.get());
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.create_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.create_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.last_update_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.last_update_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        if !conditions.is_empty() {
            sql += &format!("WHERE {}", conditions.join(" AND "));
        }

        if let Some(field) = options.order_by {
            sql += &format!(
                " ORDER BY {} {}",
                field.field_name(),
                if options.desc { "DESC" } else { "ASC" }
            );
        }

        if let Some(n) = options.limit {
            sql += &format!(" LIMIT {}", n);
            if let Some(n) = options.offset {
                sql += &format!(" OFFSET {}", n);
            }
        }

        (sql, args)
    }

    pub fn get_contexts(
        &self,
        options: &GetContextsOptions,
        count: bool,
    ) -> (String, AnyArguments) {
        let mut sql = format!(
            "SELECT {} FROM Context as A ",
            if count {
                "COUNT(*)"
            } else {
                "A.id, A.name, A.type_id, A.create_time_since_epoch, A.last_update_time_since_epoch "
            }
        );
        let mut args = AnyArguments::default();

        if options.type_name.is_some() {
            sql += "JOIN Type as T ON A.type_id = T.id ";
        };
        if !options.artifact_ids.is_empty() {
            sql += "JOIN Attribution as B ON A.id = B.context_id ";
        }
        if !options.execution_ids.is_empty() {
            sql += "JOIN Association as C ON A.id = C.context_id ";
        }

        let mut conditions = Vec::new();
        if let Some(v) = options.type_name.clone() {
            conditions.push("T.name = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.context_name.clone() {
            conditions.push("A.name = ?".to_owned());
            args.add(v);
        }
        if let Some(v) = options.context_name_pattern.clone() {
            conditions.push("A.name LIKE ?".to_owned());
            args.add(v);
        }
        if !options.context_ids.is_empty() {
            conditions.push(format!("A.id IN ({})", params(options.context_ids.len())));
            for id in &options.context_ids {
                args.add(id.get());
            }
        }
        if !options.artifact_ids.is_empty() {
            conditions.push(format!(
                "B.artifact_id IN ({})",
                params(options.artifact_ids.len())
            ));
            for id in &options.artifact_ids {
                args.add(id.get());
            }
        }
        if !options.execution_ids.is_empty() {
            conditions.push(format!(
                "C.execution_id IN ({})",
                params(options.execution_ids.len())
            ));
            for id in &options.execution_ids {
                args.add(id.get());
            }
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.create_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .create_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.create_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.create_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.start)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("? <= A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("? < A.last_update_time_since_epoch".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        match options
            .update_time
            .clone()
            .map_or(Bound::Unbounded, |x| x.end)
        {
            Bound::Unbounded => {}
            Bound::Included(v) => {
                conditions.push("A.last_update_time_since_epoch <= ?".to_owned());
                args.add(v.as_millis() as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("A.last_update_time_since_epoch < ?".to_owned());
                args.add(v.as_millis() as i64);
            }
        }

        if !conditions.is_empty() {
            sql += &format!("WHERE {}", conditions.join(" AND "));
        }

        if let Some(field) = options.order_by {
            sql += &format!(
                " ORDER BY {} {}",
                field.field_name(),
                if options.desc { "DESC" } else { "ASC" }
            );
        }

        if let Some(n) = options.limit {
            sql += &format!(" LIMIT {}", n);
            if let Some(n) = options.offset {
                sql += &format!(" OFFSET {}", n);
            }
        }

        (sql, args)
    }

    pub fn get_last_item_id(&self, type_kind: TypeKind) -> String {
        format!(
            "SELECT id FROM {} ORDER BY id DESC LIMIT 1",
            type_kind.item_table_name()
        )
    }

    pub fn check_item_name(
        &self,
        type_kind: TypeKind,
        type_id: TypeId,
        item_id: Option<Id>,
        item_name: &str,
    ) -> (String, AnyArguments) {
        let mut sql = format!(
            "SELECT count(*) FROM {} WHERE type_id=? AND name=?",
            type_kind.item_table_name()
        );
        let mut args = AnyArguments::default();
        args.add(type_id.get());
        args.add(item_name.to_owned());

        if let Some(item_id) = item_id {
            sql += " AND id != ?";
            args.add(item_id.get());
        }

        (sql, args)
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

    pub fn get_events(&self, options: &GetEventsOptions, count: bool) -> String {
        let mut query = format!(
            "SELECT {} FROM Event ",
            if count {
                "count(*)"
            } else {
                "Event.id, artifact_id, execution_id, Event.type, milliseconds_since_epoch"
            }
        );
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
                params(options.artifact_ids.len())
            ));
        }
        if !options.execution_ids.is_empty() {
            conditions.push(format!(
                "Execution.id IN ({}) ",
                params(options.execution_ids.len())
            ));
        }
        if !conditions.is_empty() {
            query += &format!("WHERE {}", conditions.join(" AND "));
        }
        if let Some(field) = options.order_by {
            query += &format!(
                " ORDER BY {} {}",
                field.field_name(),
                if options.desc { "DESC" } else { "ASC" }
            );
        }

        if let Some(n) = options.limit {
            query += &format!(" LIMIT {}", n);
            if let Some(n) = options.offset {
                query += &format!(" OFFSET {}", n);
            }
        }
        query
    }

    pub fn get_event_paths(&self, n_events: usize) -> String {
        format!("SELECT event_id, is_index_step, step_index, step_key FROM EventPath WHERE event_id IN ({})",
                params(n_events))
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
                "   `byte_value` BLOB, ",
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
                "   `byte_value` BLOB, ",
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
                "   `byte_value` BLOB, ",
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
                "   `milliseconds_since_epoch` INT, ",
                "   UNIQUE(`artifact_id`, `execution_id`, `type`) ",
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
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_eventpath_event_id` ",
                " ON `EventPath`(`event_id`); "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_artifact_property_int` ",
                " ON `ArtifactProperty`(`name`, `is_custom_property`, `int_value`) ",
                " WHERE `int_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_artifact_property_double` ",
                " ON `ArtifactProperty`(`name`, `is_custom_property`, `double_value`) ",
                " WHERE `double_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_artifact_property_string` ",
                " ON `ArtifactProperty`(`name`, `is_custom_property`, `string_value`) ",
                " WHERE `string_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_execution_property_int` ",
                " ON `ExecutionProperty`(`name`, `is_custom_property`, `int_value`) ",
                " WHERE `int_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_execution_property_double` ",
                " ON `ExecutionProperty`(`name`, `is_custom_property`, `double_value`) ",
                " WHERE `double_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_execution_property_string` ",
                " ON `ExecutionProperty`(`name`, `is_custom_property`, `string_value`) ",
                " WHERE `string_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_context_property_int` ",
                " ON `ContextProperty`(`name`, `is_custom_property`, `int_value`) ",
                " WHERE `int_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_context_property_double` ",
                " ON `ContextProperty`(`name`, `is_custom_property`, `double_value`) ",
                " WHERE `double_value` IS NOT NULL; "
            ),
            concat!(
                " CREATE INDEX IF NOT EXISTS `idx_context_property_string` ",
                " ON `ContextProperty`(`name`, `is_custom_property`, `string_value`) ",
                " WHERE `string_value` IS NOT NULL; "
            ),
        ]
    }

    fn insert_attribution(&self) -> &'static str {
        "INSERT OR IGNORE INTO Attribution (context_id, artifact_id) VALUES (?, ?)"
    }

    fn insert_association(&self) -> &'static str {
        "INSERT OR IGNORE INTO Association (context_id, execution_id) VALUES (?, ?)"
    }

    fn upsert_item_property_sql(&self, item_id: Id, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO {3}Property ",
                "({4}_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON CONFLICT ({4}_id, name, is_custom_property) ",
                "DO UPDATE SET int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?"),
            item_id.kind().item_table_name(),
            item_id.kind()
        )
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

    fn upsert_item_property_sql(&self, item_id: Id, value: &PropertyValue) -> String {
        format!(
            concat!(
                "INSERT INTO {3}Property ",
                "({4}_id, name, is_custom_property, int_value, double_value, string_value) ",
                "VALUES (?, ?, ?, {0}, {1}, {2}) ",
                "ON DUPLICATE KEY ",
                "UPDATE int_value={0}, double_value={1}, string_value={2}"
            ),
            maybe_null(value.as_int().is_some(), "?"),
            maybe_null(value.as_double().is_some(), "?"),
            maybe_null(value.as_string().is_some(), "?"),
            item_id.kind().item_table_name(),
            item_id.kind()
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

#[derive(Debug)]
pub enum QueryValue<'a> {
    Int(i32),
    Str(&'a str),
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

pub trait InsertProperty {
    fn insert_property(&mut self, is_custom: bool, name: String, value: PropertyValue);
}

pub trait GetItemsQueryGenerator {
    type Item: for<'a> sqlx::FromRow<'a, sqlx::any::AnyRow> + InsertProperty;

    fn generate_select_items_sql(&self) -> String;
    fn query_values(&self) -> Vec<QueryValue>;
}

fn maybe_null(b: bool, s: &str) -> &str {
    if b {
        s
    } else {
        "NULL"
    }
}

fn current_millis() -> i64 {
    UNIX_EPOCH.elapsed().unwrap_or_default().as_millis() as i64
}

fn params(n: usize) -> String {
    (0..n).map(|_| "?").collect::<Vec<_>>().join(",")
}
