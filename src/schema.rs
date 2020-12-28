table! {
    Artifact (id) {
        id -> Nullable<Integer>,
        type_id -> Integer,
        uri -> Nullable<Text>,
        state -> Nullable<Integer>,
        name -> Nullable<Text>,
        create_time_since_epoch -> Integer,
        last_update_time_since_epoch -> Integer,
    }
}

table! {
    ArtifactProperty (artifact_id, name, is_custom_property) {
        artifact_id -> Integer,
        name -> Text,
        is_custom_property -> Bool,
        int_value -> Nullable<Integer>,
        double_value -> Nullable<Double>,
        string_value -> Nullable<Text>,
    }
}

table! {
    Association (id) {
        id -> Nullable<Integer>,
        context_id -> Integer,
        execution_id -> Integer,
    }
}

table! {
    Attribution (id) {
        id -> Nullable<Integer>,
        context_id -> Integer,
        artifact_id -> Integer,
    }
}

table! {
    Context (id) {
        id -> Nullable<Integer>,
        type_id -> Integer,
        name -> Text,
        create_time_since_epoch -> Integer,
        last_update_time_since_epoch -> Integer,
    }
}

table! {
    ContextProperty (context_id, name, is_custom_property) {
        context_id -> Integer,
        name -> Text,
        is_custom_property -> Bool,
        int_value -> Nullable<Integer>,
        double_value -> Nullable<Double>,
        string_value -> Nullable<Text>,
    }
}

table! {
    Event (id) {
        id -> Nullable<Integer>,
        artifact_id -> Integer,
        execution_id -> Integer,
        #[sql_name = "type"]
        type_ -> Integer,
        milliseconds_since_epoch -> Nullable<Integer>,
    }
}

table! {
    Execution (id) {
        id -> Nullable<Integer>,
        type_id -> Integer,
        last_known_state -> Nullable<Integer>,
        name -> Nullable<Text>,
        create_time_since_epoch -> Integer,
        last_update_time_since_epoch -> Integer,
    }
}

table! {
    ExecutionProperty (execution_id, name, is_custom_property) {
        execution_id -> Integer,
        name -> Text,
        is_custom_property -> Bool,
        int_value -> Nullable<Integer>,
        double_value -> Nullable<Double>,
        string_value -> Nullable<Text>,
    }
}

table! {
    MLMDEnv (schema_version) {
        schema_version -> Nullable<Integer>,
    }
}

table! {
    ParentContext (context_id, parent_context_id) {
        context_id -> Integer,
        parent_context_id -> Integer,
    }
}

table! {
    ParentType (type_id, parent_type_id) {
        type_id -> Integer,
        parent_type_id -> Integer,
    }
}

table! {
    Type (id) {
        id -> Nullable<Integer>,
        name -> Text,
        version -> Nullable<Text>,
        type_kind -> Bool,
        description -> Nullable<Text>,
        input_type -> Nullable<Text>,
        output_type -> Nullable<Text>,
    }
}

table! {
    TypeProperty (type_id, name) {
        type_id -> Integer,
        name -> Text,
        data_type -> Nullable<Integer>,
    }
}

allow_tables_to_appear_in_same_query!(
    Artifact,
    ArtifactProperty,
    Association,
    Attribution,
    Context,
    ContextProperty,
    Event,
    Execution,
    ExecutionProperty,
    MLMDEnv,
    ParentContext,
    ParentType,
    Type,
    TypeProperty,
);
