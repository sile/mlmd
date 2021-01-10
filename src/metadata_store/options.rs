use crate::metadata::PropertyType;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct PutArtifactTypeOptions {
    pub(crate) can_add_fields: bool,
    pub(crate) can_omit_fields: bool,
    pub(crate) properties: BTreeMap<String, PropertyType>,
}

impl PutArtifactTypeOptions {
    pub fn can_add_fields(mut self) -> Self {
        self.can_add_fields = true;
        self
    }

    pub fn can_omit_fields(mut self) -> Self {
        self.can_omit_fields = true;
        self
    }

    pub fn property(mut self, key: &str, value_type: PropertyType) -> Self {
        self.properties.insert(key.to_owned(), value_type);
        self
    }

    pub fn property_int(self, key: &str) -> Self {
        self.property(key, PropertyType::Int)
    }

    pub fn property_double(self, key: &str) -> Self {
        self.property(key, PropertyType::Double)
    }

    pub fn property_string(self, key: &str) -> Self {
        self.property(key, PropertyType::String)
    }
}
