use crate::plain_term::{PlainTermArray, PlainTermEncoding};
use datafusion::arrow::array::{Array, StringArray, StructArray};
use std::sync::Arc;

/// Provides a convenient API for building a [PlainTermArray] from its components.
pub struct PlainTermArrayBuilder {
    term_types: Arc<dyn Array>,
    values: Arc<dyn Array>,
    data_types: Option<Arc<dyn Array>>,
    language_tags: Option<Arc<dyn Array>>,
}

impl PlainTermArrayBuilder {
    pub fn new(term_types: Arc<dyn Array>, values: Arc<dyn Array>) -> Self {
        Self {
            term_types,
            values,
            data_types: None,
            language_tags: None,
        }
    }

    pub fn with_data_types(mut self, data_types: Arc<dyn Array>) -> Self {
        self.data_types = Some(data_types);
        self
    }

    pub fn with_language_tags(mut self, language_tags: Arc<dyn Array>) -> Self {
        self.language_tags = Some(language_tags);
        self
    }

    pub fn finish(self) -> PlainTermArray {
        let len = self.term_types.len();
        let nulls = self.term_types.nulls().cloned();
        let struct_array = StructArray::new(
            PlainTermEncoding::fields(),
            vec![
                self.term_types,
                self.values,
                self.data_types
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(len))),
                self.language_tags
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(len))),
            ],
            nulls,
        );
        PlainTermArray::new_unchecked(Arc::new(struct_array))
    }
}
