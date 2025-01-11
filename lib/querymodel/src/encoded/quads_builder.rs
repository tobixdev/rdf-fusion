use crate::encoded::{
    FIELDS_STRING, FIELDS_TERM, FIELDS_TYPED_LITERAL, TYPE_ID_INTEGER, TYPE_ID_NAMED_NODE,
    TYPE_ID_STRING, TYPE_ID_TYPED_LITERAL,
};
use crate::{AResult, DFResult};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, StringViewBuilder, StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use std::sync::Arc;

pub struct RdfTermBuilder {
    type_ids: Vec<i8>,
    named_node_builder: StringViewBuilder,
    blank_node_builder: StringViewBuilder,
    string_builder: StructBuilder,
    boolean_builder: BooleanBuilder,
    float32_builder: Float32Builder,
    float64_builder: Float64Builder,
    int32_builder: Int32Builder,
    integer_builder: Int64Builder,
    typed_literal_builder: StructBuilder,
}

impl RdfTermBuilder {
    pub fn new() -> Self {
        Self {
            type_ids: vec![],
            named_node_builder: StringViewBuilder::new(),
            blank_node_builder: StringViewBuilder::new(),
            string_builder: StructBuilder::from_fields(FIELDS_STRING.clone(), 0),
            boolean_builder: BooleanBuilder::new(),
            float32_builder: Float32Builder::new(),
            float64_builder: Float64Builder::new(),
            int32_builder: Int32Builder::new(),
            integer_builder: Int64Builder::new(),
            typed_literal_builder: StructBuilder::from_fields(FIELDS_TYPED_LITERAL.clone(), 0),
        }
    }

    pub fn append_named_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(*TYPE_ID_NAMED_NODE);
        self.named_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        self.type_ids.push(*TYPE_ID_STRING);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);

        let language_builder = self
            .typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .unwrap();
        if let Some(language) = language {
            language_builder.append_value(language);
        } else {
            language_builder.append_null();
        }
        Ok(())
    }

    pub fn append_integer(&mut self, integer: i64) -> AResult<()> {
        self.type_ids.push(*TYPE_ID_INTEGER);
        self.integer_builder.append_value(integer);
        Ok(())
    }

    pub fn append_typed_literal(&mut self, value: &str, type_id: &str) -> AResult<()> {
        self.type_ids.push(*TYPE_ID_TYPED_LITERAL);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(type_id);
        Ok(())
    }

    pub fn finish(mut self) -> DFResult<ArrayRef> {
        Ok(Arc::new(UnionArray::try_new(
            FIELDS_TERM.clone(),
            ScalarBuffer::from(self.type_ids),
            None,
            vec![
                Arc::new(self.named_node_builder.finish()),
                Arc::new(self.blank_node_builder.finish()),
                Arc::new(self.string_builder.finish()),
                Arc::new(self.boolean_builder.finish()),
                Arc::new(self.float32_builder.finish()),
                Arc::new(self.float64_builder.finish()),
                Arc::new(self.int32_builder.finish()),
                Arc::new(self.typed_literal_builder.finish()),
            ],
        )?))
    }
}
