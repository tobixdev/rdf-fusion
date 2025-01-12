use crate::encoded::{
    ENC_FIELDS_STRING, ENC_FIELDS_TERM, ENC_FIELDS_TYPED_LITERAL, ENC_TYPE_ID_INTEGER,
    ENC_TYPE_ID_NAMED_NODE, ENC_TYPE_ID_STRING, ENC_TYPE_ID_TYPED_LITERAL,
};
use crate::{AResult, DFResult};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use std::sync::Arc;

pub struct RdfTermBuilder {
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    named_node_builder: StringBuilder,
    blank_node_builder: StringBuilder,
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
            type_ids: Vec::new(),
            offsets: Vec::new(),
            named_node_builder: StringBuilder::new(),
            blank_node_builder: StringBuilder::new(),
            string_builder: StructBuilder::from_fields(ENC_FIELDS_STRING.clone(), 0),
            boolean_builder: BooleanBuilder::new(),
            float32_builder: Float32Builder::new(),
            float64_builder: Float64Builder::new(),
            int32_builder: Int32Builder::new(),
            integer_builder: Int64Builder::new(),
            typed_literal_builder: StructBuilder::from_fields(ENC_FIELDS_TYPED_LITERAL.clone(), 0),
        }
    }

    pub fn append_named_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_NAMED_NODE);
        self.offsets.push(self.named_node_builder.len() as i32);
        self.named_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_STRING);
        self.offsets.push(self.string_builder.len() as i32);

        self.string_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);

        let language_builder = self
            .string_builder
            .field_builder::<StringBuilder>(1)
            .unwrap();
        if let Some(language) = language {
            language_builder.append_value(language);
        } else {
            language_builder.append_null();
        }
        self.string_builder.append(true);

        Ok(())
    }

    pub fn append_integer(&mut self, integer: i64) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_INTEGER);
        self.offsets.push(self.integer_builder.len() as i32);
        self.integer_builder.append_value(integer);
        Ok(())
    }

    pub fn append_typed_literal(&mut self, value: &str, type_id: &str) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_TYPED_LITERAL);
        self.offsets.push(self.typed_literal_builder.len() as i32);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(type_id);
        self.typed_literal_builder.append(true);
        Ok(())
    }

    pub fn finish(mut self) -> DFResult<ArrayRef> {
        Ok(Arc::new(UnionArray::try_new(
            ENC_FIELDS_TERM.clone(),
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            vec![
                Arc::new(self.named_node_builder.finish()),
                Arc::new(self.blank_node_builder.finish()),
                Arc::new(self.string_builder.finish()),
                Arc::new(self.boolean_builder.finish()),
                Arc::new(self.float32_builder.finish()),
                Arc::new(self.float64_builder.finish()),
                Arc::new(self.int32_builder.finish()),
                Arc::new(self.integer_builder.finish()),
                Arc::new(self.typed_literal_builder.finish()),
            ],
        )?))
    }
}
