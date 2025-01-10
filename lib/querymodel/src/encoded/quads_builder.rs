use crate::encoded::rdf_dictionary_builder::EncodedStringBuilder;
use crate::encoded::{
    EncDictKey, ENC_BIG_TYPED_LITERAL_FIELDS, ENC_NUMERICAL_BNODE_SIZE, ENC_SMALL_STRING_SIZE,
    ENC_SMALL_TYPED_LITERAL_FIELDS, ENC_TERM_BIG_STRING_TYPE_ID,
    ENC_TERM_BIG_TYPED_LITERAL_TYPE_ID, ENC_TERM_FIELDS, ENC_TERM_SMALL_STRING_TYPE_ID,
};
use crate::{AResult, DFResult};
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder,
    StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::error::ArrowError;
use std::sync::Arc;

pub struct RdfTermBuilder {
    type_ids: Vec<i8>,
    named_builder: EncodedStringBuilder,
    numerical_blank_builder: FixedSizeBinaryBuilder,
    small_blank_node_builder: FixedSizeBinaryBuilder,
    big_blank_node_builder: EncodedStringBuilder,
    small_string_builder: FixedSizeBinaryBuilder,
    big_string_builder: EncodedStringBuilder,
    boolean_builder: BooleanBuilder,
    float32_builder: Float32Builder,
    float64_builder: Float64Builder,
    int32_builder: Int32Builder,
    small_typed_literal: StructBuilder,
    big_typed_literal: StructBuilder,
}

impl RdfTermBuilder {
    pub fn new() -> Self {
        Self {
            type_ids: vec![],
            named_builder: EncodedStringBuilder::new(),
            numerical_blank_builder: FixedSizeBinaryBuilder::new(ENC_NUMERICAL_BNODE_SIZE as i32),
            small_blank_node_builder: FixedSizeBinaryBuilder::new(ENC_SMALL_STRING_SIZE as i32),
            big_blank_node_builder: EncodedStringBuilder::new(),
            small_string_builder: FixedSizeBinaryBuilder::new(ENC_SMALL_STRING_SIZE as i32),
            big_string_builder: EncodedStringBuilder::new(),
            boolean_builder: BooleanBuilder::new(),
            float32_builder: Float32Builder::new(),
            float64_builder: Float64Builder::new(),
            int32_builder: Int32Builder::new(),
            small_typed_literal: StructBuilder::from_fields(
                ENC_SMALL_TYPED_LITERAL_FIELDS.clone(),
                0,
            ),
            big_typed_literal: StructBuilder::from_fields(ENC_BIG_TYPED_LITERAL_FIELDS.clone(), 0),
        }
    }

    pub fn append_small_string(&mut self, string: &str) -> AResult<()> {
        let string_bytes = string.as_bytes();
        if string_bytes.len() > ENC_SMALL_STRING_SIZE {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Small string is too long ({})",
                string_bytes.len()
            )));
        }

        self.type_ids.push(*ENC_TERM_SMALL_STRING_TYPE_ID);
        if string_bytes.len() < ENC_SMALL_STRING_SIZE {
            let mut buffer = [0u8; ENC_SMALL_STRING_SIZE];
            buffer[..string_bytes.len()].copy_from_slice(string_bytes);
            self.small_string_builder.append_value(buffer)
        } else {
            self.small_string_builder.append_value(string_bytes)
        }
    }

    pub fn append_big_string(&mut self, id: &EncDictKey) -> AResult<()> {
        self.type_ids.push(*ENC_TERM_BIG_STRING_TYPE_ID);
        self.big_string_builder.append_value(id, "test") // TODO
    }

    pub fn append_big_typed_literal(
        &mut self,
        value_id: &EncDictKey,
        type_id: &EncDictKey,
    ) -> AResult<()> {
        self.type_ids.push(*ENC_TERM_BIG_TYPED_LITERAL_TYPE_ID);
        self.big_typed_literal
            .field_builder::<FixedSizeBinaryBuilder>(0)
            .unwrap()
            .append_value(value_id)?;
        self.big_typed_literal
            .field_builder::<FixedSizeBinaryBuilder>(1)
            .unwrap()
            .append_value(type_id)
    }

    pub fn finish(mut self) -> DFResult<ArrayRef> {
        Ok(Arc::new(UnionArray::try_new(
            ENC_TERM_FIELDS.clone(),
            ScalarBuffer::from(self.type_ids),
            None,
            vec![
                Arc::new(self.named_builder.finish()),
                Arc::new(self.numerical_blank_builder.finish()),
                Arc::new(self.small_blank_node_builder.finish()),
                Arc::new(self.big_blank_node_builder.finish()),
                Arc::new(self.small_string_builder.finish()),
                Arc::new(self.boolean_builder.finish()),
                Arc::new(self.float32_builder.finish()),
                Arc::new(self.float64_builder.finish()),
                Arc::new(self.int32_builder.finish()),
            ],
        )?))
    }
}
