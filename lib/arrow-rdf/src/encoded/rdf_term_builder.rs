use crate::encoded::{
    ENC_FIELDS_STRING, ENC_FIELDS_TERM, ENC_FIELDS_TYPED_LITERAL, ENC_TYPE_ID_BLANK_NODE,
    ENC_TYPE_ID_BOOLEAN, ENC_TYPE_ID_FLOAT32, ENC_TYPE_ID_FLOAT64, ENC_TYPE_ID_INT,
    ENC_TYPE_ID_INTEGER, ENC_TYPE_ID_NAMED_NODE, ENC_TYPE_ID_STRING, ENC_TYPE_ID_TYPED_LITERAL,
};
use crate::error::TermEncodingError;
use crate::{AResult, DFResult};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::Term;
use std::sync::Arc;

pub struct EncRdfTermBuilder {
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

impl EncRdfTermBuilder {
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

    pub fn append_term(&mut self, value: &Term) -> Result<(), TermEncodingError> {
        Ok(match value {
            Term::NamedNode(nn) => self.append_named_node(nn.as_str())?,
            Term::BlankNode(bnode) => self.append_blank_node(bnode.as_str())?,
            Term::Literal(literal) => match literal.datatype() {
                xsd::BOOLEAN => self.append_boolean(literal.value().parse().unwrap())?,
                xsd::FLOAT => self.append_float32(literal.value().parse().unwrap())?,
                xsd::DOUBLE => self.append_float64(literal.value().parse().unwrap())?,
                xsd::INTEGER => self.append_integer(literal.value().parse().unwrap())?,
                xsd::INT => self.append_int(literal.value().parse().unwrap())?,
                rdf::LANG_STRING => self.append_string(literal.value(), Some(literal.value()))?,
                xsd::STRING => self.append_string(literal.value(), None)?,
                _ => self.append_typed_literal(literal.value(), literal.datatype().as_str())?,
            },
            _ => unimplemented!(),
        })
    }

    pub fn append_boolean(&mut self, value: bool) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_BOOLEAN);
        self.offsets.push(self.boolean_builder.len() as i32);
        self.boolean_builder.append_value(value);
        Ok(())
    }

    pub fn append_named_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_NAMED_NODE);
        self.offsets.push(self.named_node_builder.len() as i32);
        self.named_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_blank_node(&mut self, value: &str) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_BLANK_NODE);
        self.offsets.push(self.blank_node_builder.len() as i32);
        self.blank_node_builder.append_value(value);
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

    pub fn append_int(&mut self, int: i32) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_INT);
        self.offsets.push(self.int32_builder.len() as i32);
        self.int32_builder.append_value(int);
        Ok(())
    }

    pub fn append_float32(&mut self, value: f32) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_FLOAT32);
        self.offsets.push(self.float32_builder.len() as i32);
        self.float32_builder.append_value(value);
        Ok(())
    }

    pub fn append_float64(&mut self, value: f64) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_FLOAT64);
        self.offsets.push(self.float64_builder.len() as i32);
        self.float64_builder.append_value(value);
        Ok(())
    }

    pub fn append_integer(&mut self, integer: i64) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_INTEGER);
        self.offsets.push(self.integer_builder.len() as i32);
        self.integer_builder.append_value(integer);
        Ok(())
    }

    pub fn append_typed_literal(&mut self, value: &str, datatype: &str) -> AResult<()> {
        self.type_ids.push(ENC_TYPE_ID_TYPED_LITERAL);
        self.offsets.push(self.typed_literal_builder.len() as i32);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(value);
        self.typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(datatype);
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
