use crate::datatypes::{XsdDecimal, XsdDouble, XsdFloat, XsdInt, XsdInteger};
use crate::encoded::{EncTerm, EncTermField};
use crate::error::LiteralEncodingError;
use crate::{AResult, DFResult, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, NullBuilder, StringBuilder, StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::error::ArrowError;
use oxiri::Iri;
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNode, Literal, Term};
use std::sync::Arc;

pub struct EncRdfTermBuilder {
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    named_node_builder: StringBuilder,
    blank_node_builder: StringBuilder,
    string_builder: StructBuilder,
    boolean_builder: BooleanBuilder,
    float_builder: Float32Builder,
    double_builder: Float64Builder,
    decimal_builder: Decimal128Builder,
    int32_builder: Int32Builder,
    integer_builder: Int64Builder,
    typed_literal_builder: StructBuilder,
    null_builder: NullBuilder,
}

impl EncRdfTermBuilder {
    pub fn new() -> Self {
        Self {
            type_ids: Vec::new(),
            offsets: Vec::new(),
            named_node_builder: StringBuilder::with_capacity(0, 0),
            blank_node_builder: StringBuilder::with_capacity(0, 0),
            string_builder: StructBuilder::from_fields(EncTerm::string_fields(), 0),
            boolean_builder: BooleanBuilder::with_capacity(0),
            float_builder: Float32Builder::with_capacity(0),
            double_builder: Float64Builder::with_capacity(0),
            decimal_builder: Decimal128Builder::with_capacity(0)
                .with_precision_and_scale(RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE)
                .expect("Precision and scale fixed"),
            int32_builder: Int32Builder::with_capacity(0),
            integer_builder: Int64Builder::with_capacity(0),
            typed_literal_builder: StructBuilder::from_fields(EncTerm::typed_literal_fields(), 0),
            null_builder: NullBuilder::new(),
        }
    }

    pub fn append_term(&mut self, value: &Term) -> Result<(), ArrowError> {
        Ok(match value {
            Term::NamedNode(nn) => self.append_named_node(nn.as_str())?,
            Term::BlankNode(bnode) => self.append_blank_node(bnode.as_str())?,
            Term::Literal(literal) => match self.append_literal(literal) {
                Ok(_) => (),
                Err(LiteralEncodingError::Arrow(error)) => return Err(error),
                Err(LiteralEncodingError::ParsingError(_)) => {
                    self.append_typed_literal(literal.value(), literal.datatype().as_str())?
                }
            },
            _ => unimplemented!(),
        })
    }

    fn append_literal(&mut self, literal: &Literal) -> Result<(), LiteralEncodingError> {
        Ok(match literal.datatype() {
            xsd::BOOLEAN => self.append_boolean(literal.value().parse()?)?,
            xsd::FLOAT => self.append_float(literal.value().parse()?)?,
            xsd::DOUBLE => self.append_double(literal.value().parse()?)?,
            xsd::INTEGER => self.append_integer(literal.value().parse()?)?,
            xsd::INT => self.append_int(literal.value().parse()?)?,
            rdf::LANG_STRING => self.append_string(literal.value(), literal.language())?,
            xsd::STRING => self.append_string(literal.value(), None)?,
            _ => self.append_typed_literal(literal.value(), literal.datatype().as_str())?,
        })
    }

    pub fn append_boolean(&mut self, value: bool) -> AResult<()> {
        self.type_ids.push(EncTermField::Boolean.type_id());
        self.offsets.push(self.boolean_builder.len() as i32);
        self.boolean_builder.append_value(value);
        Ok(())
    }

    pub fn append_named_node(&mut self, value: &str) -> AResult<()> {
        if Iri::parse(value).is_err() {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "Invalid IRI.",
            )));
        }

        self.type_ids.push(EncTermField::NamedNode.type_id());
        self.offsets.push(self.named_node_builder.len() as i32);
        self.named_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_blank_node(&mut self, value: &str) -> AResult<()> {
        if BlankNode::new(value).is_err() {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "Invalid BlankNode id",
            )));
        }

        self.type_ids.push(EncTermField::BlankNode.type_id());
        self.offsets.push(self.blank_node_builder.len() as i32);
        self.blank_node_builder.append_value(value);
        Ok(())
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        if language == Some("") {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "Empty language.",
            )));
        }

        self.type_ids.push(EncTermField::String.type_id());
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

    pub fn append_int(&mut self, int: XsdInt) -> AResult<()> {
        self.type_ids.push(EncTermField::Int.type_id());
        self.offsets.push(self.int32_builder.len() as i32);
        self.int32_builder.append_value(int.as_i32());
        Ok(())
    }

    pub fn append_integer(&mut self, integer: XsdInteger) -> AResult<()> {
        self.type_ids.push(EncTermField::Integer.type_id());
        self.offsets.push(self.integer_builder.len() as i32);
        self.integer_builder.append_value(integer.as_i64());
        Ok(())
    }

    pub fn append_float(&mut self, value: XsdFloat) -> AResult<()> {
        self.type_ids.push(EncTermField::Float.type_id());
        self.offsets.push(self.float_builder.len() as i32);
        self.float_builder.append_value(value.as_f32());
        Ok(())
    }

    pub fn append_double(&mut self, value: XsdDouble) -> AResult<()> {
        self.type_ids.push(EncTermField::Double.type_id());
        self.offsets.push(self.double_builder.len() as i32);
        self.double_builder.append_value(value.as_f64());
        Ok(())
    }

    pub fn append_decimal(&mut self, value: XsdDecimal) -> AResult<()> {
        self.type_ids.push(EncTermField::Decimal.type_id());
        self.offsets.push(self.decimal_builder.len() as i32);
        self.decimal_builder
            .append_value(i128::from_be_bytes(value.to_be_bytes()));
        Ok(())
    }

    pub fn append_typed_literal(&mut self, value: &str, datatype: &str) -> AResult<()> {
        if Iri::parse(datatype).is_err() {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "Invalid IRI for datatype.",
            )));
        }

        self.type_ids.push(EncTermField::TypedLiteral.type_id());
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

    pub fn append_null(&mut self) -> AResult<()> {
        self.type_ids.push(EncTermField::Null.type_id());
        self.offsets.push(self.null_builder.len() as i32);
        self.null_builder.append_null();
        Ok(())
    }

    pub fn finish(mut self) -> DFResult<ArrayRef> {
        Ok(Arc::new(UnionArray::try_new(
            EncTerm::term_fields(),
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            vec![
                Arc::new(self.null_builder.finish()),
                Arc::new(self.named_node_builder.finish()),
                Arc::new(self.blank_node_builder.finish()),
                Arc::new(self.string_builder.finish()),
                Arc::new(self.boolean_builder.finish()),
                Arc::new(self.float_builder.finish()),
                Arc::new(self.double_builder.finish()),
                Arc::new(self.decimal_builder.finish()),
                Arc::new(self.int32_builder.finish()),
                Arc::new(self.integer_builder.finish()),
                Arc::new(self.typed_literal_builder.finish()),
            ],
        )?))
    }
}
