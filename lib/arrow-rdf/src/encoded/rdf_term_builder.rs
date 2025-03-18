use crate::encoded::{EncTerm, EncTermField};
use crate::error::LiteralEncodingError;
use crate::{AResult, DFResult};
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, NullBuilder, StringBuilder, StructBuilder, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::error::ArrowError;
use datamodel::{BlankNode, DayTimeDuration, YearMonthDuration};
use datamodel::{Decimal, DecodedTerm, Double, Float, Int, Integer, Iri, Literal};
use oxrdf::vocab::{rdf, xsd};
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
    duration_builder: StructBuilder,
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
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .expect("Precision and scale fixed"),
            int32_builder: Int32Builder::with_capacity(0),
            integer_builder: Int64Builder::with_capacity(0),
            duration_builder: StructBuilder::from_fields(EncTerm::duration_fields(), 0),
            typed_literal_builder: StructBuilder::from_fields(EncTerm::typed_literal_fields(), 0),
            null_builder: NullBuilder::new(),
        }
    }

    pub fn append_term(&mut self, value: &DecodedTerm) -> Result<(), ArrowError> {
        Ok(match value {
            DecodedTerm::NamedNode(nn) => self.append_named_node(nn.as_str())?,
            DecodedTerm::BlankNode(bnode) => self.append_blank_node(bnode.as_str())?,
            DecodedTerm::Literal(literal) => match self.append_literal(literal) {
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

    pub fn append_int(&mut self, int: Int) -> AResult<()> {
        self.type_ids.push(EncTermField::Int.type_id());
        self.offsets.push(self.int32_builder.len() as i32);
        self.int32_builder.append_value(int.into());
        Ok(())
    }

    pub fn append_integer(&mut self, integer: Integer) -> AResult<()> {
        self.type_ids.push(EncTermField::Integer.type_id());
        self.offsets.push(self.integer_builder.len() as i32);
        self.integer_builder.append_value(integer.into());
        Ok(())
    }

    pub fn append_float(&mut self, value: Float) -> AResult<()> {
        self.type_ids.push(EncTermField::Float.type_id());
        self.offsets.push(self.float_builder.len() as i32);
        self.float_builder.append_value(value.into());
        Ok(())
    }

    pub fn append_double(&mut self, value: Double) -> AResult<()> {
        self.type_ids.push(EncTermField::Double.type_id());
        self.offsets.push(self.double_builder.len() as i32);
        self.double_builder.append_value(value.into());
        Ok(())
    }

    pub fn append_decimal(&mut self, value: Decimal) -> AResult<()> {
        self.type_ids.push(EncTermField::Decimal.type_id());
        self.offsets.push(self.decimal_builder.len() as i32);
        self.decimal_builder
            .append_value(i128::from_be_bytes(value.to_be_bytes()));
        Ok(())
    }

    pub fn append_duration(
        &mut self,
        year_month: Option<YearMonthDuration>,
        day_time: Option<DayTimeDuration>,
    ) -> AResult<()> {
        if year_month.is_none() && day_time.is_none() {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "One duration component required",
            )));
        }

        self.type_ids.push(EncTermField::Duration.type_id());
        self.offsets.push(self.duration_builder.len() as i32);

        let year_month_builder = self
            .duration_builder
            .field_builder::<Int64Builder>(0)
            .unwrap();
        if let Some(year_month) = year_month {
            year_month_builder.append_value(year_month.as_i64());
        } else {
            year_month_builder.append_null();
        }

        let day_time_builder = self
            .duration_builder
            .field_builder::<Decimal128Builder>(0)
            .unwrap();
        if let Some(day_time) = day_time {
            day_time_builder.append_value(day_time.as_seconds().as_i128());
        } else {
            day_time_builder.append_null();
        }

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
