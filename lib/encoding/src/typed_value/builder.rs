use crate::error::LiteralEncodingError;
use crate::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use crate::AResult;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, NullBuilder, StringBuilder, StructBuilder,
    UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::error::ArrowError;
use graphfusion_model::vocab::{rdf, xsd};
use graphfusion_model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Duration, LiteralRef, NamedNodeRef,
    Numeric, TermRef, Time, Timestamp, YearMonthDuration,
};
use graphfusion_model::{Decimal, Double, Float, Int, Integer};
use std::sync::Arc;

pub struct TypedValueArrayBuilder {
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
    date_time_builder: StructBuilder,
    time_builder: StructBuilder,
    date_builder: StructBuilder,
    duration_builder: StructBuilder,
    typed_literal_builder: StructBuilder,
    null_builder: NullBuilder,
}

impl Default for TypedValueArrayBuilder {
    fn default() -> Self {
        Self {
            type_ids: Vec::new(),
            offsets: Vec::new(),
            named_node_builder: StringBuilder::with_capacity(0, 0),
            blank_node_builder: StringBuilder::with_capacity(0, 0),
            string_builder: StructBuilder::from_fields(TypedValueEncoding::string_fields(), 0),
            boolean_builder: BooleanBuilder::with_capacity(0),
            float_builder: Float32Builder::with_capacity(0),
            double_builder: Float64Builder::with_capacity(0),
            #[allow(clippy::expect_used, reason = "PRECISION and SCALE are fixed")]
            decimal_builder: Decimal128Builder::with_capacity(0)
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .expect("PRECISION and SCALE fixed"),
            int32_builder: Int32Builder::with_capacity(0),
            integer_builder: Int64Builder::with_capacity(0),
            date_time_builder: StructBuilder::from_fields(
                TypedValueEncoding::timestamp_fields(),
                0,
            ),
            time_builder: StructBuilder::from_fields(TypedValueEncoding::timestamp_fields(), 0),
            date_builder: StructBuilder::from_fields(TypedValueEncoding::timestamp_fields(), 0),
            duration_builder: StructBuilder::from_fields(TypedValueEncoding::duration_fields(), 0),
            typed_literal_builder: StructBuilder::from_fields(
                TypedValueEncoding::typed_literal_fields(),
                0,
            ),
            null_builder: NullBuilder::new(),
        }
    }
}

impl TypedValueArrayBuilder {
    pub fn append_term(&mut self, term: TermRef<'_>) -> Result<(), ArrowError> {
        match term {
            TermRef::NamedNode(nn) => self.append_named_node(nn)?,
            TermRef::BlankNode(bnode) => self.append_blank_node(bnode)?,
            TermRef::Literal(literal) => match self.append_literal(literal) {
                Ok(()) => (),
                Err(LiteralEncodingError::Arrow(error)) => return Err(error),
                Err(LiteralEncodingError::ParsingError(_)) => self.append_null()?,
            },
        };
        Ok(())
    }

    pub fn append_literal(&mut self, literal: LiteralRef<'_>) -> Result<(), LiteralEncodingError> {
        match literal.datatype() {
            // TODO: Other literals
            xsd::BOOLEAN => self.append_boolean(literal.value().parse()?)?,
            xsd::FLOAT => self.append_float(literal.value().parse()?)?,
            xsd::DOUBLE => self.append_double(literal.value().parse()?)?,
            xsd::DECIMAL => self.append_decimal(literal.value().parse()?)?,
            xsd::INTEGER => self.append_integer(literal.value().parse()?)?,
            xsd::INT => self.append_int(literal.value().parse()?)?,
            xsd::DURATION => {
                let duration: Duration = literal.value().parse()?;
                self.append_duration(Some(duration.year_month()), Some(duration.day_time()))?
            }
            xsd::YEAR_MONTH_DURATION => {
                let duration: YearMonthDuration = literal.value().parse()?;
                self.append_duration(Some(duration), None)?
            }
            xsd::DURATION => {
                let duration: DayTimeDuration = literal.value().parse()?;
                self.append_duration(None, Some(duration))?
            }
            xsd::DATE_TIME => self.append_date_time(literal.value().parse()?)?,
            xsd::TIME => self.append_time(literal.value().parse()?)?,
            xsd::DATE => self.append_date(literal.value().parse()?)?,
            rdf::LANG_STRING | xsd::STRING => {
                self.append_string(literal.value(), literal.language())?
            }
            _ => self.append_other_literal(literal)?,
        };
        Ok(())
    }

    pub fn append_boolean(&mut self, value: Boolean) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::Boolean,
            self.boolean_builder.len(),
        )?;
        self.boolean_builder.append_value(value.as_bool());
        Ok(())
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::NamedNode,
            self.named_node_builder.len(),
        )?;
        self.named_node_builder.append_value(value.as_str());
        Ok(())
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::BlankNode,
            self.blank_node_builder.len(),
        )?;
        self.blank_node_builder.append_value(value.as_str());
        Ok(())
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        if language == Some("") {
            return Err(ArrowError::InvalidArgumentError(String::from(
                "Empty language.",
            )));
        }

        self.append_type_id_and_offset(TypedValueEncodingField::String, self.string_builder.len())?;

        self.string_builder
            .field_builder::<StringBuilder>(0)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?
            .append_value(value);

        let language_builder = self
            .string_builder
            .field_builder::<StringBuilder>(1)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?;
        if let Some(language) = language {
            language_builder.append_value(language);
        } else {
            language_builder.append_null();
        }
        self.string_builder.append(true);

        Ok(())
    }

    pub fn append_date_time(&mut self, value: DateTime) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::DateTime,
            self.date_time_builder.len(),
        )?;
        append_timestamp(&mut self.date_time_builder, value.timestamp());
        Ok(())
    }

    pub fn append_time(&mut self, value: Time) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Time, self.time_builder.len())?;
        append_timestamp(&mut self.time_builder, value.timestamp());
        Ok(())
    }

    pub fn append_date(&mut self, value: Date) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Date, self.date_builder.len())?;
        append_timestamp(&mut self.date_builder, value.timestamp());
        Ok(())
    }

    pub fn append_int(&mut self, int: Int) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Int, self.int32_builder.len())?;
        self.int32_builder.append_value(int.into());
        Ok(())
    }

    pub fn append_integer(&mut self, integer: Integer) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::Integer,
            self.integer_builder.len(),
        )?;
        self.integer_builder.append_value(integer.into());
        Ok(())
    }

    pub fn append_float(&mut self, value: Float) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Float, self.float_builder.len())?;
        self.float_builder.append_value(value.into());
        Ok(())
    }

    pub fn append_double(&mut self, value: Double) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Double, self.double_builder.len())?;
        self.double_builder.append_value(value.into());
        Ok(())
    }

    pub fn append_decimal(&mut self, value: Decimal) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::Decimal,
            self.decimal_builder.len(),
        )?;
        self.decimal_builder
            .append_value(i128::from_be_bytes(value.to_be_bytes()));
        Ok(())
    }

    pub fn append_numeric(&mut self, value: Numeric) -> AResult<()> {
        match value {
            Numeric::Int(value) => self.append_int(value),
            Numeric::Integer(value) => self.append_integer(value),
            Numeric::Float(value) => self.append_float(value),
            Numeric::Double(value) => self.append_double(value),
            Numeric::Decimal(value) => self.append_decimal(value),
        }
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

        self.append_type_id_and_offset(
            TypedValueEncodingField::Duration,
            self.duration_builder.len(),
        )?;
        let year_month_builder = self
            .duration_builder
            .field_builder::<Int64Builder>(0)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?;
        if let Some(year_month) = year_month {
            year_month_builder.append_value(year_month.as_i64());
        } else {
            year_month_builder.append_null();
        }

        let day_time_builder = self
            .duration_builder
            .field_builder::<Decimal128Builder>(1)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?;
        if let Some(day_time) = day_time {
            let day_time = i128::from_be_bytes(day_time.as_seconds().to_be_bytes());
            day_time_builder.append_value(day_time);
        } else {
            day_time_builder.append_null();
        }

        self.duration_builder.append(true);

        Ok(())
    }

    /// Appends a `literal` that is encoded in the [TypedValueEncodingField::OtherLiteral].
    ///
    /// *CAVEAT*: Only call this function if you're positive that there is no specialized encoding
    /// for the data type of the `literal`. Otherwise, call [Self::append_literal].
    pub fn append_other_literal(&mut self, literal: LiteralRef<'_>) -> AResult<()> {
        self.append_type_id_and_offset(
            TypedValueEncodingField::OtherLiteral,
            self.typed_literal_builder.len(),
        )?;
        self.typed_literal_builder
            .field_builder::<StringBuilder>(0)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?
            .append_value(literal.value());
        self.typed_literal_builder
            .field_builder::<StringBuilder>(1)
            .ok_or(ArrowError::ComputeError(
                "Invalid builder access".to_owned(),
            ))?
            .append_value(literal.datatype().as_str());
        self.typed_literal_builder.append(true);
        Ok(())
    }

    pub fn append_null(&mut self) -> AResult<()> {
        self.append_type_id_and_offset(TypedValueEncodingField::Null, self.null_builder.len())?;
        self.null_builder.append_null();
        Ok(())
    }

    pub fn finish(mut self) -> ArrayRef {
        Arc::new(
            UnionArray::try_new(
                TypedValueEncoding::fields(),
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
                    Arc::new(self.date_time_builder.finish()),
                    Arc::new(self.time_builder.finish()),
                    Arc::new(self.date_builder.finish()),
                    Arc::new(self.duration_builder.finish()),
                    Arc::new(self.typed_literal_builder.finish()),
                ],
            )
            .expect("Fields and type match"),
        )
    }

    fn append_type_id_and_offset(
        &mut self,
        field: TypedValueEncodingField,
        offset: usize,
    ) -> AResult<()> {
        let offset = i32::try_from(offset).map_err(|_| {
            ArrowError::ArithmeticOverflow("Field {} of union got too large.".to_owned())
        })?;
        self.type_ids.push(field.type_id());
        self.offsets.push(offset);
        Ok(())
    }
}

fn append_timestamp(builder: &mut StructBuilder, value: Timestamp) {
    builder
        .field_builder::<Decimal128Builder>(0)
        .unwrap()
        .append_value(i128::from_be_bytes(value.value().to_be_bytes()));

    let offset_builder = builder.field_builder::<Int16Builder>(1).unwrap();
    if let Some(offset) = value.offset() {
        offset_builder.append_value(offset.in_minutes());
    } else {
        offset_builder.append_null();
    }
    builder.append(true);
}
