use crate::value_encoding::RdfValueEncodingField;
use crate::sortable::term_type::SortableTermType;
use crate::sortable::{SortableTerm, SortableTermField};
use datafusion::arrow::array::{
    BinaryBuilder, Float64Builder, StructArray, StructBuilder, UInt8Builder,
};
use model::{BlankNodeRef, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Double, Duration, Integer, Numeric, Time,
    YearMonthDuration,
};

pub struct SortableTermBuilder {
    builder: StructBuilder,
}

impl SortableTermBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StructBuilder::from_fields(SortableTerm::fields(), capacity),
        }
    }

    pub fn append_null(&mut self) {
        self.append(
            SortableTermType::Null,
            RdfValueEncodingField::Null,
            None,
            &[],
            None,
        )
    }

    pub fn append_boolean(&mut self, value: Boolean) {
        self.append(
            SortableTermType::Boolean,
            RdfValueEncodingField::Boolean,
            Some(value.into()),
            &value.to_be_bytes(),
            None,
        )
    }

    pub fn append_numeric(&mut self, value: Numeric, original_be_bytes: &[u8]) {
        let field = match value {
            Numeric::Int(_) => RdfValueEncodingField::Int,
            Numeric::Integer(_) => RdfValueEncodingField::Integer,
            Numeric::Float(_) => RdfValueEncodingField::Float,
            Numeric::Double(_) => RdfValueEncodingField::Double,
            Numeric::Decimal(_) => RdfValueEncodingField::Decimal,
        };
        let value = Double::from(value);
        self.append(
            SortableTermType::Numeric,
            field,
            Some(value),
            original_be_bytes,
            None,
        )
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) {
        self.append(
            SortableTermType::BlankNodes,
            RdfValueEncodingField::BlankNode,
            None,
            value.as_str().as_bytes(),
            None,
        )
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) {
        self.append(
            SortableTermType::NamedNode,
            RdfValueEncodingField::NamedNode,
            None,
            value.as_str().as_bytes(),
            None,
        )
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) {
        self.append(
            SortableTermType::String,
            RdfValueEncodingField::String,
            None,
            value.as_bytes(),
            language.map(str::as_bytes),
        )
    }

    pub(crate) fn append_date_time(&mut self, value: DateTime) {
        self.append(
            SortableTermType::DateTime,
            RdfValueEncodingField::DateTime,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
            None,
        )
    }

    pub(crate) fn append_time(&mut self, value: Time) {
        self.append(
            SortableTermType::Time,
            RdfValueEncodingField::Time,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
            None,
        )
    }

    pub(crate) fn append_date(&mut self, value: Date) {
        self.append(
            SortableTermType::Date,
            RdfValueEncodingField::Date,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
            None,
        )
    }

    pub(crate) fn append_duration(&mut self, value: Duration) {
        self.append(
            SortableTermType::Duration,
            RdfValueEncodingField::Duration,
            None, // Sort by bytes
            &value.to_be_bytes(),
            None,
        )
    }

    pub(crate) fn append_year_month_duration(&mut self, value: YearMonthDuration) {
        self.append(
            SortableTermType::YearMonthDuration,
            RdfValueEncodingField::Duration,
            Some(Integer::from(value.as_i64()).into()),
            Duration::from(value).to_be_bytes().as_slice(),
            None,
        )
    }

    pub(crate) fn append_day_time_duration(&mut self, value: DayTimeDuration) {
        self.append(
            SortableTermType::DayTimeDuration,
            RdfValueEncodingField::Duration,
            Some(value.as_seconds().into()),
            Duration::from(value).to_be_bytes().as_slice(),
            None,
        )
    }

    pub fn append_literal(&mut self, value: &str, literal_type: &str) {
        self.append(
            SortableTermType::UnsupportedLiteral,
            RdfValueEncodingField::OtherTypedLiteral,
            None,
            value.as_bytes(),
            Some(literal_type.as_bytes()),
        )
    }

    fn append(
        &mut self,
        sort_type: SortableTermType,
        enc_type: RdfValueEncodingField,
        numeric: Option<Double>,
        bytes: &[u8],
        additional_bytes: Option<&[u8]>,
    ) {
        self.builder
            .field_builder::<UInt8Builder>(SortableTermField::Type.index())
            .unwrap()
            .append_value(sort_type.as_u8());
        #[allow(clippy::cast_sign_loss, reason = "EncTermType cannot be negative.")]
        self.builder
            .field_builder::<UInt8Builder>(SortableTermField::RdfTermType.index())
            .unwrap()
            .append_value(enc_type.type_id() as u8);

        let numeric_builder = self
            .builder
            .field_builder::<Float64Builder>(SortableTermField::Numeric.index())
            .unwrap();
        match numeric {
            None => numeric_builder.append_null(),
            Some(numeric) => numeric_builder.append_value(numeric.into()),
        }

        let bytes_builder = self
            .builder
            .field_builder::<BinaryBuilder>(SortableTermField::Bytes.index())
            .unwrap();
        bytes_builder.append_value(bytes);

        let additional_bytes_builder = self
            .builder
            .field_builder::<BinaryBuilder>(SortableTermField::AdditionalBytes.index())
            .unwrap();
        if let Some(additional_bytes) = additional_bytes {
            additional_bytes_builder.append_value(additional_bytes);
        } else {
            additional_bytes_builder.append_null();
        }

        self.builder.append(true)
    }

    pub fn finish(mut self) -> StructArray {
        self.builder.finish()
    }
}
