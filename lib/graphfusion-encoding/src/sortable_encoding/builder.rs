use crate::sortable_encoding::term_type::SortableTermType;
use crate::sortable_encoding::{SortableTerm, SortableTermField};
use datafusion::arrow::array::{
    BinaryBuilder, Float64Builder, StructArray, StructBuilder, UInt8Builder,
};
use model::{BlankNodeRef, LiteralRef, NamedNodeRef};
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
        self.append(SortableTermType::Null, None, &[])
    }

    pub fn append_boolean(&mut self, value: Boolean) {
        self.append(
            SortableTermType::Boolean,
            Some(value.into()),
            &value.to_be_bytes(),
        )
    }

    pub fn append_numeric(&mut self, value: Numeric, original_be_bytes: &[u8]) {
        let value = Double::from(value);
        self.append(SortableTermType::Numeric, Some(value), original_be_bytes)
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) {
        self.append(
            SortableTermType::BlankNodes,
            None,
            value.as_str().as_bytes(),
        )
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) {
        self.append(SortableTermType::NamedNode, None, value.as_str().as_bytes())
    }

    pub fn append_string(&mut self, value: &str) {
        self.append(SortableTermType::String, None, value.as_bytes())
    }

    pub(crate) fn append_date_time(&mut self, value: DateTime) {
        self.append(
            SortableTermType::DateTime,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub(crate) fn append_time(&mut self, value: Time) {
        self.append(
            SortableTermType::Time,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub(crate) fn append_date(&mut self, value: Date) {
        self.append(
            SortableTermType::Date,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub(crate) fn append_duration(&mut self, value: Duration) {
        self.append(
            SortableTermType::Duration,
            None, // Sort by bytes
            &value.to_be_bytes(),
        )
    }

    pub(crate) fn append_year_month_duration(&mut self, value: YearMonthDuration) {
        self.append(
            SortableTermType::YearMonthDuration,
            Some(Integer::from(value.as_i64()).into()),
            Duration::from(value).to_be_bytes().as_slice(),
        )
    }

    pub(crate) fn append_day_time_duration(&mut self, value: DayTimeDuration) {
        self.append(
            SortableTermType::DayTimeDuration,
            Some(value.as_seconds().into()),
            Duration::from(value).to_be_bytes().as_slice(),
        )
    }

    pub fn append_literal(&mut self, literal: LiteralRef<'_>) {
        self.append(SortableTermType::UnsupportedLiteral, None, literal.value().as_bytes())
    }

    fn append(&mut self, sort_type: SortableTermType, numeric: Option<Double>, bytes: &[u8]) {
        self.builder
            .field_builder::<UInt8Builder>(SortableTermField::Type.index())
            .unwrap()
            .append_value(sort_type.as_u8());

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

        self.builder.append(true)
    }

    pub fn finish(mut self) -> StructArray {
        self.builder.finish()
    }
}
