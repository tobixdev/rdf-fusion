use crate::encoded::EncTermField;
use crate::struct_encoded::{StructEncTerm, StructEncTermField};
use datafusion::arrow::array::{
    Float64Builder, StringBuilder, StructArray, StructBuilder, UInt8Builder,
};
use datamodel::{Boolean, Date, DateTime, DayTimeDuration, Double, Duration, Integer, Numeric, Time, YearMonthDuration};
use oxrdf::{BlankNodeRef, NamedNodeRef};

enum StructEncTermType {
    Null,
    BlankNodes,
    NamedNode,
    Boolean,
    Numeric,
    String,
    DateTime,
    Time,
    Date,
    Duration,
    YearMonthDuration,
    DayTimeDuration,
    UnsupportedLiteral,
}

impl StructEncTermType {
    pub fn as_u8(&self) -> u8 {
        match self {
            StructEncTermType::Null => 0,
            StructEncTermType::BlankNodes => 1,
            StructEncTermType::NamedNode => 2,
            StructEncTermType::Boolean => 3,
            StructEncTermType::Numeric => 4,
            StructEncTermType::String => 5,
            StructEncTermType::DateTime => 6,
            StructEncTermType::Time => 7,
            StructEncTermType::Date => 8,
            StructEncTermType::Duration => 9,
            StructEncTermType::YearMonthDuration => 10,
            StructEncTermType::DayTimeDuration => 11,
            StructEncTermType::UnsupportedLiteral => 12,
        }
    }
}

pub struct StructEncTermBuilder {
    builder: StructBuilder,
}

impl StructEncTermBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StructBuilder::from_fields(StructEncTerm::fields(), capacity),
        }
    }

    pub fn append_null(&mut self) {
        self.append(StructEncTermType::Null, EncTermField::Null, None, None)
    }

    pub fn append_boolean(&mut self, value: Boolean) {
        self.append(StructEncTermType::Boolean, EncTermField::Boolean, Some(value.into()), None)
    }

    pub fn append_numeric(&mut self, value: Numeric) {
        let field = match value {
            Numeric::Int(_) => EncTermField::Int,
            Numeric::Integer(_) => EncTermField::Integer,
            Numeric::Float(_) => EncTermField::Float,
            Numeric::Double(_) => EncTermField::Double,
            Numeric::Decimal(_) => EncTermField::Decimal
        };
        let value = Double::from(value);
        self.append(StructEncTermType::Numeric, field, Some(value), None)
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) {
        self.append(StructEncTermType::BlankNodes, EncTermField::BlankNode, None, Some(value.as_str()))
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) {
        self.append(StructEncTermType::NamedNode, EncTermField::NamedNode, None, Some(value.as_str()))
    }

    pub fn append_string(&mut self, value: &str) {
        self.append(StructEncTermType::String, EncTermField::String, None, Some(value))
    }

    pub(crate) fn append_date_time(&mut self, value: DateTime) {
        self.append(
            StructEncTermType::DateTime,
            EncTermField::DateTime,
            Some(value.timestamp().value().into()),
            None,
        )
    }

    pub(crate) fn append_time(&mut self, value: Time) {
        self.append(
            StructEncTermType::Time,
            EncTermField::Time,
            Some(value.timestamp().value().into()),
            None,
        )
    }

    pub(crate) fn append_date(&mut self, value: Date) {
        self.append(
            StructEncTermType::Date,
            EncTermField::Date,
            Some(value.timestamp().value().into()),
            None,
        )
    }

    pub(crate) fn append_duration(&mut self, value: Duration) {
        self.append(
            StructEncTermType::Duration,
            EncTermField::Duration,
            Some(Integer::from(value.all_months()).into()),
            Some(&value.seconds().to_string()),
        )
    }

    pub(crate) fn append_year_month_duration(&mut self, value: YearMonthDuration) {
        self.append(
            StructEncTermType::YearMonthDuration,
            EncTermField::Duration,
            Some(Integer::from(value.as_i64()).into()),
            None,
        )
    }

    pub(crate) fn append_day_time_duration(&mut self, value: DayTimeDuration) {
        self.append(
            StructEncTermType::DayTimeDuration,
            EncTermField::Duration,
            Some(value.as_seconds().into()),
            None,
        )
    }

    pub fn append_literal(&mut self, value: &str) {
        self.append(
            StructEncTermType::UnsupportedLiteral,
            EncTermField::TypedLiteral,
            None,
            Some(value),
        )
    }

    fn append(
        &mut self,
        sort_type: StructEncTermType,
        enc_type: EncTermField,
        numeric: Option<Double>,
        string: Option<&str>,
    ) {
        self.builder
            .field_builder::<UInt8Builder>(StructEncTermField::Type.index())
            .unwrap()
            .append_value(sort_type.as_u8());
        self.builder
            .field_builder::<UInt8Builder>(StructEncTermField::EncTermType.index())
            .unwrap()
            .append_value(enc_type.type_id() as u8);

        let numeric_builder = self
            .builder
            .field_builder::<Float64Builder>(StructEncTermField::Numeric.index())
            .unwrap();
        match numeric {
            None => numeric_builder.append_null(),
            Some(numeric) => numeric_builder.append_value(numeric.into()),
        }

        let string_builder = self
            .builder
            .field_builder::<StringBuilder>(StructEncTermField::String.index())
            .unwrap();
        match string {
            None => string_builder.append_null(),
            Some(string) => string_builder.append_value(string),
        }

        self.builder.append(true)
    }

    pub fn finish(mut self) -> StructArray {
        self.builder.finish()
    }
}
