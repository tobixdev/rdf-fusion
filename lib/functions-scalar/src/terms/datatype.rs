use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{NamedNodeRef, Numeric, InternalTermRef, ThinError};
use model::vocab::{rdf, xsd};

#[derive(Debug)]
pub struct DatatypeRdfOp;

impl Default for DatatypeRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DatatypeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for DatatypeRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let datatype = match value {
            InternalTermRef::BlankNode(_) | InternalTermRef::NamedNode(_) => return ThinError::expected(),
            InternalTermRef::SimpleLiteral(_) => xsd::STRING,
            InternalTermRef::NumericLiteral(value) => match value {
                Numeric::Int(_) => xsd::INT,
                Numeric::Integer(_) => xsd::INTEGER,
                Numeric::Float(_) => xsd::FLOAT,
                Numeric::Double(_) => xsd::DOUBLE,
                Numeric::Decimal(_) => xsd::DECIMAL,
            },
            InternalTermRef::BooleanLiteral(_) => xsd::BOOLEAN,
            InternalTermRef::LanguageStringLiteral(_) => rdf::LANG_STRING,
            InternalTermRef::DateTimeLiteral(_) => xsd::DATE_TIME,
            InternalTermRef::TimeLiteral(_) => xsd::TIME,
            InternalTermRef::DateLiteral(_) => xsd::DATE,
            InternalTermRef::DurationLiteral(_) => xsd::DURATION,
            InternalTermRef::YearMonthDurationLiteral(_) => xsd::YEAR_MONTH_DURATION,
            InternalTermRef::DayTimeDurationLiteral(_) => xsd::DAY_TIME_DURATION,
            InternalTermRef::TypedLiteral(value) => NamedNodeRef::new_unchecked(value.literal_type),
        };
        Ok(datatype)
    }
}
