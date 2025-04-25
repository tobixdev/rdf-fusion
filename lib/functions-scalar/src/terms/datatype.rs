use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{Numeric, TermRef, ThinError};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNodeRef;

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
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let datatype = match value {
            TermRef::BlankNode(_) | TermRef::NamedNode(_) => return ThinError::expected(),
            TermRef::SimpleLiteral(_) => xsd::STRING,
            TermRef::NumericLiteral(value) => match value {
                Numeric::Int(_) => xsd::INT,
                Numeric::Integer(_) => xsd::INTEGER,
                Numeric::Float(_) => xsd::FLOAT,
                Numeric::Double(_) => xsd::DOUBLE,
                Numeric::Decimal(_) => xsd::DECIMAL,
            },
            TermRef::BooleanLiteral(_) => xsd::BOOLEAN,
            TermRef::LanguageStringLiteral(_) => rdf::LANG_STRING,
            TermRef::DateTimeLiteral(_) => xsd::DATE_TIME,
            TermRef::TimeLiteral(_) => xsd::TIME,
            TermRef::DateLiteral(_) => xsd::DATE,
            TermRef::DurationLiteral(_) => xsd::DURATION,
            TermRef::YearMonthDurationLiteral(_) => xsd::YEAR_MONTH_DURATION,
            TermRef::DayTimeDurationLiteral(_) => xsd::DAY_TIME_DURATION,
            TermRef::TypedLiteral(value) => NamedNodeRef::new_unchecked(value.literal_type),
        };
        Ok(datatype)
    }
}
