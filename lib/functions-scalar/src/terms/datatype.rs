use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Numeric, TermRef, StringLiteralRef};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNodeRef;

#[derive(Debug)]
pub struct DatatypeRdfOp {}

impl DatatypeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for DatatypeRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let datatype = match value {
            TermRef::NamedNode(_) => None,
            TermRef::BlankNode(_) => None,
            TermRef::SimpleLiteral(_) => Some(xsd::STRING),
            TermRef::NumericLiteral(value) => Some(match value {
                Numeric::Int(_) => xsd::INT,
                Numeric::Integer(_) => xsd::INTEGER,
                Numeric::Float(_) => xsd::FLOAT,
                Numeric::Double(_) => xsd::DOUBLE,
                Numeric::Decimal(_) => xsd::DECIMAL,
            }),
            TermRef::BooleanLiteral(_) => Some(xsd::BOOLEAN),
            TermRef::LanguageStringLiteral(_) => Some(rdf::LANG_STRING),
            TermRef::DateTimeLiteral(_) => Some(xsd::DATE_TIME),
            TermRef::TimeLiteral(_) => Some(xsd::TIME),
            TermRef::DateLiteral(_) => Some(xsd::DATE),
            TermRef::DurationLiteral(_) => Some(xsd::DURATION),
            TermRef::YearMonthDurationLiteral(_) => Some(xsd::YEAR_MONTH_DURATION),
            TermRef::DayTimeDurationLiteral(_) => Some(xsd::DAY_TIME_DURATION),
            TermRef::TypedLiteral(value) => Some(NamedNodeRef::new_unchecked(value.literal_type)),
        }
        .ok_or(())?;
        Ok(datatype)
    }
}
