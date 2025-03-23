use oxrdf::NamedNodeRef;
use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Numeric, TermRef};
use oxrdf::vocab::{rdf, xsd};

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
            TermRef::SimpleLiteral(_) => Some(xsd::STRING.as_str()),
            TermRef::NumericLiteral(value) => Some(match value {
                Numeric::Int(_) => xsd::INT.as_str(),
                Numeric::Integer(_) => xsd::INTEGER.as_str(),
                Numeric::Float(_) => xsd::FLOAT.as_str(),
                Numeric::Double(_) => xsd::DOUBLE.as_str(),
                Numeric::Decimal(_) => xsd::DECIMAL.as_str(),
            }),
            TermRef::BooleanLiteral(_) => Some(xsd::BOOLEAN.as_str()),
            TermRef::LanguageStringLiteral(_) => Some(rdf::LANG_STRING.as_str()),
            TermRef::DurationLiteral(_) => Some(xsd::DURATION.as_str()),
            TermRef::YearMonthDurationLiteral(_) => Some(xsd::YEAR_MONTH_DURATION.as_str()),
            TermRef::DayTimeDurationLiteral(_) => Some(xsd::DAY_TIME_DURATION.as_str()),
            TermRef::TypedLiteral(value) => Some(value.literal_type),
        }
        .ok_or(())?;
        Ok(NamedNodeRef::new_unchecked(datatype))
    }
}
