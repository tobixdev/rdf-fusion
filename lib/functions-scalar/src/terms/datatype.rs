use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::vocab::{rdf, xsd};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{NamedNodeRef, Numeric, ThinError};

#[derive(Debug)]
pub struct DatatypeSparqlOp;

impl Default for DatatypeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DatatypeSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for DatatypeSparqlOp {}

impl UnarySparqlOp for DatatypeSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let datatype = match value {
            TypedValueRef::BlankNode(_) | TypedValueRef::NamedNode(_) => {
                return ThinError::expected()
            }
            TypedValueRef::SimpleLiteral(_) => xsd::STRING,
            TypedValueRef::NumericLiteral(value) => match value {
                Numeric::Int(_) => xsd::INT,
                Numeric::Integer(_) => xsd::INTEGER,
                Numeric::Float(_) => xsd::FLOAT,
                Numeric::Double(_) => xsd::DOUBLE,
                Numeric::Decimal(_) => xsd::DECIMAL,
            },
            TypedValueRef::BooleanLiteral(_) => xsd::BOOLEAN,
            TypedValueRef::LanguageStringLiteral(_) => rdf::LANG_STRING,
            TypedValueRef::DateTimeLiteral(_) => xsd::DATE_TIME,
            TypedValueRef::TimeLiteral(_) => xsd::TIME,
            TypedValueRef::DateLiteral(_) => xsd::DATE,
            TypedValueRef::DurationLiteral(_) => xsd::DURATION,
            TypedValueRef::YearMonthDurationLiteral(_) => xsd::YEAR_MONTH_DURATION,
            TypedValueRef::DayTimeDurationLiteral(_) => xsd::DAY_TIME_DURATION,
            TypedValueRef::OtherLiteral(value) => value.datatype(),
        };
        Ok(datatype)
    }
}
