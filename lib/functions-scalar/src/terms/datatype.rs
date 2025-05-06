use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::vocab::{rdf, xsd};
use graphfusion_model::{NamedNodeRef, Numeric, TermRef, ThinError};

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

impl SparqlOp for DatatypeSparqlOp {
    fn name(&self) -> &str {
        "datatype"
    }
}

impl UnaryTermValueOp for DatatypeSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let datatype = match value {
            TermValueRef::BlankNode(_) | TermValueRef::NamedNode(_) => return ThinError::expected(),
            TermValueRef::SimpleLiteral(_) => xsd::STRING,
            TermValueRef::NumericLiteral(value) => match value {
                Numeric::Int(_) => xsd::INT,
                Numeric::Integer(_) => xsd::INTEGER,
                Numeric::Float(_) => xsd::FLOAT,
                Numeric::Double(_) => xsd::DOUBLE,
                Numeric::Decimal(_) => xsd::DECIMAL,
            },
            TermValueRef::BooleanLiteral(_) => xsd::BOOLEAN,
            TermValueRef::LanguageStringLiteral(_) => rdf::LANG_STRING,
            TermValueRef::DateTimeLiteral(_) => xsd::DATE_TIME,
            TermValueRef::TimeLiteral(_) => xsd::TIME,
            TermValueRef::DateLiteral(_) => xsd::DATE,
            TermValueRef::DurationLiteral(_) => xsd::DURATION,
            TermValueRef::YearMonthDurationLiteral(_) => xsd::YEAR_MONTH_DURATION,
            TermValueRef::DayTimeDurationLiteral(_) => xsd::DAY_TIME_DURATION,
            TermValueRef::OtherLiteral(value) => value.datatype(),
        };
        Ok(datatype)
    }
}

impl UnaryRdfTermOp for DatatypeSparqlOp {
    type Result<'data> = NamedNodeRef<'data>;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let datatype = match value {
            TermRef::BlankNode(_) | TermRef::NamedNode(_) => return ThinError::expected(),
            TermRef::Literal(lit) => lit.datatype(),
        };
        Ok(datatype)
    }
}
