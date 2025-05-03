use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use model::TermValueRef;
use model::{Boolean, Numeric, ThinError};

#[derive(Debug)]
pub struct AsBooleanSparqlOp;

impl Default for AsBooleanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsBooleanSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsBooleanSparqlOp {
    fn name(&self) -> &str {
        "xsd::boolean"
    }
}

impl UnaryTermValueOp for AsBooleanSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => v,
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Boolean::from(v),
                Numeric::Integer(v) => Boolean::from(v),
                Numeric::Float(v) => Boolean::from(v),
                Numeric::Double(v) => Boolean::from(v),
                Numeric::Decimal(v) => Boolean::from(v),
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
