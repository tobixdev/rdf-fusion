use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{Integer, Numeric, ThinError};

#[derive(Debug)]
pub struct AsIntegerSparqlOp;

impl Default for AsIntegerSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntegerSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsIntegerSparqlOp {
    fn name(&self) -> &str {
        "xsd:integer"
    }
}

impl UnaryTermValueOp for AsIntegerSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => Integer::from(v),
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Integer::from(v),
                Numeric::Integer(v) => v,
                Numeric::Float(v) => Integer::try_from(v)?,
                Numeric::Double(v) => Integer::try_from(v)?,
                Numeric::Decimal(v) => Integer::try_from(v)?,
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
