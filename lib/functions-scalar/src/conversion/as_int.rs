use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{Int, Numeric, ThinError};

#[derive(Debug)]
pub struct AsIntSparqlOp;

impl Default for AsIntSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsIntSparqlOp {
    fn name(&self) -> &str {
        "xsd:int"
    }
}

impl UnaryTermValueOp for AsIntSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Int;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => Int::from(v),
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => v,
                Numeric::Integer(v) => Int::try_from(v)?,
                Numeric::Float(v) => Int::try_from(v)?,
                Numeric::Double(v) => Int::try_from(v)?,
                Numeric::Decimal(v) => Int::try_from(v)?,
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
