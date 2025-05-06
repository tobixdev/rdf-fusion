use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{Double, Numeric, ThinError};

#[derive(Debug)]
pub struct AsDoubleSparqlOp;

impl Default for AsDoubleSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDoubleSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsDoubleSparqlOp {
    fn name(&self) -> &str {
        "xsd:double"
    }
}

impl UnaryTermValueOp for AsDoubleSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Double;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => Double::from(v),
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Double::from(v),
                Numeric::Integer(v) => Double::from(v),
                Numeric::Float(v) => Double::from(v),
                Numeric::Double(v) => v,
                Numeric::Decimal(v) => Double::from(v),
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
