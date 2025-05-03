use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use model::TermValueRef;
use model::{Decimal, Numeric, ThinError};

#[derive(Debug)]
pub struct AsDecimalSparqlOp;

impl Default for AsDecimalSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDecimalSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsDecimalSparqlOp {
    fn name(&self) -> &str {
        "xsd:decimal"
    }
}

impl UnaryTermValueOp for AsDecimalSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::BooleanLiteral(v) => Decimal::from(v),
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Decimal::from(v),
                Numeric::Integer(v) => Decimal::from(v),
                Numeric::Float(v) => Decimal::try_from(v)?,
                Numeric::Double(v) => Decimal::try_from(v)?,
                Numeric::Decimal(v) => v,
            },
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
