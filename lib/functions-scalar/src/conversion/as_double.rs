use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{Double, Numeric, TermRef, ThinError};

#[derive(Debug)]
pub struct AsDoubleRdfOp;

impl Default for AsDoubleRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDoubleRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsDoubleRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Double;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => Double::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse()?,
            TermRef::NumericLiteral(numeric) => match numeric {
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
