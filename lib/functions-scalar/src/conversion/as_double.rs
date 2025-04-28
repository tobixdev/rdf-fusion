use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Double, InternalTermRef, Numeric, ThinError};

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
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Double;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            InternalTermRef::BooleanLiteral(v) => Double::from(v),
            InternalTermRef::SimpleLiteral(v) => v.value.parse()?,
            InternalTermRef::NumericLiteral(numeric) => match numeric {
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
