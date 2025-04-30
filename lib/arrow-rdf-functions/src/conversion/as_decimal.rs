use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Decimal, InternalTermRef, Numeric, ThinError};

#[derive(Debug)]
pub struct AsDecimalRdfOp;

impl Default for AsDecimalRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDecimalRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsDecimalRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            InternalTermRef::BooleanLiteral(v) => Decimal::from(v),
            InternalTermRef::SimpleLiteral(v) => v.value.parse()?,
            InternalTermRef::NumericLiteral(numeric) => match numeric {
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
