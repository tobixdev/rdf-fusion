use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Int, InternalTermRef, Numeric, ThinError};

#[derive(Debug)]
pub struct AsIntRdfOp;

impl Default for AsIntRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsIntRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Int;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            InternalTermRef::BooleanLiteral(v) => Int::from(v),
            InternalTermRef::SimpleLiteral(v) => v.value.parse()?,
            InternalTermRef::NumericLiteral(numeric) => match numeric {
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
