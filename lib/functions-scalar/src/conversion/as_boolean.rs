use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, Numeric, TermRef, ThinError};

#[derive(Debug)]
pub struct AsBooleanRdfOp;

impl Default for AsBooleanRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsBooleanRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsBooleanRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => v,
            TermRef::SimpleLiteral(v) => v.value.parse()?,
            TermRef::NumericLiteral(numeric) => match numeric {
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
