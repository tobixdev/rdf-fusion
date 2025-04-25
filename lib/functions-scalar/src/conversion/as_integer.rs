use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{Integer, Numeric, TermRef, ThinError};

#[derive(Debug)]
pub struct AsIntegerRdfOp;

impl Default for AsIntegerRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsIntegerRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsIntegerRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => Integer::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse()?,
            TermRef::NumericLiteral(numeric) => match numeric {
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
