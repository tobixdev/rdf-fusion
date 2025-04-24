use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Int, Numeric, RdfOpError, TermRef};

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
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Int;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => Int::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse()?,
            TermRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => v,
                Numeric::Integer(v) => Int::try_from(v)?,
                Numeric::Float(v) => Int::try_from(v)?,
                Numeric::Double(v) => Int::try_from(v)?,
                Numeric::Decimal(v) => Int::try_from(v)?,
            },
            _ => return Err(RdfOpError),
        };
        Ok(converted)
    }
}
