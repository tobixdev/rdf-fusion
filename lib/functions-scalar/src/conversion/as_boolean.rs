use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Boolean, Numeric, RdfOpError, TermRef};

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

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => v,
            TermRef::SimpleLiteral(v) => v.value.parse().map_err(|_| ())?,
            TermRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Boolean::from(v),
                Numeric::Integer(v) => Boolean::from(v),
                Numeric::Float(v) => Boolean::from(v),
                Numeric::Double(v) => Boolean::from(v),
                Numeric::Decimal(v) => Boolean::from(v),
            },
            _ => return Err(RdfOpError),
        };
        Ok(converted)
    }
}
