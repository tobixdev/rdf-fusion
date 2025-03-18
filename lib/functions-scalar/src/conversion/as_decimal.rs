use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Decimal, Numeric, TermRef};

#[derive(Debug)]
pub struct AsDecimalRdfOp {}

impl AsDecimalRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsDecimalRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::Boolean(v) => Decimal::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse().map_err(|_| ())?,
            TermRef::Numeric(numeric) => match numeric {
                Numeric::Int(v) => Decimal::from(v),
                Numeric::Integer(v) => Decimal::from(v),
                Numeric::Float(v) => Decimal::try_from(v).map_err(|_| ())?,
                Numeric::Double(v) => Decimal::try_from(v).map_err(|_| ())?,
                Numeric::Decimal(v) => v,
            },
            _ => return Err(()),
        };
        Ok(converted)
    }
}
