use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Integer, Numeric, TermRef};

#[derive(Debug)]
pub struct AsIntegerRdfOp {}

impl AsIntegerRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsIntegerRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::BooleanLiteral(v) => Integer::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse().map_err(|_| ())?,
            TermRef::NumericLiteral(numeric) => match numeric {
                Numeric::Int(v) => Integer::from(v),
                Numeric::Integer(v) => v,
                Numeric::Float(v) => Integer::try_from(v).map_err(|_| ())?,
                Numeric::Double(v) => Integer::try_from(v).map_err(|_| ())?,
                Numeric::Decimal(v) => Integer::try_from(v).map_err(|_| ())?,
            },
            _ => return Err(()),
        };
        Ok(converted)
    }
}
