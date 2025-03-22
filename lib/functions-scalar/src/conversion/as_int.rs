use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Int, Numeric, TermRef};

#[derive(Debug)]
pub struct AsIntRdfOp {}

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
            TermRef::Boolean(v) => Int::from(v),
            TermRef::SimpleLiteral(v) => v.value.parse().map_err(|_| ())?,
            TermRef::Numeric(numeric) => match numeric {
                Numeric::Int(v) => v,
                Numeric::Integer(v) => Int::try_from(v).map_err(|_| ())?,
                Numeric::Float(v) => Int::try_from(v).map_err(|_| ())?,
                Numeric::Double(v) => Int::try_from(v).map_err(|_| ())?,
                Numeric::Decimal(v) => Int::try_from(v).map_err(|_| ())?,
            },
            _ => return Err(()),
        };
        Ok(converted)
    }
}
