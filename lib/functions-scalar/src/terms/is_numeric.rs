use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Boolean, TermRef};

#[derive(Debug)]
pub struct IsNumericRdfOp {}

impl IsNumericRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for IsNumericRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = match value {
            TermRef::Numeric(_) => true,
            TermRef::TypedLiteral(literal) => literal.is_numeric(),
            _ => false,
        };
        Ok(result.into())
    }
}
