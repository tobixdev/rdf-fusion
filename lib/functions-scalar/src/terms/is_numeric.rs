use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, TermRef};

#[derive(Debug)]
pub struct IsNumericRdfOp;

impl Default for IsNumericRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNumericRdfOp {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarUnaryRdfOp for IsNumericRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermRef::NumericLiteral(_) => true,
            TermRef::TypedLiteral(literal) => literal.is_numeric(),
            _ => false,
        };
        Ok(result.into())
    }
}
