use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

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
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            InternalTermRef::NumericLiteral(_) => true,
            InternalTermRef::TypedLiteral(literal) => literal.is_numeric(),
            _ => false,
        };
        Ok(result.into())
    }
}
