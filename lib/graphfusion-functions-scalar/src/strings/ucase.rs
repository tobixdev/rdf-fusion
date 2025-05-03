use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use model::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct UCaseSparqlOp;

impl Default for UCaseSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UCaseSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for UCaseSparqlOp {
    fn name(&self) -> &str {
        "ucase"
    }
}

impl UnaryTermValueOp for UCaseSparqlOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral(
            value.0.to_owned().to_uppercase(),
            value.1.map(ToOwned::to_owned),
        ))
    }
}
