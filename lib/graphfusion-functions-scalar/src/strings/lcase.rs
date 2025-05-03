use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use model::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct LCaseSparqlOp;

impl Default for LCaseSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LCaseSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for LCaseSparqlOp {
    fn name(&self) -> &str {
        "lcase"
    }
}

impl UnaryTermValueOp for LCaseSparqlOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral(
            value.0.to_owned().to_lowercase(),
            value.1.map(String::from),
        ))
    }
}
