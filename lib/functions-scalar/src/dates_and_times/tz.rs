use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{DateTime, OwnedStringLiteral};

#[derive(Debug)]
pub struct TzRdfOp;

impl Default for TzRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TzRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for TzRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = value
            .timezone_offset()
            .map(|offset| offset.to_string())
            .unwrap_or_default();
        Ok(OwnedStringLiteral::new(result, None))
    }
}
