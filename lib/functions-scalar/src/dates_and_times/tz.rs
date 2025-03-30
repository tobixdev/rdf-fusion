use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, OwnedStringLiteral};

#[derive(Debug)]
pub struct TzRdfOp {}

impl TzRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for TzRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = value
            .timezone_offset()
            .map(|offset| offset.to_string())
            .unwrap_or(String::new());
        Ok(OwnedStringLiteral::new(result, None))
    }
}
