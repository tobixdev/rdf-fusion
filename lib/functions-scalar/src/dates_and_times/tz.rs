use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::{DateTime, OwnedStringLiteral};

#[derive(Debug)]
pub struct TzSparqlOp;

impl Default for TzSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TzSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for TzSparqlOp {}

impl UnarySparqlOp for TzSparqlOp {
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
