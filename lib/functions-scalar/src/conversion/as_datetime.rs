use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use graphfusion_model::{DateTime, ThinError};
use graphfusion_model::TermValueRef;

#[derive(Debug)]
pub struct AsDateTimeSparqlOp;

impl Default for AsDateTimeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDateTimeSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsDateTimeSparqlOp {
    fn name(&self) -> &str {
        "xsd:dateTime"
    }
}

impl UnaryTermValueOp for AsDateTimeSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = DateTime;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::SimpleLiteral(v) => v.value.parse()?,
            TermValueRef::DateTimeLiteral(v) => v,
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
