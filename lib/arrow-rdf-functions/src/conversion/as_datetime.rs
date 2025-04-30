use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{DateTime, InternalTermRef, ThinError};

#[derive(Debug)]
pub struct AsDateTimeRdfOp;

impl Default for AsDateTimeRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDateTimeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsDateTimeRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = DateTime;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            InternalTermRef::SimpleLiteral(v) => v.value.parse()?,
            InternalTermRef::DateTimeLiteral(v) => v,
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
