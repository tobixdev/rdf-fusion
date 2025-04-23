use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, RdfOpError, TermRef};

#[derive(Debug)]
pub struct AsDateTimeRdfOp {}

impl AsDateTimeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsDateTimeRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = DateTime;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::SimpleLiteral(v) => v.value.parse().map_err(|_| ())?,
            TermRef::DateTimeLiteral(v) => v,
            _ => return Err(RdfOpError),
        };
        Ok(converted)
    }
}
