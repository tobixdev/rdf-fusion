use crate::{ScalarNAryRdfOp, ThinResult};
use datamodel::{TermRef, ThinError};

#[derive(Debug)]
pub struct CoalesceRdfOp;

impl Default for CoalesceRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNAryRdfOp for CoalesceRdfOp {
    type Args<'data> = TermRef<'data>;
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>> {
        args.first().copied().ok_or(ThinError::Expected)
    }

    fn evaluate_error<'data>(
        &self,
        args: &[ThinResult<Self::Args<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        args.iter()
            .find_map(|arg| arg.ok())
            .ok_or(ThinError::Expected)
    }
}
