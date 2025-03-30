use crate::{RdfOpResult, ScalarNAryRdfOp};
use datamodel::TermRef;

#[derive(Debug)]
pub struct CoalesceRdfOp {}

impl CoalesceRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNAryRdfOp for CoalesceRdfOp {
    type Args<'data> = TermRef<'data>;
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> RdfOpResult<Self::Result<'data>> {
        args.first().ok_or(()).copied()
    }

    fn evaluate_error<'data>(
        &self,
        args: &[RdfOpResult<Self::Args<'data>>],
    ) -> RdfOpResult<Self::Result<'data>> {
        args.iter()
            .find(|arg| arg.is_ok())
            .map(|arg| arg.unwrap())
            .ok_or(())
    }
}
