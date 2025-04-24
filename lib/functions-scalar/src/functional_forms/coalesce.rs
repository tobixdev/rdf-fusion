use crate::{RdfOpResult, ScalarNAryRdfOp};
use datamodel::{RdfOpError, TermRef};

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

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> RdfOpResult<Self::Result<'data>> {
        args.first().copied().ok_or(RdfOpError)
    }

    fn evaluate_error<'data>(
        &self,
        args: &[RdfOpResult<Self::Args<'data>>],
    ) -> RdfOpResult<Self::Result<'data>> {
        args.iter()
            .find_map(|arg| arg.ok())
            .ok_or(RdfOpError)
    }
}
