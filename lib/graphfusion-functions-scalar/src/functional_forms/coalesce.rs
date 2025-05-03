use crate::{NAryRdfTermOp, NAryRdfTermValueOp, SparqlOp, ThinResult};
use model::TermValueRef;
use model::{TermRef, ThinError};

#[derive(Debug)]
pub struct CoalesceSparqlOp;

impl Default for CoalesceSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for CoalesceSparqlOp {
    fn name(&self) -> &str {
        "coalesce"
    }
}

impl NAryRdfTermValueOp for CoalesceSparqlOp {
    type Args<'data> = TermValueRef<'data>;
    type Result<'data> = TermValueRef<'data>;

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

impl NAryRdfTermOp for CoalesceSparqlOp {
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(&self, args: &[TermRef<'data>]) -> ThinResult<Self::Result<'data>> {
        args.first().copied().ok_or(ThinError::Expected)
    }

    fn evaluate_error<'data>(
        &self,
        args: &[ThinResult<TermRef<'data>>],
    ) -> ThinResult<Self::Result<'data>> {
        args.iter()
            .find_map(|arg| arg.ok())
            .ok_or(ThinError::Expected)
    }
}
