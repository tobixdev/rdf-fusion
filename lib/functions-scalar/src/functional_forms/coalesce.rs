use crate::{NArySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::TypedValueRef;
use graphfusion_model::ThinError;

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
}

impl NArySparqlOp for CoalesceSparqlOp {
    type Args<'data> = TypedValueRef<'data>;
    type Result<'data> = TypedValueRef<'data>;

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
