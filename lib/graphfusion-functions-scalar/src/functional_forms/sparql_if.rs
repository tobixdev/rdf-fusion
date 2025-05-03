use crate::{SparqlOp, TernaryRdfTermValueOp, ThinResult};
use model::Boolean;
use model::TermValueRef;

#[derive(Debug)]
pub struct IfSparqlOp;

impl Default for IfSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IfSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for IfSparqlOp {
    fn name(&self) -> &str {
        "if"
    }
}

impl TernaryRdfTermValueOp for IfSparqlOp {
    type Arg0<'data> = Boolean;
    type Arg1<'data> = TermValueRef<'data>;
    type Arg2<'data> = TermValueRef<'data>;
    type Result<'data> = TermValueRef<'data>;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        Ok(if arg0.as_bool() { arg1 } else { arg2 })
    }

    fn evaluate_error<'data>(
        &self,
        arg0: ThinResult<Self::Arg0<'data>>,
        arg1: ThinResult<Self::Arg1<'data>>,
        arg2: ThinResult<Self::Arg2<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        if arg0?.as_bool() {
            arg1
        } else {
            arg2
        }
    }
}
