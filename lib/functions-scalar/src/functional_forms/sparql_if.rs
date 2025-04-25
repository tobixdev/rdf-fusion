use crate::{ScalarTernaryRdfOp, ThinResult};
use datamodel::{Boolean, TermRef};

#[derive(Debug)]
pub struct IfRdfOp;

impl Default for IfRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IfRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarTernaryRdfOp for IfRdfOp {
    type Arg0<'data> = Boolean;
    type Arg1<'data> = TermRef<'data>;
    type Arg2<'data> = TermRef<'data>;
    type Result<'data> = TermRef<'data>;

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
