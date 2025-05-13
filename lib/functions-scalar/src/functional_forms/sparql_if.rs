use crate::{SparqlOp, TernarySparqlOp, ThinResult};
use rdf_fusion_model::Boolean;
use rdf_fusion_model::TypedValueRef;

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

impl SparqlOp for IfSparqlOp {}

impl TernarySparqlOp for IfSparqlOp {
    type Arg0<'data> = Boolean;
    type Arg1<'data> = TypedValueRef<'data>;
    type Arg2<'data> = TypedValueRef<'data>;
    type Result<'data> = TypedValueRef<'data>;

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
