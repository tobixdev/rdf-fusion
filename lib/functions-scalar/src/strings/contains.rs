use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct ContainsSparqlOp;

impl Default for ContainsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for ContainsSparqlOp {}

impl BinarySparqlOp for ContainsSparqlOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;
        Ok(args.lhs.contains(args.rhs).into())
    }
}
