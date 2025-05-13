use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrStartsSparqlOp;

impl Default for StrStartsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrStartsSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrStartsSparqlOp {}

impl BinarySparqlOp for StrStartsSparqlOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;
        Ok(args.lhs.starts_with(args.rhs).into())
    }
}
