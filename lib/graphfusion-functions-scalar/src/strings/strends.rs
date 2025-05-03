use crate::{BinaryTermValueOp, SparqlOp, ThinResult};
use model::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrEndsSparqlOp;

impl Default for StrEndsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrEndsSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrEndsSparqlOp {
    fn name(&self) -> &str {
        "strends"
    }
}

impl BinaryTermValueOp for StrEndsSparqlOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;
        Ok(args.lhs.ends_with(args.rhs).into())
    }
}
