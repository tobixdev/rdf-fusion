use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrStartsRdfOp;

impl Default for StrStartsRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrStartsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrStartsRdfOp {
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
