use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct ContainsRdfOp;

impl Default for ContainsRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for ContainsRdfOp {
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
