use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Boolean, CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrEndsRdfOp;

impl Default for StrEndsRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrEndsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrEndsRdfOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;
        Ok(args.lhs.ends_with(args.rhs).into())
    }
}
