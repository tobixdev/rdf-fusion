use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrBeforeRdfOp;

impl Default for StrBeforeRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrBeforeRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrBeforeRdfOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = StringLiteralRef<'data>;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(lhs, rhs)?;

        if let Some(position) = args.lhs.find(args.rhs) {
            Ok(StringLiteralRef(&args.lhs[..position], args.language))
        } else {
            Ok(StringLiteralRef("", None))
        }
    }
}
