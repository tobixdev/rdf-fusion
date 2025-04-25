use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrAfterRdfOp;

impl Default for StrAfterRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrAfterRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for StrAfterRdfOp {
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
            let start = position + args.rhs.len();
            Ok(StringLiteralRef(&args.lhs[start..], args.language))
        } else {
            Ok(StringLiteralRef("", None))
        }
    }
}
