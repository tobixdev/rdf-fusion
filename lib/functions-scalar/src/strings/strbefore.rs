use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{CompatibleStringArgs, StringLiteralRef};

#[derive(Debug)]
pub struct StrBeforeRdfOp {}

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
        arg_lhs: Self::ArgLhs<'data>,
        arg_rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let args = CompatibleStringArgs::try_from(arg_lhs, arg_rhs)?;

        if let Some(position) = args.lhs.find(args.rhs) {
            Ok(StringLiteralRef(&args.lhs[..position], args.language))
        } else {
            Ok(StringLiteralRef("", None))
        }
    }
}
